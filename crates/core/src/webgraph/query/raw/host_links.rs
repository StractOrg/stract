// Stract is an open source web search engine.
// Copyright (C) 2024 Stract ApS
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use tantivy::{columnar::Column, postings::SegmentPostings, DocSet};

use crate::webgraph::{
    schema::{Field, FieldEnum},
    warmed_column_fields::WarmedColumnFields,
    NodeID,
};

#[derive(Debug, Clone)]
pub struct HostLinksQuery {
    node: NodeID,
    field: FieldEnum,
    deduplication_field: FieldEnum,
    warmed_column_fields: WarmedColumnFields,
    skip_self_links: bool,
}

impl HostLinksQuery {
    pub fn new<F: Field, FDedup: Field>(
        node: NodeID,
        lookup_field: F,
        deduplication_field: FDedup,
        warmed_column_fields: WarmedColumnFields,
    ) -> Self {
        Self {
            node,
            field: lookup_field.into(),
            deduplication_field: deduplication_field.into(),
            warmed_column_fields,
            skip_self_links: true,
        }
    }

    pub fn skip_self_links(mut self, skip_self_links: bool) -> Self {
        self.skip_self_links = skip_self_links;
        self
    }
}

impl tantivy::query::Query for HostLinksQuery {
    fn weight(
        &self,
        _: tantivy::query::EnableScoring<'_>,
    ) -> tantivy::Result<Box<dyn tantivy::query::Weight>> {
        Ok(Box::new(HostLinksWeight {
            node: self.node,
            field: self.field,
            deduplication_field: self.deduplication_field,
            warmed_column_fields: self.warmed_column_fields.clone(),
            skip_self_links: self.skip_self_links,
        }))
    }
}

struct HostLinksWeight {
    node: NodeID,
    field: FieldEnum,
    deduplication_field: FieldEnum,
    warmed_column_fields: WarmedColumnFields,
    skip_self_links: bool,
}

impl tantivy::query::Weight for HostLinksWeight {
    fn scorer(
        &self,
        reader: &tantivy::SegmentReader,
        _: tantivy::Score,
    ) -> tantivy::Result<Box<dyn tantivy::query::Scorer>> {
        let schema = reader.schema();
        let field = schema.get_field(self.field.name())?;
        let term = tantivy::Term::from_field_u64(field, self.node.as_u64());

        match HostLinksScorer::new(
            reader,
            term,
            self.deduplication_field,
            &self.warmed_column_fields,
            self.node.as_u64(),
            self.skip_self_links,
        ) {
            Ok(Some(scorer)) => Ok(Box::new(scorer)),
            _ => Ok(Box::new(tantivy::query::EmptyScorer)),
        }
    }

    fn explain(
        &self,
        _: &tantivy::SegmentReader,
        _: tantivy::DocId,
    ) -> tantivy::Result<tantivy::query::Explanation> {
        Ok(tantivy::query::Explanation::new("HostLinksWeight", 0.0))
    }
}

struct HostLinksScorer {
    postings: SegmentPostings,
    host_id_column: Column<u64>,
    last_host_id: Option<u64>,
    self_host_id: u64,
    skip_self_links: bool,
}

impl HostLinksScorer {
    fn new(
        reader: &tantivy::SegmentReader,
        term: tantivy::Term,
        deduplication_field: FieldEnum,
        warmed_column_fields: &WarmedColumnFields,
        self_host_id: u64,
        skip_self_links: bool,
    ) -> tantivy::Result<Option<Self>> {
        let host_id_column = warmed_column_fields
            .segment(&reader.segment_id())
            .u64(deduplication_field)
            .unwrap();

        Ok(reader
            .inverted_index(term.field())?
            .read_postings(&term, tantivy::schema::IndexRecordOption::Basic)?
            .map(|mut postings| {
                let mut last_host_id = None;

                if postings.doc() != tantivy::TERMINATED {
                    last_host_id = host_id_column.first(postings.doc());
                }

                while postings.doc() != tantivy::TERMINATED && last_host_id == Some(self_host_id) {
                    postings.advance();

                    if postings.doc() != tantivy::TERMINATED {
                        last_host_id = host_id_column.first(postings.doc());
                    } else {
                        last_host_id = None;
                    }
                }

                Self {
                    last_host_id,
                    host_id_column,
                    postings,
                    self_host_id,
                    skip_self_links,
                }
            }))
    }
}

impl HostLinksScorer {
    fn host_id(&self, doc: tantivy::DocId) -> Option<u64> {
        if doc == tantivy::TERMINATED {
            return None;
        }

        self.host_id_column.first(doc)
    }

    fn has_seen_host(&self, doc: tantivy::DocId) -> bool {
        self.host_id(doc)
            .map(|host_id| self.last_host_id == Some(host_id))
            .unwrap_or(false)
    }

    fn skip_self(&self, doc: tantivy::DocId) -> bool {
        self.skip_self_links && self.host_id(doc) == Some(self.self_host_id)
    }
}

impl tantivy::query::Scorer for HostLinksScorer {
    fn score(&mut self) -> tantivy::Score {
        unimplemented!()
    }
}
impl tantivy::DocSet for HostLinksScorer {
    fn advance(&mut self) -> tantivy::DocId {
        self.postings.advance();

        while self.has_seen_host(
            self.postings
                .block_cursor()
                .skip_reader()
                .last_doc_in_block(),
        ) && self.doc() != tantivy::TERMINATED
        {
            self.postings.mut_block_cursor().advance();
            self.postings.reset_cursor_start_block();
        }

        while (self.has_seen_host(self.doc()) || self.skip_self(self.doc()))
            && self.doc() != tantivy::TERMINATED
        {
            self.postings.advance();
        }

        if let Some(host_id) = self.host_id(self.doc()) {
            self.last_host_id = Some(host_id);
        }

        self.doc()
    }

    fn doc(&self) -> tantivy::DocId {
        self.postings.doc()
    }

    fn size_hint(&self) -> u32 {
        self.postings.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        webgraph::{query::HostBacklinksQuery, Edge, Node, Webgraph},
        webpage::RelFlags,
    };

    #[test]
    fn test_simple() {
        let temp_dir = crate::gen_temp_dir().unwrap();
        let mut graph = Webgraph::builder(&temp_dir, 0u64.into()).open().unwrap();

        let node_a = Node::from("A");
        let node_b = Node::from("B");
        let node_c = Node::from("C");

        graph
            .insert(Edge {
                from: node_a.clone(),
                to: node_b.clone(),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();

        graph
            .insert(Edge {
                from: node_c.clone(),
                to: node_b.clone(),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();

        graph.commit().unwrap();

        let res = graph.search(&HostBacklinksQuery::new(node_b.id())).unwrap();

        assert_eq!(res.len(), 2);
        assert!(res.iter().any(|r| r.from == node_a.id()));
        assert!(res.iter().any(|r| r.from == node_c.id()));
    }

    #[test]
    fn test_self_host_skipped() {
        let temp_dir = crate::gen_temp_dir().unwrap();
        let mut graph = Webgraph::builder(&temp_dir, 0u64.into()).open().unwrap();

        let node_a = Node::from("A");
        let node_b = Node::from("B");

        graph
            .insert(Edge {
                from: node_a.clone(),
                to: node_b.clone(),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();

        graph
            .insert(Edge {
                from: node_b.clone(),
                to: node_b.clone(),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();

        graph.commit().unwrap();

        let res = graph.search(&HostBacklinksQuery::new(node_b.id())).unwrap();

        assert_eq!(res.len(), 1);
        assert!(res[0].from == node_a.id());
    }

    #[test]
    fn test_deduplication() {
        let temp_dir = crate::gen_temp_dir().unwrap();
        let mut graph = Webgraph::builder(&temp_dir, 0u64.into()).open().unwrap();

        graph
            .insert(Edge {
                from: Node::from("https://A.com/1"),
                to: Node::from("https://B.com/1"),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();

        graph
            .insert(Edge {
                from: Node::from("https://A.com/2"),
                to: Node::from("https://B.com/2"),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();

        graph
            .insert(Edge {
                from: Node::from("https://A.com/3"),
                to: Node::from("https://B.com/3"),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();

        graph.commit().unwrap();

        let res = graph
            .search(&HostBacklinksQuery::new(
                Node::from("https://B.com/").into_host().id(),
            ))
            .unwrap();

        assert_eq!(res.len(), 1);
        assert!(res[0].from == Node::from("https://A.com/").into_host().id());
    }
}
