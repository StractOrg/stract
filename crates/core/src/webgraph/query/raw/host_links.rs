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

use rustc_hash::FxHashSet;
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
        }
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
        }))
    }
}

struct HostLinksWeight {
    node: NodeID,
    field: FieldEnum,
    deduplication_field: FieldEnum,
    warmed_column_fields: WarmedColumnFields,
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
    seen_host_ids: FxHashSet<u64>,
}

impl HostLinksScorer {
    fn new(
        reader: &tantivy::SegmentReader,
        term: tantivy::Term,
        deduplication_field: FieldEnum,
        warmed_column_fields: &WarmedColumnFields,
    ) -> tantivy::Result<Option<Self>> {
        let host_id_column = warmed_column_fields
            .segment(&reader.segment_id())
            .u64(deduplication_field)
            .unwrap();

        Ok(reader
            .inverted_index(term.field())?
            .read_postings(&term, tantivy::schema::IndexRecordOption::Basic)?
            .map(|postings| {
                let mut seen_host_ids = FxHashSet::default();

                if let Some(host_id) = host_id_column.first(postings.doc()) {
                    seen_host_ids.insert(host_id);
                }

                Self {
                    seen_host_ids,
                    host_id_column,
                    postings,
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
            .map(|host_id| self.seen_host_ids.contains(&host_id))
            .unwrap_or(false)
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

        while self.has_seen_host(self.postings.doc()) && self.doc() != tantivy::TERMINATED {
            self.postings.advance();
        }

        if let Some(host_id) = self.host_id(self.postings.doc()) {
            self.seen_host_ids.insert(host_id);
        }

        self.postings.doc()
    }

    fn doc(&self) -> tantivy::DocId {
        self.postings.doc()
    }

    fn size_hint(&self) -> u32 {
        self.postings.size_hint()
    }
}
