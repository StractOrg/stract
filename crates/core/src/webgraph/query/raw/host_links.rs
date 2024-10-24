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
    NodeID,
};

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct HostLinksQuery {
    node: NodeID,
    field: FieldEnum,
    deduplication_field: FieldEnum,
}

impl HostLinksQuery {
    pub fn new<F: Field, FDedup: Field>(
        node: NodeID,
        lookup_field: F,
        deduplication_field: FDedup,
    ) -> Self {
        Self {
            node,
            field: lookup_field.into(),
            deduplication_field: deduplication_field.into(),
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
        }))
    }
}

struct HostLinksWeight {
    node: NodeID,
    field: FieldEnum,
    deduplication_field: FieldEnum,
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

        match HostLinksScorer::new(reader, term, self.deduplication_field) {
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
}

impl HostLinksScorer {
    fn new(
        reader: &tantivy::SegmentReader,
        term: tantivy::Term,
        deduplication_field: FieldEnum,
    ) -> tantivy::Result<Option<Self>> {
        let host_id_column = reader.column_fields().u64(deduplication_field.name())?;

        Ok(reader
            .inverted_index(term.field())?
            .read_postings(&term, tantivy::schema::IndexRecordOption::Basic)?
            .map(|postings| Self {
                last_host_id: host_id_column.first(postings.doc()),
                host_id_column,
                postings,
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
}

impl tantivy::query::Scorer for HostLinksScorer {
    fn score(&mut self) -> tantivy::Score {
        unimplemented!()
    }
}
impl tantivy::DocSet for HostLinksScorer {
    fn advance(&mut self) -> tantivy::DocId {
        self.postings.advance();

        while self.last_host_id
            == self.host_id(
                self.postings
                    .block_cursor()
                    .skip_reader()
                    .last_doc_in_block(),
            )
            && self.doc() != tantivy::TERMINATED
        {
            self.postings.mut_block_cursor().advance();
            self.postings.reset_cursor_start_block();
        }

        while self.host_id(self.postings.doc()) == self.last_host_id
            && self.doc() != tantivy::TERMINATED
        {
            self.postings.advance();
        }

        self.last_host_id = self.host_id(self.postings.doc());
        self.postings.doc()
    }

    fn doc(&self) -> tantivy::DocId {
        self.postings.doc()
    }

    fn size_hint(&self) -> u32 {
        self.postings.size_hint()
    }
}
