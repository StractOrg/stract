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

use tantivy::{postings::SegmentPostings, query::EmptyScorer, Term};

use crate::webgraph::{
    schema::{Field, FieldEnum},
    NodeID,
};

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct LinksQuery {
    node: NodeID,
    field: FieldEnum,
}

impl tantivy::query::Query for LinksQuery {
    fn weight(
        &self,
        _: tantivy::query::EnableScoring<'_>,
    ) -> tantivy::Result<Box<dyn tantivy::query::Weight>> {
        Ok(Box::new(LinksWeight {
            node: self.node,
            field: self.field,
        }))
    }
}

struct LinksWeight {
    node: NodeID,
    field: FieldEnum,
}

impl tantivy::query::Weight for LinksWeight {
    fn scorer(
        &self,
        reader: &tantivy::SegmentReader,
        _: tantivy::Score,
    ) -> tantivy::Result<Box<dyn tantivy::query::Scorer>> {
        let field = reader.schema().get_field(self.field.name())?;
        let term = Term::from_field_u64(field, self.node.as_u64());

        let index = reader.inverted_index(field)?;
        match index.read_postings(&term, tantivy::schema::IndexRecordOption::Basic)? {
            Some(postings) => Ok(Box::new(LinksScorer { postings })),
            None => Ok(Box::new(EmptyScorer)),
        }
    }

    fn explain(
        &self,
        _: &tantivy::SegmentReader,
        _: tantivy::DocId,
    ) -> tantivy::Result<tantivy::query::Explanation> {
        let explanation = tantivy::query::Explanation::new("LinksQuery", 0.0);
        Ok(explanation)
    }
}

struct LinksScorer {
    postings: SegmentPostings,
}

impl tantivy::query::Scorer for LinksScorer {
    fn score(&mut self) -> tantivy::Score {
        unimplemented!()
    }
}

impl tantivy::DocSet for LinksScorer {
    fn advance(&mut self) -> tantivy::DocId {
        self.postings.advance()
    }

    fn doc(&self) -> tantivy::DocId {
        self.postings.doc()
    }

    fn size_hint(&self) -> u32 {
        self.postings.size_hint()
    }
}
