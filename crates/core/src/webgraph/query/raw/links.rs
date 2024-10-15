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

use crate::webgraph::NodeID;

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct LinksQuery {
    node: NodeID,
    field_name: String,
}

impl tantivy::query::Query for LinksQuery {
    fn weight(
        &self,
        enable_scoring: tantivy::query::EnableScoring<'_>,
    ) -> tantivy::Result<Box<dyn tantivy::query::Weight>> {
        todo!()
    }
}

struct LinksWeight {
    node: NodeID,
}

impl tantivy::query::Weight for LinksWeight {
    fn scorer(
        &self,
        reader: &tantivy::SegmentReader,
        boost: tantivy::Score,
    ) -> tantivy::Result<Box<dyn tantivy::query::Scorer>> {
        todo!()
    }

    fn explain(
        &self,
        reader: &tantivy::SegmentReader,
        doc: tantivy::DocId,
    ) -> tantivy::Result<tantivy::query::Explanation> {
        todo!()
    }
}

struct LinksScorer {
    node: NodeID,
}

impl tantivy::query::Scorer for LinksScorer {
    fn score(&mut self) -> tantivy::Score {
        todo!()
    }
}

impl tantivy::DocSet for LinksScorer {
    fn advance(&mut self) -> tantivy::DocId {
        todo!()
    }

    fn doc(&self) -> tantivy::DocId {
        todo!()
    }

    fn size_hint(&self) -> u32 {
        todo!()
    }
}
