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

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct DummyQuery;

impl tantivy::query::Query for DummyQuery {
    fn weight(
        &self,
        _: tantivy::query::EnableScoring<'_>,
    ) -> tantivy::Result<Box<dyn tantivy::query::Weight>> {
        Ok(Box::new(DummyWeight))
    }
}

struct DummyWeight;

impl tantivy::query::Weight for DummyWeight {
    fn scorer(
        &self,
        _: &tantivy::SegmentReader,
        _: tantivy::Score,
    ) -> tantivy::Result<Box<dyn tantivy::query::Scorer>> {
        Ok(Box::new(DummyScorer))
    }

    fn explain(
        &self,
        _: &tantivy::SegmentReader,
        _: tantivy::DocId,
    ) -> tantivy::Result<tantivy::query::Explanation> {
        Ok(tantivy::query::Explanation::new(
            "dummy query that doesn't match any documents",
            0.0,
        ))
    }
}

struct DummyScorer;

impl tantivy::query::Scorer for DummyScorer {
    fn score(&mut self) -> tantivy::Score {
        unreachable!()
    }
}

impl tantivy::DocSet for DummyScorer {
    fn advance(&mut self) -> tantivy::DocId {
        tantivy::TERMINATED
    }

    fn doc(&self) -> tantivy::DocId {
        tantivy::TERMINATED
    }

    fn size_hint(&self) -> u32 {
        0
    }
}
