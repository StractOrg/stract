// Stract is an open source web search engine.
// Copyright (C) 2023 Stract ApS
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

use tantivy::{
    query::{Explanation, Query, Scorer, Weight},
    DocSet, Score,
};

#[derive(Debug)]
pub struct ConstQuery {
    subquery: Box<dyn Query>,
    score: Score,
}

impl ConstQuery {
    pub fn new(subquery: Box<dyn Query>, score: Score) -> Self {
        Self { subquery, score }
    }
}

impl Clone for ConstQuery {
    fn clone(&self) -> Self {
        Self {
            subquery: self.subquery.box_clone(),
            score: self.score,
        }
    }
}

impl Query for ConstQuery {
    fn weight(
        &self,
        enable_scoring: tantivy::query::EnableScoring,
    ) -> tantivy::Result<Box<dyn Weight>> {
        Ok(Box::new(ConstWeight {
            subweight: self.subquery.weight(enable_scoring)?,
            score: self.score,
        }))
    }
}

struct ConstWeight {
    subweight: Box<dyn Weight>,
    score: Score,
}

impl Weight for ConstWeight {
    fn scorer(
        &self,
        reader: &tantivy::SegmentReader,
        boost: Score,
    ) -> tantivy::Result<Box<dyn Scorer>> {
        let subscorer = self.subweight.scorer(reader, boost)?;
        Ok(Box::new(ConstScorer {
            subscorer,
            score: boost,
        }))
    }

    fn explain(
        &self,
        reader: &tantivy::SegmentReader,
        doc: tantivy::DocId,
    ) -> tantivy::Result<tantivy::query::Explanation> {
        let mut expl = Explanation::new("Constant score for query", self.score);
        expl.add_detail(self.subweight.explain(reader, doc)?);
        Ok(expl)
    }
}

struct ConstScorer {
    subscorer: Box<dyn Scorer>,
    score: Score,
}

impl Scorer for ConstScorer {
    fn score(&mut self) -> Score {
        self.score
    }
}

impl DocSet for ConstScorer {
    fn advance(&mut self) -> tantivy::DocId {
        self.subscorer.advance()
    }

    fn doc(&self) -> tantivy::DocId {
        self.subscorer.doc()
    }

    fn size_hint(&self) -> u32 {
        self.subscorer.size_hint()
    }
}
