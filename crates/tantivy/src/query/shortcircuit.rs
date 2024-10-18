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
use crate::{
    query::{Explanation, Query, Scorer, Weight},
    DocSet, Score,
};

#[derive(Debug)]
pub struct ShortCircuitQuery {
    subquery: Box<dyn Query>,
    max_docs_per_segment: u64,
}

impl ShortCircuitQuery {
    pub fn new(subquery: Box<dyn Query>, max_docs_per_segment: u64) -> Self {
        Self {
            subquery,
            max_docs_per_segment,
        }
    }
}

impl Clone for ShortCircuitQuery {
    fn clone(&self) -> Self {
        Self {
            subquery: self.subquery.box_clone(),
            max_docs_per_segment: self.max_docs_per_segment,
        }
    }
}

impl Query for ShortCircuitQuery {
    fn weight(
        &self,
        enable_scoring: crate::query::EnableScoring,
    ) -> crate::Result<Box<dyn Weight>> {
        Ok(Box::new(ShortCircuitWeight {
            subweight: self.subquery.weight(enable_scoring)?,
            max_docs_per_segment: self.max_docs_per_segment,
        }))
    }
}

struct ShortCircuitWeight {
    subweight: Box<dyn Weight>,
    max_docs_per_segment: u64,
}

impl Weight for ShortCircuitWeight {
    fn scorer(
        &self,
        reader: &crate::SegmentReader,
        boost: Score,
    ) -> crate::Result<Box<dyn Scorer>> {
        let subscorer = self.subweight.scorer(reader, boost)?;
        Ok(Box::new(ShortCircuitScorer {
            subscorer,
            num_docs: 0,
            max_docs_per_segment: self.max_docs_per_segment,
        }))
    }

    fn explain(
        &self,
        reader: &crate::SegmentReader,
        doc: crate::DocId,
    ) -> crate::Result<crate::query::Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(crate::TantivyError::InvalidArgument(format!(
                "Document #({doc}) does not match"
            )));
        }
        let score = scorer.score();

        let mut expl = Explanation::new("Short circuited query", score);
        expl.add_context(format!(
            "Max docs per segment: {}",
            self.max_docs_per_segment
        ));
        expl.add_detail(self.subweight.explain(reader, doc)?);
        Ok(expl)
    }
}

struct ShortCircuitScorer {
    subscorer: Box<dyn Scorer>,
    num_docs: u64,
    max_docs_per_segment: u64,
}

impl Scorer for ShortCircuitScorer {
    fn score(&mut self) -> Score {
        self.subscorer.score()
    }
}

impl DocSet for ShortCircuitScorer {
    fn advance(&mut self) -> crate::DocId {
        if self.num_docs >= self.max_docs_per_segment {
            return crate::TERMINATED;
        }

        self.num_docs += 1;
        self.subscorer.advance()
    }

    fn doc(&self) -> crate::DocId {
        if self.num_docs >= self.max_docs_per_segment {
            return crate::TERMINATED;
        }

        self.subscorer.doc()
    }

    fn size_hint(&self) -> u32 {
        self.subscorer
            .size_hint()
            .min(self.max_docs_per_segment as u32)
    }
}
