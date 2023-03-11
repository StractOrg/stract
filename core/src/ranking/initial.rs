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

use crate::fastfield_reader::FastFieldReader;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tantivy::collector::{ScoreSegmentTweaker, ScoreTweaker};
use tantivy::{DocId, SegmentReader};

use super::SignalAggregator;

pub(crate) struct InitialScoreTweaker {
    tv_searcher: tantivy::Searcher,
    fastfield_reader: FastFieldReader,
    aggregator: SignalAggregator,
}

impl InitialScoreTweaker {
    pub fn new(
        tv_searcher: tantivy::Searcher,
        aggregator: SignalAggregator,
        fastfield_reader: FastFieldReader,
    ) -> Self {
        Self {
            tv_searcher,
            aggregator,
            fastfield_reader,
        }
    }
}

pub(crate) struct InitialSegmentScoreTweaker {
    aggregator: SignalAggregator,
}

impl ScoreTweaker<Score> for InitialScoreTweaker {
    type Child = InitialSegmentScoreTweaker;

    fn segment_tweaker(&self, segment_reader: &SegmentReader) -> tantivy::Result<Self::Child> {
        let mut aggregator = self.aggregator.clone();

        let fastfield_segment_reader = self
            .fastfield_reader
            .get_segment(&segment_reader.segment_id());

        aggregator
            .register_segment(&self.tv_searcher, segment_reader, fastfield_segment_reader)
            .unwrap();

        let current_timestamp = Utc::now().timestamp() as usize;
        aggregator.set_current_timestamp(current_timestamp);

        Ok(InitialSegmentScoreTweaker { aggregator })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Score {
    pub bm25: tantivy::Score,
    pub total: f64,
}

impl ScoreSegmentTweaker<Score> for InitialSegmentScoreTweaker {
    fn score(&mut self, doc: DocId, score: tantivy::Score) -> Score {
        let total = self
            .aggregator
            .compute_signals(doc)
            .flatten()
            .map(|computed| computed.coefficient * computed.value)
            .sum();

        Score { bm25: score, total }
    }
}
