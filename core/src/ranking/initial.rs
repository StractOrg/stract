// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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

use std::sync::Arc;

use crate::fastfield_cache::FastFieldCache;
use crate::webpage::region::{Region, RegionCount};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tantivy::collector::{ScoreSegmentTweaker, ScoreTweaker};
use tantivy::{DocId, SegmentReader};

use super::SignalAggregator;

pub(crate) struct InitialScoreTweaker {
    region_count: Arc<RegionCount>,
    selected_region: Option<Region>,
    fastfield_cache: Arc<FastFieldCache>,
    aggregator: SignalAggregator,
}

impl InitialScoreTweaker {
    pub fn new(
        region_count: Arc<RegionCount>,
        selected_region: Option<Region>,
        aggregator: SignalAggregator,
        fastfield_cache: Arc<FastFieldCache>,
    ) -> Self {
        Self {
            region_count,
            selected_region,
            aggregator,
            fastfield_cache,
        }
    }
}

pub(crate) struct InitialSegmentScoreTweaker {
    aggregator: SignalAggregator,
    region_count: Arc<RegionCount>,
    current_timestamp: usize,
    selected_region: Option<Region>,
}

impl ScoreTweaker<Score> for InitialScoreTweaker {
    type Child = InitialSegmentScoreTweaker;

    fn segment_tweaker(&self, segment_reader: &SegmentReader) -> tantivy::Result<Self::Child> {
        let mut aggregator = self.aggregator.clone();

        let fastfield_cache_segment = self
            .fastfield_cache
            .get_segment(&segment_reader.segment_id());

        aggregator.register_segment(fastfield_cache_segment);

        let current_timestamp = Utc::now().timestamp() as usize;

        Ok(InitialSegmentScoreTweaker {
            aggregator,
            current_timestamp,
            selected_region: self.selected_region,
            region_count: Arc::clone(&self.region_count),
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Score {
    pub bm25: tantivy::Score,
    pub total: f64,
}

impl ScoreSegmentTweaker<Score> for InitialSegmentScoreTweaker {
    fn score(&mut self, doc: DocId, score: tantivy::Score) -> Score {
        self.aggregator.score(
            doc,
            score,
            &self.region_count,
            self.current_timestamp,
            self.selected_region,
        )
    }
}
