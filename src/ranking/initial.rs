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

use crate::schema::{Field, CENTRALITY_SCALING};
use crate::webpage::region::{Region, RegionCount};
use chrono::Utc;
use tantivy::collector::{ScoreSegmentTweaker, ScoreTweaker};
use tantivy::fastfield::{DynamicFastFieldReader, FastFieldReader};
use tantivy::{DocId, Score, SegmentReader};

pub(crate) struct InitialScoreTweaker {
    region_count: Arc<RegionCount>,
    selected_region: Option<Region>,
}

impl InitialScoreTweaker {
    pub fn new(region_count: Arc<RegionCount>, selected_region: Option<Region>) -> Self {
        Self {
            region_count,
            selected_region,
        }
    }
}

pub(crate) struct InitialSegmentScoreTweaker {
    centrality_reader: DynamicFastFieldReader<u64>,
    is_homepage_reader: DynamicFastFieldReader<u64>,
    fetch_time_ms_reader: DynamicFastFieldReader<u64>,
    update_timestamp_reader: DynamicFastFieldReader<u64>,
    num_trackers_reader: DynamicFastFieldReader<u64>,
    region_reader: DynamicFastFieldReader<u64>,
    region_count: Arc<RegionCount>,
    current_timestamp: f64,
    selected_region: Option<Region>,
}

impl ScoreTweaker<f64> for InitialScoreTweaker {
    type Child = InitialSegmentScoreTweaker;

    fn segment_tweaker(&self, segment_reader: &SegmentReader) -> tantivy::Result<Self::Child> {
        let centrality_field = segment_reader
            .schema()
            .get_field(Field::Centrality.as_str())
            .expect("Faild to load centrality field");
        let centrality_reader = segment_reader
            .fast_fields()
            .u64(centrality_field)
            .expect("Failed to get centrality fast-field reader");

        let is_homepage_field = segment_reader
            .schema()
            .get_field(Field::IsHomepage.as_str())
            .expect("Faild to load is_homepage field");
        let is_homepage_reader = segment_reader
            .fast_fields()
            .u64(is_homepage_field)
            .expect("Failed to get is_homepage fast-field reader");

        let fetch_time_ms_field = segment_reader
            .schema()
            .get_field(Field::FetchTimeMs.as_str())
            .expect("Faild to load fetch_time_ms field");
        let fetch_time_ms_reader = segment_reader
            .fast_fields()
            .u64(fetch_time_ms_field)
            .expect("Failed to get fetch_time_ms fast-field reader");

        let update_timestamp_field = segment_reader
            .schema()
            .get_field(Field::LastUpdated.as_str())
            .expect("Faild to load last_updated field");
        let update_timestamp_reader = segment_reader
            .fast_fields()
            .u64(update_timestamp_field)
            .expect("Failed to get last_updated fast-field reader");

        let num_trackers_field = segment_reader
            .schema()
            .get_field(Field::NumTrackers.as_str())
            .expect("Faild to load num_trackers field");
        let num_trackers_reader = segment_reader
            .fast_fields()
            .u64(num_trackers_field)
            .expect("Failed to get num_trackers fast-field reader");

        let region_field = segment_reader
            .schema()
            .get_field(Field::Region.as_str())
            .expect("Faild to load region field");
        let region_reader = segment_reader
            .fast_fields()
            .u64(region_field)
            .expect("Failed to get region fast-field reader");

        let current_timestamp = Utc::now().timestamp() as f64;

        Ok(InitialSegmentScoreTweaker {
            centrality_reader,
            is_homepage_reader,
            fetch_time_ms_reader,
            update_timestamp_reader,
            num_trackers_reader,
            current_timestamp,
            region_reader,
            selected_region: self.selected_region,
            region_count: Arc::clone(&self.region_count),
        })
    }
}

fn time_to_score(time: f64) -> f64 {
    1.0 / ((time + 1.0).log2())
}

fn region_score(
    region_count: &RegionCount,
    selected_region: Option<Region>,
    webpage_region: Region,
) -> f64 {
    let boost = selected_region.map_or(
        0.0,
        |region| if region == webpage_region { 50.0 } else { 0.0 },
    );

    boost + region_count.score(&webpage_region)
}

impl ScoreSegmentTweaker<f64> for InitialSegmentScoreTweaker {
    fn score(&mut self, doc: DocId, score: Score) -> f64 {
        let score = score as f64;
        let centrality: f64 = self.centrality_reader.get(doc) as f64 / CENTRALITY_SCALING as f64;
        let is_homepage = self.is_homepage_reader.get(doc) as f64;
        let fetch_time_ms = self.fetch_time_ms_reader.get(doc) as f64;
        let update_timestamp = self.update_timestamp_reader.get(doc) as f64;
        let hours_since_update = (self.current_timestamp - update_timestamp).max(0.000001) / 3600.0;
        let num_trackers = self.num_trackers_reader.get(doc) as f64;
        let region = Region::from_id(self.region_reader.get(doc));

        (3.0 * score)
            + (3200.0 * centrality)
            + (1.0 * is_homepage)
            + (1.0 / (fetch_time_ms + 1.0))
            + (1500.0 * time_to_score(hours_since_update))
            + (200.0 * (1.0 / (num_trackers + 1.0)))
            + (25.0 * region_score(&self.region_count, self.selected_region, region))
    }
}
