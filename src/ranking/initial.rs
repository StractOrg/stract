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

use crate::schema::{Field, CENTRALITY_SCALING};
use tantivy::collector::{ScoreSegmentTweaker, ScoreTweaker};
use tantivy::fastfield::{DynamicFastFieldReader, FastFieldReader};
use tantivy::{DocId, Score, SegmentReader};

#[derive(Default)]
pub(crate) struct InitialScoreTweaker {}

pub(crate) struct InitialSegmentScoreTweaker {
    centrality_reader: DynamicFastFieldReader<u64>,
    is_homepage_reader: DynamicFastFieldReader<u64>,
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

        Ok(InitialSegmentScoreTweaker {
            centrality_reader,
            is_homepage_reader,
        })
    }
}

impl ScoreSegmentTweaker<f64> for InitialSegmentScoreTweaker {
    fn score(&mut self, doc: DocId, score: Score) -> f64 {
        let score = score as f64;
        let centrality: f64 = self.centrality_reader.get(doc) as f64 / CENTRALITY_SCALING as f64;
        let is_homepage = self.is_homepage_reader.get(doc) as f64;

        score + 1_000.0 * centrality + 5.0 * is_homepage
    }
}
