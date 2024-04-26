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

use tantivy::collector::{ScoreSegmentTweaker, ScoreTweaker};
use tantivy::{DocId, SegmentReader};

use super::SignalComputer;

pub struct InitialScoreTweaker {
    tv_searcher: tantivy::Searcher,
    fastfield_reader: FastFieldReader,
    computer: SignalComputer,
}

/// SAFETY:
/// InitialScoreTweaker is thread-safe because it never mutates it's internal state.
/// It only ever spawns InitialSegmentScoreTweakers which are not thread-safe.
unsafe impl Sync for InitialScoreTweaker {}
unsafe impl Send for InitialScoreTweaker {}

impl InitialScoreTweaker {
    pub fn new(
        tv_searcher: tantivy::Searcher,
        computer: SignalComputer,
        fastfield_reader: FastFieldReader,
    ) -> Self {
        Self {
            tv_searcher,
            computer,
            fastfield_reader,
        }
    }
}

impl ScoreTweaker<Score> for InitialScoreTweaker {
    type Child = InitialSegmentScoreTweaker;

    fn segment_tweaker(&self, segment_reader: &SegmentReader) -> tantivy::Result<Self::Child> {
        let mut computer = self.computer.clone();

        let current_timestamp = Utc::now().timestamp() as usize;
        computer.set_current_timestamp(current_timestamp);

        computer
            .register_segment(&self.tv_searcher, segment_reader, &self.fastfield_reader)
            .unwrap();

        Ok(InitialSegmentScoreTweaker { computer })
    }
}

pub struct InitialSegmentScoreTweaker {
    computer: SignalComputer,
}

#[derive(
    Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Clone, PartialEq,
)]
pub struct Score {
    pub total: f64,
}

impl ScoreSegmentTweaker<Score> for InitialSegmentScoreTweaker {
    fn score(&mut self, doc: DocId, _score: tantivy::Score) -> Score {
        let mut total = self
            .computer
            .compute_signals(doc)
            .flatten()
            .map(|computed| self.computer.coefficient(&computed.signal) * computed.score)
            .sum();

        if let Some(boost) = self.computer.boosts(doc) {
            total *= boost;
        }

        Score { total }
    }
}
