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

use std::sync::Mutex;

use crate::{
    ranking::{inbound_similarity, signal},
    searcher::api::ScoredWebpagePointer,
};

use super::Scorer;

pub struct InboundScorer {
    scorer: Mutex<inbound_similarity::Scorer>,
}

impl InboundScorer {
    pub fn new(scorer: inbound_similarity::Scorer) -> Self {
        Self {
            scorer: Mutex::new(scorer),
        }
    }
}

impl Scorer<ScoredWebpagePointer> for InboundScorer {
    fn score(&self, webpages: &mut [ScoredWebpagePointer]) {
        let mut scorer = self.scorer.lock().unwrap();

        for webpage in webpages {
            let score = scorer.score(
                webpage.as_ranking().host_id(),
                webpage.as_ranking().inbound_edges(),
            );
            webpage
                .as_ranking_mut()
                .signals_mut()
                .insert(signal::InboundSimilarity.into(), score);
        }
    }
}
