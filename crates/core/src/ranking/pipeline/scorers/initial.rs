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

use std::sync::Arc;

use crate::{
    ranking::{
        models::lambdamart::LambdaMART,
        pipeline::{RankableWebpage, RecallRankingWebpage},
        SignalCoefficient,
    },
    searcher::SearchQuery,
};

use super::{calculate_score, Scorer};

#[derive(Default)]
pub struct Initial {
    model: Option<Arc<LambdaMART>>,
    signal_coefficients: Option<SignalCoefficient>,
}

impl Initial {
    pub fn new(model: Option<Arc<LambdaMART>>) -> Self {
        Self {
            model,
            signal_coefficients: None,
        }
    }

    pub fn with_coefficients(mut self, signal_coefficients: Option<SignalCoefficient>) -> Self {
        self.signal_coefficients = signal_coefficients;
        self
    }
}

impl Scorer<RecallRankingWebpage> for Initial {
    fn score(&self, webpages: &mut [RecallRankingWebpage]) {
        for webpage in webpages {
            webpage.set_score(calculate_score(
                &self.model,
                &self.signal_coefficients,
                &webpage.signals,
            ));
        }
    }

    fn set_query_info(&mut self, query: &SearchQuery) {
        self.signal_coefficients = query.optic.as_ref().map(SignalCoefficient::from_optic);
    }
}

impl Scorer<crate::searcher::api::ScoredWebpagePointer> for Initial {
    fn score(&self, webpages: &mut [crate::searcher::api::ScoredWebpagePointer]) {
        for webpage in webpages {
            webpage.set_score(calculate_score(
                &self.model,
                &self.signal_coefficients,
                &webpage.as_ranking().signals,
            ));
        }
    }

    fn set_query_info(&mut self, query: &SearchQuery) {
        self.signal_coefficients = query.optic.as_ref().map(SignalCoefficient::from_optic);
    }
}
