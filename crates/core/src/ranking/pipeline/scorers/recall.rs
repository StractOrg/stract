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
    models::dual_encoder::DualEncoder,
    ranking::{
        models::lambdamart::LambdaMART,
        pipeline::{RankableWebpage, RecallRankingWebpage},
        SignalCoefficient,
    },
    searcher::{api::ScoredWebpagePointer, SearchQuery},
};

use super::{
    calculate_score,
    embedding::{EmbeddingScorer, KeywordEmbeddings, TitleEmbeddings},
    MultiScorer, Scorer,
};

pub struct Recall<T: RankableWebpage> {
    sub_scorers: MultiScorer<T>,
    lambdamart: Option<Arc<LambdaMART>>,
    signal_coefficients: Option<SignalCoefficient>,
}

impl Recall<RecallRankingWebpage> {
    pub fn new(
        lambdamart: Option<Arc<LambdaMART>>,
        dual_encoder: Option<Arc<DualEncoder>>,
    ) -> Self {
        Self {
            lambdamart,
            signal_coefficients: None,
            sub_scorers: MultiScorer::new(vec![
                Box::new(
                    EmbeddingScorer::<RecallRankingWebpage, TitleEmbeddings>::new(
                        dual_encoder.clone(),
                    ),
                ),
                Box::new(
                    EmbeddingScorer::<RecallRankingWebpage, KeywordEmbeddings>::new(dual_encoder),
                ),
            ]),
        }
    }
}

impl Recall<ScoredWebpagePointer> {
    pub fn new(
        lambdamart: Option<Arc<LambdaMART>>,
        dual_encoder: Option<Arc<DualEncoder>>,
    ) -> Self {
        Self {
            lambdamart,
            signal_coefficients: None,
            sub_scorers: MultiScorer::new(vec![
                Box::new(
                    EmbeddingScorer::<ScoredWebpagePointer, TitleEmbeddings>::new(
                        dual_encoder.clone(),
                    ),
                ),
                Box::new(
                    EmbeddingScorer::<ScoredWebpagePointer, KeywordEmbeddings>::new(dual_encoder),
                ),
            ]),
        }
    }
}

impl Scorer<RecallRankingWebpage> for Recall<RecallRankingWebpage> {
    fn score(&self, webpages: &mut [RecallRankingWebpage]) {
        self.sub_scorers.score(webpages);

        for webpage in webpages {
            webpage.set_score(calculate_score(
                &self.lambdamart,
                self.signal_coefficients.clone().unwrap_or_default(),
                &webpage.signals,
            ));
        }
    }

    fn set_query_info(&mut self, query: &SearchQuery) {
        self.sub_scorers.set_query_info(query);
        self.signal_coefficients = query.optic.as_ref().map(SignalCoefficient::from_optic);
    }
}

impl Scorer<ScoredWebpagePointer> for Recall<ScoredWebpagePointer> {
    fn score(&self, webpages: &mut [crate::searcher::api::ScoredWebpagePointer]) {
        self.sub_scorers.score(webpages);

        for webpage in webpages {
            webpage.set_score(calculate_score(
                &self.lambdamart,
                self.signal_coefficients.clone().unwrap_or_default(),
                &webpage.as_ranking().signals,
            ));
        }
    }

    fn set_query_info(&mut self, query: &SearchQuery) {
        self.signal_coefficients = query.optic.as_ref().map(SignalCoefficient::from_optic);
    }
}
