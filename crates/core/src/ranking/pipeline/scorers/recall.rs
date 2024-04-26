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
    ranking::pipeline::{RankableWebpage, RecallRankingWebpage},
    searcher::{api::ScoredWebpagePointer, SearchQuery},
};

use super::{
    embedding::{EmbeddingScorer, KeywordEmbeddings, TitleEmbeddings},
    MultiScorer, Scorer,
};

pub struct Recall<T: RankableWebpage> {
    scorer: MultiScorer<T>,
}

impl Recall<RecallRankingWebpage> {
    pub fn new(dual_encoder: Option<Arc<DualEncoder>>) -> Self {
        Self {
            scorer: MultiScorer::new(vec![
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
    pub fn new(dual_encoder: Option<Arc<DualEncoder>>) -> Self {
        Self {
            scorer: MultiScorer::new(vec![
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
        self.scorer.score(webpages);
    }

    fn set_query_info(&mut self, query: &SearchQuery) {
        self.scorer.set_query_info(query);
    }
}

impl Scorer<ScoredWebpagePointer> for Recall<ScoredWebpagePointer> {
    fn score(&self, webpages: &mut [crate::searcher::api::ScoredWebpagePointer]) {
        self.scorer.score(webpages);
    }
}
