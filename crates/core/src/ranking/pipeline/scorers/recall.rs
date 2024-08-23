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
        inbound_similarity,
        pipeline::{LocalRecallRankingWebpage, RankableWebpage},
    },
    searcher::{
        api::{self},
        SearchQuery,
    },
};

use super::{
    embedding::{EmbeddingScorer, KeywordEmbeddings, TitleEmbeddings},
    inbound_similarity::InboundScorer,
    term_distance, MultiScorer, Scorer,
};

pub struct Recall<T: RankableWebpage> {
    scorer: MultiScorer<T>,
}

impl Recall<api::ScoredWebpagePointer> {
    pub fn new(
        inbound: inbound_similarity::Scorer,
        dual_encoder: Option<Arc<DualEncoder>>,
    ) -> Self {
        Self {
            scorer: MultiScorer::new(vec![
                Box::new(
                    EmbeddingScorer::<api::ScoredWebpagePointer, TitleEmbeddings>::new(
                        dual_encoder.clone(),
                    ),
                ),
                Box::new(EmbeddingScorer::<
                    api::ScoredWebpagePointer,
                    KeywordEmbeddings,
                >::new(dual_encoder)),
                Box::new(InboundScorer::new(inbound)),
            ]),
        }
    }
}

impl Scorer<api::ScoredWebpagePointer> for Recall<api::ScoredWebpagePointer> {
    fn score(&self, webpages: &mut [api::ScoredWebpagePointer]) {
        self.scorer.score(webpages);
    }
}

impl Recall<LocalRecallRankingWebpage> {
    pub fn new(dual_encoder: Option<Arc<DualEncoder>>) -> Self {
        Self {
            scorer: MultiScorer::new(vec![
                Box::new(term_distance::TitleDistanceScorer),
                Box::new(term_distance::BodyDistanceScorer),
                Box::new(
                    EmbeddingScorer::<LocalRecallRankingWebpage, TitleEmbeddings>::new(
                        dual_encoder.clone(),
                    ),
                ),
                Box::new(EmbeddingScorer::<
                    LocalRecallRankingWebpage,
                    KeywordEmbeddings,
                >::new(dual_encoder)),
            ]),
        }
    }
}

impl Scorer<LocalRecallRankingWebpage> for Recall<LocalRecallRankingWebpage> {
    fn score(&self, webpages: &mut [LocalRecallRankingWebpage]) {
        self.scorer.score(webpages);
    }

    fn set_query_info(&mut self, query: &SearchQuery) {
        self.scorer.set_query_info(query);
    }
}
