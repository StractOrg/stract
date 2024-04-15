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
    collector,
    config::CollectorConfig,
    inverted_index::RetrievedWebpage,
    ranking::{
        models::{cross_encoder::CrossEncoder, lambdamart::LambdaMART},
        pipeline::{
            scorers::IdentityScorer, RankableWebpage, RankingPipeline, RankingStage, ReRanker,
            Scorer,
        },
    },
    searcher::SearchQuery,
    Result,
};

use super::RecallRankingWebpage;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct PrecisionRankingWebpage {
    pub retrieved_webpage: RetrievedWebpage,
    pub ranking: RecallRankingWebpage,
}

impl collector::Doc for PrecisionRankingWebpage {
    fn score(&self) -> f64 {
        self.ranking.score
    }

    fn hashes(&self) -> collector::Hashes {
        self.ranking.pointer.hashes
    }
}

impl RankableWebpage for PrecisionRankingWebpage {
    fn set_score(&mut self, score: f64) {
        self.ranking.score = score;
    }

    fn boost(&self) -> Option<f64> {
        self.ranking.optic_boost
    }
}

impl PrecisionRankingWebpage {
    pub fn new(retrieved_webpage: RetrievedWebpage, ranking: RecallRankingWebpage) -> Self {
        Self {
            retrieved_webpage,
            ranking,
        }
    }

    pub fn into_retrieved_webpage(self) -> RetrievedWebpage {
        self.retrieved_webpage
    }
}

impl RankingPipeline<PrecisionRankingWebpage> {
    fn create_reranking_stage<M: CrossEncoder + 'static>(
        crossencoder: Option<Arc<M>>,
        lambda: Option<Arc<LambdaMART>>,
        collector_config: CollectorConfig,
        top_n_considered: usize,
    ) -> Result<Self> {
        let scorer = match crossencoder {
            Some(cross_encoder) => Box::new(ReRanker::new(cross_encoder, lambda))
                as Box<dyn Scorer<PrecisionRankingWebpage>>,
            None => Box::new(IdentityScorer) as Box<dyn Scorer<PrecisionRankingWebpage>>,
        };

        let stage = RankingStage {
            scorer,
            stage_top_n: top_n_considered,
            derank_similar: true,
        };

        Ok(Self {
            stage,
            page: 0,
            top_n: 0,
            collector_config,
        })
    }

    pub fn reranker<M: CrossEncoder + 'static>(
        query: &mut SearchQuery,
        crossencoder: Option<Arc<M>>,
        lambda: Option<Arc<LambdaMART>>,
        collector_config: CollectorConfig,
        top_n_considered: usize,
    ) -> Result<Self> {
        let mut pipeline =
            Self::create_reranking_stage(crossencoder, lambda, collector_config, top_n_considered)?;
        pipeline.set_query_info(query);

        Ok(pipeline)
    }
}
