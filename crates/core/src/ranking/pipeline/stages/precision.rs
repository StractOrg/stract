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

//! The precision stage of the ranking pipeline.
//!
//! This stage focusses on refining the first page of results
//! from the recall stage.

use std::sync::Arc;

use crate::{
    collector,
    enum_map::EnumMap,
    inverted_index::RetrievedWebpage,
    ranking::{
        models::{self, cross_encoder::CrossEncoder},
        pipeline::{
            scorers::lambdamart::PrecisionLambda, RankableWebpage, RankingPipeline, ReRanker,
        },
        SignalCalculation, SignalEnum,
    },
    searcher::SearchQuery,
};

use super::RecallRankingWebpage;

#[derive(Clone, Debug, bincode::Encode, bincode::Decode)]
pub struct PrecisionRankingWebpage {
    retrieved_webpage: RetrievedWebpage,
    ranking: RecallRankingWebpage,
}

impl PrecisionRankingWebpage {
    pub fn retrieved_webpage(&self) -> &RetrievedWebpage {
        &self.retrieved_webpage
    }

    pub fn ranking(&self) -> &RecallRankingWebpage {
        &self.ranking
    }

    pub fn ranking_mut(&mut self) -> &mut RecallRankingWebpage {
        &mut self.ranking
    }
}

impl collector::Doc for PrecisionRankingWebpage {
    fn score(&self) -> f64 {
        RankableWebpage::score(self)
    }

    fn hashes(&self) -> collector::Hashes {
        self.ranking.pointer().hashes
    }
}

impl RankableWebpage for PrecisionRankingWebpage {
    fn set_raw_score(&mut self, score: f64) {
        self.ranking.set_raw_score(score);
    }

    fn unboosted_score(&self) -> f64 {
        self.ranking.unboosted_score()
    }

    fn boost(&self) -> f64 {
        self.ranking.boost()
    }

    fn set_boost(&mut self, boost: f64) {
        self.ranking.set_boost(boost)
    }

    fn signals(&self) -> &EnumMap<SignalEnum, SignalCalculation> {
        self.ranking.signals()
    }

    fn signals_mut(&mut self) -> &mut EnumMap<SignalEnum, SignalCalculation> {
        self.ranking.signals_mut()
    }

    fn as_local_recall(&self) -> &super::LocalRecallRankingWebpage {
        self.ranking.as_local_recall()
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
    pub fn reranker<M: CrossEncoder + 'static>(
        query: &SearchQuery,
        crossencoder: Arc<M>,
        lambda: Option<Arc<models::LambdaMART>>,
    ) -> Self {
        let mut s = Self::new().add_stage(ReRanker::new(query.text().to_string(), crossencoder));

        if let Some(lambda) = lambda {
            let lambda = PrecisionLambda::from(lambda);
            s = s.add_stage(lambda);
        }

        s
    }
}
