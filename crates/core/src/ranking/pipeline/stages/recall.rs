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

use serde::{Deserialize, Serialize};

use crate::{
    collector,
    config::CollectorConfig,
    enum_map::EnumMap,
    inverted_index::WebpagePointer,
    ranking::{
        models::lambdamart::LambdaMART,
        pipeline::{Initial, RankableWebpage, RankingPipeline, RankingStage, Scorer},
        Signal, SignalAggregator, SignalScore,
    },
    schema::FastField,
    searcher::SearchQuery,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecallRankingWebpage {
    pub pointer: WebpagePointer,
    pub signals: EnumMap<Signal, SignalScore>,
    pub optic_boost: Option<f64>,
    pub title_embedding: Option<Vec<u8>>,
    pub score: f64,
}

impl RecallRankingWebpage {
    pub fn new(pointer: WebpagePointer, aggregator: &mut SignalAggregator) -> Self {
        let mut res = RecallRankingWebpage {
            signals: EnumMap::new(),
            score: pointer.score.total,
            optic_boost: None,
            pointer: pointer.clone(),
            title_embedding: None,
        };

        for computed_signal in aggregator.compute_signals(pointer.address.doc_id).flatten() {
            res.signals
                .insert(computed_signal.signal, computed_signal.score);
        }

        if let Some(boost) = aggregator.boosts(pointer.address.doc_id) {
            res.optic_boost = Some(boost);
        }

        res.title_embedding = aggregator
            .fastfield_readers()
            .unwrap()
            .get_field_reader(pointer.address.doc_id)
            .get(FastField::TitleEmbeddings)
            .into();

        res
    }
}
impl RankableWebpage for RecallRankingWebpage {
    fn set_score(&mut self, score: f64) {
        self.score = score;
    }

    fn boost(&self) -> Option<f64> {
        self.optic_boost
    }
}

impl collector::Doc for RecallRankingWebpage {
    fn score(&self) -> f64 {
        self.score
    }

    fn hashes(&self) -> collector::Hashes {
        self.pointer.hashes
    }
}

impl RankingPipeline<RecallRankingWebpage> {
    fn create_recall_stage(
        model: Option<Arc<LambdaMART>>,
        collector_config: CollectorConfig,
        stage_top_n: usize,
    ) -> Self {
        let last_stage = RankingStage {
            scorer: Box::new(Initial::new(model)) as Box<dyn Scorer<RecallRankingWebpage>>,
            stage_top_n,
            derank_similar: true,
        };

        Self {
            stage: last_stage,
            page: 0,
            top_n: 0,
            collector_config,
        }
    }

    pub fn recall_stage(
        query: &mut SearchQuery,
        model: Option<Arc<LambdaMART>>,
        collector_config: CollectorConfig,
        top_n_considered: usize,
    ) -> Self {
        let mut pipeline = Self::create_recall_stage(model, collector_config, top_n_considered);
        pipeline.set_query_info(query);

        pipeline
    }
}
