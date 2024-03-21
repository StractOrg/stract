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
    fastfield_reader,
    inverted_index::WebpagePointer,
    models::dual_encoder::DualEncoder,
    ranking::{
        models::lambdamart::LambdaMART,
        pipeline::{RankableWebpage, RankingPipeline, RankingStage, Recall, Scorer},
        SignalComputer, SignalEnum, SignalScore,
    },
    schema::fast_field,
    searcher::SearchQuery,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredEmbeddings(Vec<u8>);

impl StoredEmbeddings {
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecallRankingWebpage {
    pub pointer: WebpagePointer,
    pub signals: EnumMap<SignalEnum, SignalScore>,
    pub optic_boost: Option<f64>,
    pub title_embedding: Option<StoredEmbeddings>,
    pub keyword_embedding: Option<StoredEmbeddings>,
    pub score: f64,
}

impl RecallRankingWebpage {
    pub fn new(
        pointer: WebpagePointer,
        fastfield_reader: &fastfield_reader::SegmentReader,
        computer: &mut SignalComputer,
    ) -> Self {
        let fastfields = fastfield_reader.get_field_reader(pointer.address.doc_id);

        let title_embedding: Option<Vec<u8>> = fastfields
            .get(fast_field::TitleEmbeddings.into())
            .and_then(|v| v.into());

        let keyword_embedding: Option<Vec<u8>> = fastfields
            .get(fast_field::KeywordEmbeddings.into())
            .and_then(|v| v.into());

        let mut res = RecallRankingWebpage {
            signals: EnumMap::new(),
            score: pointer.score.total,
            optic_boost: None,
            pointer: pointer.clone(),
            title_embedding: title_embedding.map(StoredEmbeddings),
            keyword_embedding: keyword_embedding.map(StoredEmbeddings),
        };

        for computed_signal in computer.compute_signals(pointer.address.doc_id).flatten() {
            res.signals
                .insert(computed_signal.signal, computed_signal.score);
        }

        if let Some(boost) = computer.boosts(pointer.address.doc_id) {
            res.optic_boost = Some(boost);
        }

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
        lambdamart: Option<Arc<LambdaMART>>,
        dual_encoder: Option<Arc<DualEncoder>>,
        collector_config: CollectorConfig,
        stage_top_n: usize,
    ) -> Self {
        let last_stage = RankingStage {
            scorer: Box::new(Recall::<RecallRankingWebpage>::new(
                lambdamart,
                dual_encoder,
            )) as Box<dyn Scorer<RecallRankingWebpage>>,
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
        lambdamart: Option<Arc<LambdaMART>>,
        dual_encoder: Option<Arc<DualEncoder>>,
        collector_config: CollectorConfig,
        top_n_considered: usize,
    ) -> Self {
        let mut pipeline =
            Self::create_recall_stage(lambdamart, dual_encoder, collector_config, top_n_considered);
        pipeline.set_query_info(query);

        pipeline
    }
}
