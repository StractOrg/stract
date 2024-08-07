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
    enum_map::EnumMap,
    inverted_index::WebpagePointer,
    models::dual_encoder::DualEncoder,
    numericalfield_reader,
    ranking::{
        bitvec_similarity, inbound_similarity,
        models::lambdamart::LambdaMART,
        pipeline::{RankableWebpage, RankingPipeline, RankingStage, Recall, Scorer},
        SignalComputer, SignalEnum,
    },
    schema::numerical_field,
    searcher::{api, SearchQuery},
    webgraph,
};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct StoredEmbeddings(Vec<u8>);

impl StoredEmbeddings {
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct RecallRankingWebpage {
    local: LocalRecallRankingWebpage,
    inbound_edges: bitvec_similarity::BitVec,
}

impl RecallRankingWebpage {
    pub fn new(local: LocalRecallRankingWebpage, inbound_edges: bitvec_similarity::BitVec) -> Self {
        RecallRankingWebpage {
            local,
            inbound_edges,
        }
    }

    pub fn pointer(&self) -> &WebpagePointer {
        self.local.pointer()
    }

    pub fn title_embedding(&self) -> Option<&StoredEmbeddings> {
        self.local.title_embedding()
    }

    pub fn keyword_embedding(&self) -> Option<&StoredEmbeddings> {
        self.local.keyword_embedding()
    }

    pub fn score(&self) -> f64 {
        self.local.score()
    }

    pub fn signals(&self) -> &EnumMap<SignalEnum, f64> {
        self.local.signals()
    }

    pub fn signals_mut(&mut self) -> &mut EnumMap<SignalEnum, f64> {
        self.local.mut_signals()
    }

    pub fn boost(&self) -> Option<f64> {
        self.local.boost()
    }

    pub fn set_score(&mut self, score: f64) {
        self.local.set_score(score)
    }

    pub fn inbound_edges(&self) -> &bitvec_similarity::BitVec {
        &self.inbound_edges
    }

    pub fn host_id(&self) -> &webgraph::NodeID {
        self.local.host_id()
    }
}

impl collector::Doc for RecallRankingWebpage {
    fn score(&self) -> f64 {
        self.local.score()
    }

    fn hashes(&self) -> collector::Hashes {
        self.local.pointer().hashes
    }
}

impl RankableWebpage for RecallRankingWebpage {
    fn set_score(&mut self, score: f64) {
        self.local.set_score(score);
    }

    fn boost(&self) -> Option<f64> {
        self.local.boost()
    }

    fn signals(&self) -> &EnumMap<SignalEnum, f64> {
        self.local.signals()
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct LocalRecallRankingWebpage {
    pointer: WebpagePointer,
    signals: EnumMap<SignalEnum, f64>,
    optic_boost: Option<f64>,
    title_embedding: Option<StoredEmbeddings>,
    keyword_embedding: Option<StoredEmbeddings>,
    score: f64,
    host_id: webgraph::NodeID,
}

impl LocalRecallRankingWebpage {
    #[cfg(test)]
    pub fn new_testing(
        pointer: WebpagePointer,
        signals: EnumMap<SignalEnum, f64>,
        score: f64,
    ) -> Self {
        LocalRecallRankingWebpage {
            pointer,
            signals,
            optic_boost: None,
            title_embedding: None,
            keyword_embedding: None,
            score,
            host_id: webgraph::NodeID::from(0u64),
        }
    }

    /// The ranking webpages needs to be constructed in order
    /// of ascending doc_id as they traverse the posting lists from
    /// the index to calculate bm25.
    pub fn new(
        pointer: WebpagePointer,
        columnfield_reader: &numericalfield_reader::SegmentReader,
        computer: &mut SignalComputer,
    ) -> Self {
        let columnfields = columnfield_reader.get_field_reader(pointer.address.doc_id);

        let title_embedding: Option<Vec<u8>> = columnfields
            .get(numerical_field::TitleEmbeddings.into())
            .and_then(|v| v.into());

        let keyword_embedding: Option<Vec<u8>> = columnfields
            .get(numerical_field::KeywordEmbeddings.into())
            .and_then(|v| v.into());

        let host_id = columnfields
            .get(numerical_field::HostNodeID.into())
            .unwrap()
            .as_u64()
            .unwrap()
            .into();

        let mut res = LocalRecallRankingWebpage {
            signals: EnumMap::new(),
            score: pointer.score.total,
            optic_boost: None,
            pointer: pointer.clone(),
            title_embedding: title_embedding.map(StoredEmbeddings),
            keyword_embedding: keyword_embedding.map(StoredEmbeddings),
            host_id,
        };

        for computed_signal in computer.compute_signals(pointer.address.doc_id) {
            res.signals
                .insert(computed_signal.signal, computed_signal.score);
        }

        if let Some(boost) = computer.boosts(pointer.address.doc_id) {
            res.optic_boost = Some(boost);
        }

        res
    }

    pub fn pointer(&self) -> &WebpagePointer {
        &self.pointer
    }

    pub fn title_embedding(&self) -> Option<&StoredEmbeddings> {
        self.title_embedding.as_ref()
    }

    pub fn keyword_embedding(&self) -> Option<&StoredEmbeddings> {
        self.keyword_embedding.as_ref()
    }

    pub fn score(&self) -> f64 {
        self.score
    }

    pub fn signals(&self) -> &EnumMap<SignalEnum, f64> {
        &self.signals
    }

    pub fn mut_signals(&mut self) -> &mut EnumMap<SignalEnum, f64> {
        &mut self.signals
    }

    pub fn boost(&self) -> Option<f64> {
        self.optic_boost
    }

    pub fn set_score(&mut self, score: f64) {
        self.score = score;
    }

    pub fn host_id(&self) -> &webgraph::NodeID {
        &self.host_id
    }
}

impl RankableWebpage for LocalRecallRankingWebpage {
    fn set_score(&mut self, score: f64) {
        self.score = score;
    }

    fn boost(&self) -> Option<f64> {
        self.optic_boost
    }

    fn signals(&self) -> &EnumMap<SignalEnum, f64> {
        &self.signals
    }
}

impl collector::Doc for LocalRecallRankingWebpage {
    fn score(&self) -> f64 {
        self.score
    }

    fn hashes(&self) -> collector::Hashes {
        self.pointer.hashes
    }
}

impl RankingPipeline<LocalRecallRankingWebpage> {
    fn create_recall_stage(
        lambdamart: Option<Arc<LambdaMART>>,
        dual_encoder: Option<Arc<DualEncoder>>,
        collector_config: CollectorConfig,
        stage_top_n: usize,
    ) -> Self {
        let last_stage = RankingStage {
            scorer: Box::new(Recall::<LocalRecallRankingWebpage>::new(dual_encoder))
                as Box<dyn Scorer<LocalRecallRankingWebpage>>,
            stage_top_n,
            derank_similar: true,
            model: lambdamart,
            coefficients: Default::default(),
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

impl RankingPipeline<api::ScoredWebpagePointer> {
    fn create_recall_stage(
        inbound: inbound_similarity::Scorer,
        lambdamart: Option<Arc<LambdaMART>>,
        dual_encoder: Option<Arc<DualEncoder>>,
        collector_config: CollectorConfig,
        stage_top_n: usize,
    ) -> Self {
        let last_stage = RankingStage {
            scorer: Box::new(Recall::<api::ScoredWebpagePointer>::new(
                inbound,
                dual_encoder,
            )),
            stage_top_n,
            derank_similar: true,
            model: lambdamart,
            coefficients: Default::default(),
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
        inbound: inbound_similarity::Scorer,
        lambdamart: Option<Arc<LambdaMART>>,
        dual_encoder: Option<Arc<DualEncoder>>,
        collector_config: CollectorConfig,
        top_n_considered: usize,
    ) -> Self {
        let mut pipeline = Self::create_recall_stage(
            inbound,
            lambdamart,
            dual_encoder,
            collector_config,
            top_n_considered,
        );
        pipeline.set_query_info(query);

        pipeline
    }
}
