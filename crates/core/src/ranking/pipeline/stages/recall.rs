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
    enum_map::EnumMap,
    inverted_index::WebpagePointer,
    models::dual_encoder::DualEncoder,
    numericalfield_reader,
    ranking::{
        bitvec_similarity, inbound_similarity,
        models::lambdamart::LambdaMART,
        pipeline::{
            modifiers,
            scorers::{
                embedding::{EmbeddingScorer, KeywordEmbeddings, TitleEmbeddings},
                inbound_similarity::InboundScorer,
                term_distance,
            },
            RankableWebpage, RankingPipeline,
        },
        SignalCalculation, SignalComputer, SignalEnum,
    },
    schema::{numerical_field, text_field},
    searcher::{ScoredWebpagePointer, SearchQuery},
    webgraph,
};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct StoredEmbeddings(Vec<u8>);

impl StoredEmbeddings {
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}
#[derive(Clone, Debug, bincode::Encode, bincode::Decode)]
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
    fn set_raw_score(&mut self, score: f64) {
        self.local.set_score(score);
    }

    fn unboosted_score(&self) -> f64 {
        self.local.unboosted_score()
    }

    fn boost(&self) -> f64 {
        self.local.boost()
    }

    fn set_boost(&mut self, boost: f64) {
        self.local.set_boost(boost)
    }

    fn signals(&self) -> &EnumMap<SignalEnum, SignalCalculation> {
        self.local.signals()
    }

    fn signals_mut(&mut self) -> &mut EnumMap<SignalEnum, SignalCalculation> {
        self.local.signals_mut()
    }

    fn as_local_recall(&self) -> &LocalRecallRankingWebpage {
        &self.local
    }
}

#[derive(Clone, Debug, bincode::Encode, bincode::Decode)]
pub struct LocalRecallRankingWebpage {
    pointer: WebpagePointer,
    signals: EnumMap<SignalEnum, SignalCalculation>,
    title_positions: Vec<Vec<u32>>,
    clean_body_positions: Vec<Vec<u32>>,
    boost: f64,
    title_embedding: Option<StoredEmbeddings>,
    keyword_embedding: Option<StoredEmbeddings>,
    score: f64,
    host_id: webgraph::NodeID,
}

impl LocalRecallRankingWebpage {
    #[cfg(test)]
    pub fn new_testing(
        pointer: WebpagePointer,
        signals: EnumMap<SignalEnum, SignalCalculation>,
        score: f64,
    ) -> Self {
        LocalRecallRankingWebpage {
            pointer,
            signals,
            title_positions: Vec::new(),
            clean_body_positions: Vec::new(),
            boost: 1.0,
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
        columnfield_reader: &mut numericalfield_reader::SegmentReader,
        computer: &mut SignalComputer,
    ) -> Self {
        columnfield_reader.prepare_row_for_doc(pointer.address.doc_id);
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
            .as_u128()
            .unwrap()
            .into();

        let title_positions = computer
            .get_field_positions(text_field::Title.into(), pointer.address.doc_id)
            .unwrap_or_default();

        let clean_body_positions = computer
            .get_field_positions(text_field::CleanBody.into(), pointer.address.doc_id)
            .unwrap_or_default();

        let mut res = LocalRecallRankingWebpage {
            signals: EnumMap::new(),
            score: pointer.score.total,
            boost: 1.0,
            pointer: pointer.clone(),
            title_embedding: title_embedding.map(StoredEmbeddings),
            keyword_embedding: keyword_embedding.map(StoredEmbeddings),
            title_positions,
            clean_body_positions,
            host_id,
        };

        for computed_signal in computer.compute_signals(pointer.address.doc_id) {
            res.signals
                .insert(computed_signal.signal, computed_signal.calc);
        }

        if let Some(boost) = computer.boosts(pointer.address.doc_id) {
            res.boost *= boost;
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
        self.score * self.boost
    }

    pub fn signals(&self) -> &EnumMap<SignalEnum, SignalCalculation> {
        &self.signals
    }

    pub fn signals_mut(&mut self) -> &mut EnumMap<SignalEnum, SignalCalculation> {
        &mut self.signals
    }

    pub fn set_score(&mut self, score: f64) {
        self.score = score;
    }

    pub fn host_id(&self) -> &webgraph::NodeID {
        &self.host_id
    }

    pub fn iter_title_positions(&self) -> impl Iterator<Item = &[u32]> {
        self.title_positions.iter().map(|v| v.as_slice())
    }

    pub fn iter_clean_body_positions(&self) -> impl Iterator<Item = &[u32]> {
        self.clean_body_positions.iter().map(|v| v.as_slice())
    }
}

impl RankableWebpage for LocalRecallRankingWebpage {
    fn set_raw_score(&mut self, score: f64) {
        self.score = score;
    }

    fn unboosted_score(&self) -> f64 {
        self.score
    }

    fn boost(&self) -> f64 {
        self.boost
    }

    fn set_boost(&mut self, boost: f64) {
        self.boost = boost;
    }

    fn signals(&self) -> &EnumMap<SignalEnum, SignalCalculation> {
        &self.signals
    }

    fn signals_mut(&mut self) -> &mut EnumMap<SignalEnum, SignalCalculation> {
        &mut self.signals
    }

    fn as_local_recall(&self) -> &LocalRecallRankingWebpage {
        self
    }
}

impl collector::Doc for LocalRecallRankingWebpage {
    fn score(&self) -> f64 {
        RankableWebpage::score(self)
    }

    fn hashes(&self) -> collector::Hashes {
        self.pointer.hashes
    }
}

impl RankingPipeline<ScoredWebpagePointer> {
    pub fn recall_stage(
        query: &SearchQuery,
        inbound: inbound_similarity::Scorer,
        lambdamart: Option<Arc<LambdaMART>>,
        dual_encoder: Option<Arc<DualEncoder>>,
    ) -> Self {
        let mut s = Self::new()
            .add_stage(term_distance::TitleDistanceScorer)
            .add_stage(term_distance::BodyDistanceScorer)
            .add_stage(
                EmbeddingScorer::<ScoredWebpagePointer, TitleEmbeddings>::new(
                    query.text().to_string(),
                    dual_encoder.clone(),
                ),
            )
            .add_stage(
                EmbeddingScorer::<ScoredWebpagePointer, KeywordEmbeddings>::new(
                    query.text().to_string(),
                    dual_encoder,
                ),
            )
            .add_stage(InboundScorer::new(inbound))
            .add_modifier(modifiers::InboundSimilarity);

        if let Some(lambda) = lambdamart {
            s = s.add_stage(lambda);
        }

        s
    }
}
