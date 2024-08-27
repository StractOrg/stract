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

use candle_core::Tensor;

use crate::{
    models::dual_encoder::DualEncoder,
    ranking::{
        self,
        pipeline::{stages::StoredEmbeddings, RankableWebpage},
        SignalEnum,
    },
    searcher::api::ScoredWebpagePointer,
    Result,
};

use super::FullRankingStage;

pub struct Embedding(Tensor);

impl Embedding {
    pub fn dot(&self, other: &Self) -> Result<f64> {
        Ok(self
            .0
            .to_dtype(candle_core::DType::F16)?
            .unsqueeze(0)?
            .matmul(
                &other
                    .0
                    .to_dtype(candle_core::DType::F16)?
                    .unsqueeze(0)?
                    .t()?,
            )?
            .get(0)?
            .squeeze(0)?
            .to_dtype(candle_core::DType::F64)?
            .to_vec0()?)
    }

    pub fn size(&self) -> usize {
        self.0.dims1().unwrap()
    }
}

impl Embedding {
    fn new(stored: &StoredEmbeddings, size: usize) -> Result<Self> {
        let tensor = Tensor::from_raw_buffer(
            stored.as_slice(),
            candle_core::DType::BF16,
            &[size],
            &candle_core::Device::Cpu,
        )?;

        Ok(Self(tensor))
    }
}

pub struct EmbeddingScorer<W, E: EmbeddingSignal<W>> {
    dual_encoder: Option<Arc<DualEncoder>>,
    query: String,
    _marker: std::marker::PhantomData<(E, W)>,
}

impl<W, E: EmbeddingSignal<W>> EmbeddingScorer<W, E> {
    pub fn new(query: String, dual_encoder: Option<Arc<DualEncoder>>) -> Self {
        Self {
            dual_encoder,
            query,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<W, E: EmbeddingSignal<W>> EmbeddingScorer<W, E> {
    pub fn query_emb(&self) -> Option<Embedding> {
        self.dual_encoder.as_ref().and_then(|dual_encoder| {
            dual_encoder
                .embed(&[self.query.clone()])
                .ok()
                .and_then(|d| d.squeeze(0).ok())
                .map(Embedding)
        })
    }
}

impl ScoredWebpagePointer {
    fn title_emb(&self, hidden_size: usize) -> Option<Embedding> {
        self.as_ranking()
            .title_embedding()
            .as_ref()
            .and_then(|s| Embedding::new(s, hidden_size).ok())
    }

    fn keyword_emb(&self, hidden_size: usize) -> Option<Embedding> {
        self.as_ranking()
            .keyword_embedding()
            .as_ref()
            .and_then(|s| Embedding::new(s, hidden_size).ok())
    }
}

impl<E: EmbeddingSignal<ScoredWebpagePointer>> FullRankingStage
    for EmbeddingScorer<ScoredWebpagePointer, E>
{
    type Webpage = ScoredWebpagePointer;

    fn compute(&self, webpages: &mut [Self::Webpage]) {
        if !webpages.iter().any(E::has_embedding) {
            return;
        }

        if let Some(query_emb) = self.query_emb() {
            let hidden_size = query_emb.size();
            for webpage in webpages.iter_mut() {
                if let Some(emb) = E::embedding(webpage, hidden_size) {
                    let sim = query_emb.dot(&emb).unwrap_or_default();
                    E::insert_signal(webpage, sim);
                }
            }
        }
    }
}

pub struct TitleEmbeddings;
pub struct KeywordEmbeddings;

pub trait EmbeddingSignal<W>: Send + Sync {
    fn signal() -> SignalEnum;
    fn has_embedding(webpage: &W) -> bool;
    fn embedding(webpage: &W, hidden_size: usize) -> Option<Embedding>;
    fn insert_signal(webpage: &mut W, score: f64);
}

impl EmbeddingSignal<ScoredWebpagePointer> for TitleEmbeddings {
    fn signal() -> SignalEnum {
        ranking::core::TitleEmbeddingSimilarity.into()
    }

    fn has_embedding(webpage: &ScoredWebpagePointer) -> bool {
        webpage.as_ranking().title_embedding().is_some()
    }

    fn embedding(webpage: &ScoredWebpagePointer, hidden_size: usize) -> Option<Embedding> {
        webpage.title_emb(hidden_size)
    }

    fn insert_signal(webpage: &mut ScoredWebpagePointer, score: f64) {
        let sig = <TitleEmbeddings as EmbeddingSignal<ScoredWebpagePointer>>::signal();
        webpage
            .as_ranking_mut()
            .signals_mut()
            .insert(sig, ranking::SignalCalculation::new_symmetrical(score));
    }
}

impl EmbeddingSignal<ScoredWebpagePointer> for KeywordEmbeddings {
    fn signal() -> SignalEnum {
        ranking::core::KeywordEmbeddingSimilarity.into()
    }

    fn has_embedding(webpage: &ScoredWebpagePointer) -> bool {
        webpage.as_ranking().keyword_embedding().is_some()
    }

    fn embedding(webpage: &ScoredWebpagePointer, hidden_size: usize) -> Option<Embedding> {
        webpage.keyword_emb(hidden_size)
    }

    fn insert_signal(webpage: &mut ScoredWebpagePointer, score: f64) {
        let sig = <KeywordEmbeddings as EmbeddingSignal<ScoredWebpagePointer>>::signal();
        webpage
            .as_ranking_mut()
            .signals_mut()
            .insert(sig, ranking::SignalCalculation::new_symmetrical(score));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedding_dot() {
        let emb1 = Embedding(
            Tensor::from_vec(vec![1.0, 2.0, 3.0], &[3], &candle_core::Device::Cpu).unwrap(),
        );
        let emb2 = Embedding(
            Tensor::from_vec(vec![4.0, 5.0, 6.0], &[3], &candle_core::Device::Cpu).unwrap(),
        );

        assert_eq!(emb1.dot(&emb2).unwrap(), 32.0);
    }
}
