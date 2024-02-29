// Stract is an open source web search engine.
// Copyright (C) 2023 Stract ApS
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

use anyhow::anyhow;
use anyhow::Result;
use candle_core::Module;
use candle_core::{Device, Tensor};
use candle_nn::Linear;
use candle_nn::VarBuilder;
use std::path::Path;
use tokenizers::PaddingParams;
use tokenizers::TruncationParams;

use crate::models::bert;
use crate::models::bert::BertModel;

const TRUNCATE_INPUT: usize = 128;

pub struct CrossEncoderModel {
    tokenizer: tokenizers::Tokenizer,
    encoder: BertModel,
    classifier: Linear,
    device: Device,
    dtype: candle_core::DType,
}

impl CrossEncoderModel {
    pub fn open<P: AsRef<Path>>(folder: P) -> Result<Self> {
        let device = Device::Cpu;
        let dtype = candle_core::DType::F16;

        let truncation = TruncationParams {
            max_length: TRUNCATE_INPUT,
            ..Default::default()
        };

        let padding = PaddingParams {
            ..Default::default()
        };

        let mut tokenizer =
            tokenizers::Tokenizer::from_file(folder.as_ref().join("tokenizer.json"))
                .map_err(|_| anyhow!("couldn't open tokenizer"))?;

        tokenizer
            .with_truncation(Some(truncation))
            .map_err(|_| anyhow!("tokenizer truncation settings"))?;
        tokenizer.with_padding(Some(padding));

        let vb = unsafe {
            VarBuilder::from_mmaped_safetensors(
                &[folder.as_ref().join("model.safetensors")],
                dtype,
                &device,
            )?
        };
        let config = std::fs::read_to_string(folder.as_ref().join("config.json"))?;
        let mut config: bert::Config = serde_json::from_str(&config)?;
        config.hidden_act = bert::HiddenAct::GeluApproximate;

        let classifier: Linear = candle_nn::linear(config.hidden_size, 1, vb.pp("classifier"))?;

        // all tensors can be loaded with (useful for debugging):
        // candle_core::safetensors::load(folder.as_ref().join("model.safetensors"), &device)

        let encoder = BertModel::load(vb, &config)?;

        Ok(Self {
            tokenizer,
            encoder,
            classifier,
            device,
            dtype,
        })
    }

    fn scores(&self, query: &str, bodies: &[String]) -> Vec<f64> {
        if bodies.is_empty() {
            return Vec::new();
        }

        let input: Vec<_> = bodies
            .iter()
            .map(|body| {
                (
                    query.to_string(),
                    body.split_whitespace()
                        .take(TRUNCATE_INPUT)
                        .collect::<String>(),
                )
            })
            .collect();

        let encoded = self.tokenizer.encode_batch(input, true).unwrap();

        let ids = encoded
            .iter()
            .map(|enc| Tensor::new(enc.get_ids(), &self.device).map_err(|e| anyhow!(e)))
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let input_ids = Tensor::stack(&ids, 0).unwrap();

        let token_type_ids = input_ids.zeros_like().unwrap();

        let attention_mask = encoded
            .iter()
            .map(|enc| Tensor::new(enc.get_attention_mask(), &self.device).map_err(|e| anyhow!(e)))
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let attention_mask = Tensor::stack(&attention_mask, 0)
            .unwrap()
            .to_dtype(self.dtype)
            .unwrap();

        let logits = self
            .encoder
            .forward(&input_ids, &token_type_ids, &attention_mask)
            .unwrap();

        let scores = self
            .classifier
            .forward(&logits)
            .unwrap()
            .squeeze(1)
            .unwrap()
            .to_dtype(candle_core::DType::F64)
            .unwrap();

        let scores = candle_nn::ops::sigmoid(&scores).unwrap();

        scores.to_vec1().unwrap()
    }
}

impl CrossEncoder for CrossEncoderModel {
    fn run(&self, query: &str, bodies: &[String]) -> Vec<f64> {
        let mut scores = self
            .scores(query, bodies)
            .into_iter()
            .enumerate()
            .collect::<Vec<_>>();

        scores.sort_by(|a, b| b.1.total_cmp(&a.1));

        let mut ranked_scores = scores
            .into_iter()
            .enumerate()
            .map(|(rank, (i, _))| (i, (1.0 / (rank as f64 + 1.0))))
            .collect::<Vec<_>>();

        ranked_scores.sort_by(|a, b| a.0.cmp(&b.0));
        ranked_scores.into_iter().map(|(_, score)| score).collect()
    }
}

pub trait CrossEncoder: Send + Sync {
    fn run(&self, query: &str, bodies: &[String]) -> Vec<f64>;
}

pub struct DummyCrossEncoder;

impl CrossEncoder for DummyCrossEncoder {
    fn run(&self, _query: &str, bodies: &[String]) -> Vec<f64> {
        vec![1.0; bodies.len()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity_check() {
        let data_path = Path::new("../../data/cross_encoder");
        if !data_path.exists() {
            // Skip the test if the test data is not available
            return;
        }
        let model = CrossEncoderModel::open(data_path).expect("Failed to find cross-encoder model");

        let s = model.run(
            "how many people live in paris",
            &["there are currently 1234 people living in paris".to_string()],
        );

        for _ in 0..10 {
            assert_eq!(
                s,
                model.run(
                    "how many people live in paris",
                    &["there are currently 1234 people living in paris".to_string()]
                )
            );
        }

        let res = model.run(
            "how many people live in paris",
            &[
                "there are currently 1234 people living in paris".to_string(),
                "I really like cake and my favorite cake is probably brownie".to_string(),
            ],
        );

        assert!(res[0] > res[1]);
    }
}
