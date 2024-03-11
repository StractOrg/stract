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

use anyhow::anyhow;
use candle_core::{Device, Tensor};
use candle_nn::VarBuilder;
use std::path::Path;

use crate::{
    models::bert::{self, BertModel},
    Result,
};
use tokenizers::{PaddingParams, TruncationParams};

pub struct DualEncoder {
    model: BertModel,
    tokenizer: tokenizers::Tokenizer,
    device: Device,
    dtype: candle_core::DType,
    config: bert::Config,
}

impl DualEncoder {
    pub fn open<P: AsRef<Path>>(folder: P) -> Result<Self> {
        let device = Device::Cpu;
        let dtype = candle_core::DType::F16;

        let truncation = TruncationParams {
            max_length: 256,
            ..Default::default()
        };

        let padding = PaddingParams {
            ..Default::default()
        };

        let mut tokenizer =
            tokenizers::Tokenizer::from_file(folder.as_ref().join("tokenizer.json")).unwrap();

        tokenizer.with_truncation(Some(truncation)).unwrap();
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

        // all tensors can be loaded with (useful for debugging):
        // candle_core::safetensors::load(folder.as_ref().join("model.safetensors"), &device)

        let mut model = BertModel::load(vb, &config)?;
        model.set_pooler(None); // model should use mean pooling

        Ok(Self {
            model,
            tokenizer,
            device,
            dtype,
            config,
        })
    }

    pub fn embed(&self, texts: &[String]) -> Result<Tensor> {
        let enc = self
            .tokenizer
            .encode_batch(texts.to_vec(), true)
            .map_err(|e| anyhow!(e))?;

        let ids = enc
            .iter()
            .map(|enc| Tensor::new(enc.get_ids(), &self.device).map_err(|e| anyhow!(e)))
            .collect::<Result<Vec<_>>>()?;

        let input_ids = Tensor::stack(&ids, 0)?;

        let token_type_ids = input_ids.zeros_like()?;

        let attention_mask = enc
            .iter()
            .map(|enc| Tensor::new(enc.get_attention_mask(), &self.device).map_err(|e| anyhow!(e)))
            .collect::<Result<Vec<_>>>()?;
        let attention_mask = Tensor::stack(&attention_mask, 0)?.to_dtype(self.dtype)?;

        let emb = self
            .model
            .forward(&input_ids, &token_type_ids, &attention_mask)?;

        let (_n_sentence, n_tokens, _hidden_size) = emb.dims3()?;

        let emb = (emb.sum(1)? / (n_tokens as f64))?; // mean pooling
        let emb = emb.broadcast_div(&emb.sqr()?.sum_keepdim(1)?.sqrt()?)?; // l2 normalization

        Ok(emb)
    }

    pub fn hidden_size(&self) -> usize {
        self.config.hidden_size
    }
}
