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

use std::path::Path;

use itertools::Itertools;
use tch::Tensor;
use tokenizers::PaddingParams;
use tokenizers::TruncationParams;

const TRUNCATE_INPUT: usize = 512;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Tokenizer error")]
    Tokenizer(#[from] tokenizers::Error),
    #[error("Torch error")]
    Torch(#[from] tch::TchError),
}

pub struct FactCheckModel {
    tokenizer: tokenizers::Tokenizer,
    model: tch::CModule,
}

impl FactCheckModel {
    pub fn open<P: AsRef<Path>>(folder: P) -> Result<Self, Error> {
        let truncation = TruncationParams {
            max_length: TRUNCATE_INPUT,
            ..Default::default()
        };

        let padding = PaddingParams {
            ..Default::default()
        };

        let mut tokenizer =
            tokenizers::Tokenizer::from_file(folder.as_ref().join("tokenizer.json"))?;

        tokenizer.with_truncation(Some(truncation))?;
        tokenizer.with_padding(Some(padding));

        let model = tch::CModule::load(folder.as_ref().join("model.pt"))?;

        Ok(Self { tokenizer, model })
    }

    pub fn run(&self, claim: &str, evidence: &str) -> Result<f64, Error> {
        let encoded = self.tokenizer.encode((claim, evidence), true)?;

        let ids = encoded.get_ids().iter().map(|i| *i as i64).collect_vec();

        let attention_mask = encoded
            .get_attention_mask()
            .iter()
            .map(|i| *i as i64)
            .collect_vec();

        let ids = Tensor::from_slice(&ids).unsqueeze(0);
        let attention_mask = Tensor::from_slice(&attention_mask).unsqueeze(0);

        let output = self.model.forward_ts(&[ids, attention_mask])?;

        Ok(output.double_value(&[0, 0]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity_check() {
        let model =
            FactCheckModel::open("../data/fact_model").expect("Failed to find fact-check model");

        let score = model.run(
            "Albert Einstein work in the field of computer science",
            "Albert Einstein was a German-born theoretical physicist, widely acknowledged to be one of the greatest and most influential physicists of all time.",
        ).unwrap();

        assert!(score > 0.1);
    }
}
