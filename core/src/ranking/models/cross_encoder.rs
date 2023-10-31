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
use std::path::Path;

#[cfg(feature = "libtorch")]
pub use model::*;

#[cfg(feature = "libtorch")]
mod model {
    use super::*;

    use itertools::Itertools;
    use tch::Tensor;
    use tokenizers::PaddingParams;
    use tokenizers::TruncationParams;

    const TRUNCATE_INPUT: usize = 128;

    pub struct CrossEncoderModel {
        tokenizer: tokenizers::Tokenizer,
        model: tch::CModule,
    }

    impl CrossEncoderModel {
        pub fn open(folder: &Path) -> Result<Self> {
            let truncation = TruncationParams {
                max_length: TRUNCATE_INPUT,
                ..Default::default()
            };

            let padding = PaddingParams {
                ..Default::default()
            };

            let mut tokenizer = tokenizers::Tokenizer::from_file(folder.join("tokenizer.json"))
                .map_err(|_| anyhow!("couldn't open tokenizer"))?;

            tokenizer
                .with_truncation(Some(truncation))
                .map_err(|_| anyhow!("tokenizer truncation settings"))?;
            tokenizer.with_padding(Some(padding));

            let model = tch::CModule::load(folder.join("model.pt"))?;

            Ok(Self { tokenizer, model })
        }
    }

    impl CrossEncoder for CrossEncoderModel {
        fn run(&self, query: &str, bodies: &[String]) -> Vec<f64> {
            if bodies.is_empty() {
                return Vec::new();
            }

            let bs = bodies.len();

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

            let num_tokens = encoded
                .iter()
                .map(|enc| enc.get_ids().len())
                .max()
                .unwrap_or(0);

            let ids = encoded
                .iter()
                .flat_map(|enc| enc.get_ids().iter().map(|i| *i as i64).take(TRUNCATE_INPUT))
                .collect_vec();

            let attention_mask = encoded
                .iter()
                .flat_map(|enc| {
                    enc.get_attention_mask()
                        .iter()
                        .map(|i| *i as i64)
                        .take(TRUNCATE_INPUT)
                })
                .collect_vec();

            let type_ids = encoded
                .iter()
                .flat_map(|enc| {
                    enc.get_type_ids()
                        .iter()
                        .map(|i| *i as i64)
                        .take(TRUNCATE_INPUT)
                })
                .collect_vec();

            let ids = Tensor::from_slice(&ids).reshape([bs as i64, num_tokens as i64]);
            let attention_mask =
                Tensor::from_slice(&attention_mask).reshape([bs as i64, num_tokens as i64]);
            let type_ids = Tensor::from_slice(&type_ids).reshape([bs as i64, num_tokens as i64]);

            let output = self
                .model
                .forward_ts(&[ids, attention_mask, type_ids])
                .unwrap();

            let mut res = Vec::with_capacity(bs);

            for i in 0..bs {
                let s = output.double_value(&[i as i64, 0]);
                let s = s.exp();
                let s = s / (s + 1.0);
                res.push(s);
            }

            res
        }
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
#[cfg(feature = "libtorch")]
mod tests {
    use super::*;

    #[test]
    fn sanity_check() {
        let model = CrossEncoderModel::open("../data/cross_encoder".as_ref())
            .expect("Failed to find cross-encoder model");

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

        assert!(
            model.run(
                "how many people live in paris",
                &["there are currently 1234 people living in paris".to_string()]
            )[0] > model.run(
                "how many people live in paris",
                &["I really like cake and my favorite cake is probably brownie".to_string()]
            )[0]
        );
    }
}
