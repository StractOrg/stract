// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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

use std::{path::Path, sync::Mutex};

use itertools::Itertools;
use onnxruntime::{ndarray::ArrayBase, tensor::OrtOwnedTensor, GraphOptimizationLevel};

use crate::Result;

use super::ONNX_ENVIRONMENT;
const TRUNCATE_INPUT: usize = 256;

pub struct CrossEncoderModel {
    tokenizer: tokenizers::Tokenizer,
    session: Mutex<onnxruntime::session::Session<'static>>,
}

impl CrossEncoderModel {
    pub fn open<P: AsRef<Path>>(folder: P) -> Result<Self> {
        let tokenizer = tokenizers::Tokenizer::from_file(folder.as_ref().join("tokenizer.json"))?;

        let session = Mutex::new(
            ONNX_ENVIRONMENT
                .new_session_builder()?
                .with_optimization_level(GraphOptimizationLevel::All)?
                .with_number_threads(1)?
                .with_model_from_file(folder.as_ref().join("model_quantized.onnx"))?,
        );

        Ok(Self { tokenizer, session })
    }
}

pub trait CrossEncoder: Send + Sync {
    fn run(&self, query: &str, body: &str) -> f64;
}

pub struct DummyCrossEncoder {}

impl CrossEncoder for DummyCrossEncoder {
    fn run(&self, _query: &str, _body: &str) -> f64 {
        1.0
    }
}

impl CrossEncoder for CrossEncoderModel {
    fn run(&self, query: &str, body: &str) -> f64 {
        let body: String = body.split_whitespace().take(TRUNCATE_INPUT).collect();
        let encoded = self.tokenizer.encode((query, body), true).unwrap();

        let ids = encoded
            .get_ids()
            .iter()
            .map(|i| *i as i64)
            .take(TRUNCATE_INPUT)
            .collect_vec();

        let attention_mask = encoded
            .get_attention_mask()
            .iter()
            .take(TRUNCATE_INPUT)
            .map(|i| *i as i64)
            .collect_vec();

        let type_ids = encoded
            .get_type_ids()
            .iter()
            .take(TRUNCATE_INPUT)
            .map(|i| *i as i64)
            .collect_vec();

        let num_tokens = ids.len();

        let mut sess = self.session.lock().unwrap();

        let res: Vec<OrtOwnedTensor<f32, _>> = sess
            .run(vec![
                ArrayBase::from_vec(ids)
                    .into_shape((1, num_tokens))
                    .unwrap(),
                ArrayBase::from_vec(attention_mask)
                    .into_shape((1, num_tokens))
                    .unwrap(),
                ArrayBase::from_vec(type_ids)
                    .into_shape((1, num_tokens))
                    .unwrap(),
            ])
            .unwrap();

        let res = res[0][[0, 0]] as f64;

        let s = res.exp();
        s / (s + 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity_check() {
        match CrossEncoderModel::open("../data/cross_encoder") {
            Ok(model) => {
                let s = model.run(
                    "how many people live in paris",
                    "there are currently 1234 people living in paris",
                );

                for _ in 0..10 {
                    assert_eq!(
                        s,
                        model.run(
                            "how many people live in paris",
                            "there are currently 1234 people living in paris"
                        )
                    );
                }

                assert!(
                    model.run(
                        "how many people live in paris",
                        "there are currently 1234 people living in paris"
                    ) > model.run(
                        "how many people live in paris",
                        "I really like cake and my favorite cake is probably brownie"
                    )
                );
            }
            Err(err) => {
                dbg!(err);
                panic!();
            }
        }
    }
}
