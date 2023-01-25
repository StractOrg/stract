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

use std::{cmp::Ordering, ops::Range, path::Path, sync::Mutex};

use crate::Result;
use itertools::Itertools;
use onnxruntime::{
    environment::Environment,
    ndarray::{Array2, Axis},
    tensor::OrtOwnedTensor,
    GraphOptimizationLevel, LoggingLevel,
};
use tokenizers::{PaddingParams, TruncationParams};

pub static ONNX_ENVIRONMENT: once_cell::sync::Lazy<Environment> =
    once_cell::sync::Lazy::new(|| {
        Environment::builder()
            .with_name("qa")
            .with_log_level(LoggingLevel::Info)
            .build()
            .unwrap()
    });

const TRUNCATE_INPUT: usize = 128;
const SCORE_THRESHOLD: f64 = 0.0;

pub struct QaModel {
    tokenizer: tokenizers::Tokenizer,
    session: Mutex<onnxruntime::session::Session<'static>>,
}

#[derive(Debug)]
pub struct Answer {
    pub offset: Range<usize>,
    pub context_idx: usize,
    pub score: f64,
}

impl QaModel {
    pub fn open<P: AsRef<Path>>(folder: P) -> Result<Self> {
        let truncation = TruncationParams {
            max_length: TRUNCATE_INPUT,
            ..Default::default()
        };
        let padding = PaddingParams {
            ..Default::default()
        };

        let mut tokenizer =
            tokenizers::Tokenizer::from_file(folder.as_ref().join("tokenizer.json"))?;
        tokenizer.with_truncation(Some(truncation));
        tokenizer.with_padding(Some(padding));

        let session = Mutex::new(
            ONNX_ENVIRONMENT
                .new_session_builder()?
                .with_optimization_level(GraphOptimizationLevel::All)?
                .with_number_threads(1)?
                .with_model_from_file(folder.as_ref().join("model_quantized.onnx"))?,
        );

        Ok(Self { tokenizer, session })
    }

    pub fn run(&self, question: &str, contexts: &[&str]) -> Option<Answer> {
        if contexts.is_empty() {
            return None;
        }

        let mut batches = Vec::with_capacity(contexts.len());

        for context in contexts {
            batches.push((question.to_string(), context.to_string()));
        }
        let encoded = self.tokenizer.encode_batch(batches, true).unwrap();

        let encoded_ids: Vec<_> = encoded
            .iter()
            .map(|encoding| encoding.get_ids().to_vec())
            .collect();

        let mut ids = Array2::zeros((encoded_ids.len(), encoded_ids.first().unwrap().len()));
        for i in 0..encoded_ids.len() {
            for j in 0..encoded_ids[i].len() {
                ids[[i, j]] = encoded_ids[i][j] as i64;
            }
        }

        let encoded_masks: Vec<_> = encoded
            .iter()
            .map(|encoding| encoding.get_attention_mask().to_vec())
            .collect();

        let mut masks = Array2::zeros((encoded_masks.len(), encoded_masks.first().unwrap().len()));
        for i in 0..encoded_masks.len() {
            for j in 0..encoded_masks[i].len() {
                masks[[i, j]] = encoded_masks[i][j] as i64;
            }
        }

        let mut sess = self.session.lock().unwrap();

        let mut res: Vec<OrtOwnedTensor<f32, _>> = sess.run(vec![ids, masks]).unwrap();

        if res.len() != 2 {
            return None;
        }

        let start_logits = res.remove(0);
        let end_logits = res.remove(0);

        if start_logits.shape()[0] != start_logits.shape()[0] {
            return None;
        }

        let mut best_answer: Option<Answer> = None;

        #[allow(clippy::needless_range_loop)]
        for idx in 0..start_logits.shape()[0] {
            let mut start: Vec<f32> = start_logits
                .index_axis(Axis(0), idx)
                .into_iter()
                .copied()
                .collect();
            softmax(&mut start);

            let mut end: Vec<f32> = end_logits
                .index_axis(Axis(0), idx)
                .into_iter()
                .copied()
                .collect();
            softmax(&mut end);

            let best_start = start
                .iter()
                .position_max_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
                .unwrap();
            let best_end = end
                .iter()
                .position_max_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
                .unwrap();

            if (best_start == 0 && best_end == 0) || (best_end <= best_start) {
                continue;
            }

            let score = (start[best_start] * end[best_end]) as f64;

            if score < SCORE_THRESHOLD {
                continue;
            }

            let offsets = encoded[idx].get_offsets();
            if offsets[best_end].1 <= offsets[best_start].0 {
                continue;
            }

            let offset = offsets[best_start].0..offsets[best_end].1;

            let candidate_answer = Answer {
                offset,
                context_idx: idx,
                score,
            };

            best_answer = match best_answer {
                Some(curr_best) => {
                    if candidate_answer.score > curr_best.score {
                        Some(candidate_answer)
                    } else {
                        Some(curr_best)
                    }
                }
                None => Some(candidate_answer),
            }
        }

        best_answer
    }
}

fn softmax(vec: &mut [f32]) {
    let s: f32 = vec.iter().map(|z| z.exp()).sum();

    for z in vec {
        *z = z.exp() / s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity() {
        let question = "Where is amazon rain forest located";
        let contexts = [
            r#"The Amazon rainforest (Portuguese: Floresta Amazônica or Amazônia; Spanish: Selva Amazónica, Amazonía or usually Amazonia; French: Forêt amazonienne; Dutch: Amazoneregenwoud), also known in English as Amazonia or the Amazon Jungle, is a moist broadleaf forest that covers most of the Amazon basin of South America. This basin encompasses 7,000,000 square kilometres (2,700,000 sq mi), of 
which 5,500,000 square kilometres (2,100,000 sq mi) are covered by the rainforest. This region includes territory belonging to nine nations. 
The majority of the forest is contained within Brazil, with 60% of the rainforest, followed by Peru with 13%, Colombia with 10%, and with 
minor amounts in Venezuela, Ecuador, Bolivia, Guyana, Suriname and French Guiana. States or departments in four nations contain 
"Amazonas" in their names. The Amazon represents over half of the planet's remaining rainforests, and comprises the largest and most biodiverse tract 
of tropical rainforest in the world, with an estimated 390 billion individual trees divided into 16,000 species."#,
        ];

        match QaModel::open("../data/qa_model") {
            Ok(model) => {
                let answer = model.run(question, &contexts).unwrap();

                assert!(answer.score > 0.6);
                assert_eq!(
                    &contexts[answer.context_idx][answer.offset],
                    "South America"
                );
            }
            Err(err) => {
                panic!("{err:?}");
            }
        }
    }

    #[test]
    fn truncated() {
        let question = "Where is amazon rain forest located";
        let contexts = [
            r#"The Amazon rainforest (Portuguese: Floresta Amazônica or Amazônia; Spanish: Selva Amazónica, Amazonía or usually Amazonia; French: Forêt amazonienne; Dutch: Amazoneregenwoud), also known in English as Amazonia or the Amazon Jungle, is a moist broadleaf forest that covers most of the Amazon basin of South America. This basin encompasses 7,000,000 square kilometres (2,700,000 sq mi), of 
which 5,500,000 square kilometres (2,100,000 sq mi) are covered by the rainforest. This region includes territory belonging to nine nations. 
The majority of the forest is contained within Brazil, with 60% of the rainforest, followed by Peru with 13%, Colombia with 10%, and with 
minor amounts in Venezuela, Ecuador, Bolivia, Guyana, Suriname and French Guiana. States or departments in four nations contain 
"Amazonas" in their names. The Amazon represents over half of the planet's remaining rainforests, and comprises the largest and most biodiverse tract 
of tropical rainforest in the world, with an estimated 390 billion individual trees divided into 16,000 species.
The Amazon rainforest (Portuguese: Floresta Amazônica or Amazônia; Spanish: Selva Amazónica, Amazonía or usually Amazonia; French: Forêt amazonienne; Dutch: Amazoneregenwoud), also known in English as Amazonia or the Amazon Jungle, is a moist broadleaf forest that covers most of the Amazon basin of South America. This basin encompasses 7,000,000 square kilometres (2,700,000 sq mi), of 
which 5,500,000 square kilometres (2,100,000 sq mi) are covered by the rainforest. This region includes territory belonging to nine nations. 
The majority of the forest is contained within Brazil, with 60% of the rainforest, followed by Peru with 13%, Colombia with 10%, and with 
minor amounts in Venezuela, Ecuador, Bolivia, Guyana, Suriname and French Guiana. States or departments in four nations contain 
"Amazonas" in their names. The Amazon represents over half of the planet's remaining rainforests, and comprises the largest and most biodiverse tract 
of tropical rainforest in the world, with an estimated 390 billion individual trees divided into 16,000 species.
The Amazon rainforest (Portuguese: Floresta Amazônica or Amazônia; Spanish: Selva Amazónica, Amazonía or usually Amazonia; French: Forêt amazonienne; Dutch: Amazoneregenwoud), also known in English as Amazonia or the Amazon Jungle, is a moist broadleaf forest that covers most of the Amazon basin of South America. This basin encompasses 7,000,000 square kilometres (2,700,000 sq mi), of 
which 5,500,000 square kilometres (2,100,000 sq mi) are covered by the rainforest. This region includes territory belonging to nine nations. 
The majority of the forest is contained within Brazil, with 60% of the rainforest, followed by Peru with 13%, Colombia with 10%, and with 
minor amounts in Venezuela, Ecuador, Bolivia, Guyana, Suriname and French Guiana. States or departments in four nations contain 
"Amazonas" in their names. The Amazon represents over half of the planet's remaining rainforests, and comprises the largest and most biodiverse tract 
of tropical rainforest in the world, with an estimated 390 billion individual trees divided into 16,000 species.
"#,
        ];

        match QaModel::open("../data/qa_model") {
            Ok(model) => {
                let answer = model.run(question, &contexts).unwrap();

                assert!(answer.score > 0.6);
                assert_eq!(
                    &contexts[answer.context_idx][answer.offset],
                    "South America"
                );
            }
            Err(err) => {
                panic!("{err:?}");
            }
        }
    }

    #[test]
    fn padding() {
        let question = "Where is amazon rain forest located";
        let contexts = [
            r#"The Amazon rainforest (Portuguese: Floresta Amazônica or Amazônia; Spanish: Selva Amazónica, Amazonía or usually Amazonia; French: Forêt amazonienne; Dutch: Amazoneregenwoud), also known in English as Amazonia or the Amazon Jungle, is a moist broadleaf forest that covers most of the Amazon basin of South America. This basin encompasses 7,000,000 square kilometres (2,700,000 sq mi), of"#,
            r#"The Amazon rainforest (Portuguese: Floresta Amazônica or Amazônia; Spanish: Selva Amazónica, Amazonía or usually Amazonia; French: Forêt amazonienne; Dutch: Amazoneregenwoud), also known in English as Amazonia or the Amazon Jungle, is a moist broadleaf forest that covers most of the Amazon basin of South America. This basin encompasses 7,000,000 square kilometres (2,700,000 sq mi), of 
which 5,500,000 square kilometres (2,100,000 sq mi) are covered by the rainforest. This region includes territory belonging to nine nations. 
The majority of the forest is contained within Brazil, with 60% of the rainforest, followed by Peru with 13%, Colombia with 10%, and with 
minor amounts in Venezuela, Ecuador, Bolivia, Guyana, Suriname and French Guiana. States or departments in four nations contain 
"Amazonas" in their names. The Amazon represents over half of the planet's remaining rainforests, and comprises the largest and most biodiverse tract 
of tropical rainforest in the world, with an estimated 390 billion individual trees divided into 16,000 species.
The Amazon rainforest (Portuguese: Floresta Amazônica or Amazônia; Spanish: Selva Amazónica, Amazonía or usually Amazonia; French: Forêt amazonienne; Dutch: Amazoneregenwoud), also known in English as Amazonia or the Amazon Jungle, is a moist broadleaf forest that covers most of the Amazon basin of South America. This basin encompasses 7,000,000 square kilometres (2,700,000 sq mi), of 
which 5,500,000 square kilometres (2,100,000 sq mi) are covered by the rainforest. This region includes territory belonging to nine nations. 
The majority of the forest is contained within Brazil, with 60% of the rainforest, followed by Peru with 13%, Colombia with 10%, and with 
minor amounts in Venezuela, Ecuador, Bolivia, Guyana, Suriname and French Guiana. States or departments in four nations contain 
"Amazonas" in their names. The Amazon represents over half of the planet's remaining rainforests, and comprises the largest and most biodiverse tract 
of tropical rainforest in the world, with an estimated 390 billion individual trees divided into 16,000 species.
The Amazon rainforest (Portuguese: Floresta Amazônica or Amazônia; Spanish: Selva Amazónica, Amazonía or usually Amazonia; French: Forêt amazonienne; Dutch: Amazoneregenwoud), also known in English as Amazonia or the Amazon Jungle, is a moist broadleaf forest that covers most of the Amazon basin of South America. This basin encompasses 7,000,000 square kilometres (2,700,000 sq mi), of 
which 5,500,000 square kilometres (2,100,000 sq mi) are covered by the rainforest. This region includes territory belonging to nine nations. 
The majority of the forest is contained within Brazil, with 60% of the rainforest, followed by Peru with 13%, Colombia with 10%, and with 
minor amounts in Venezuela, Ecuador, Bolivia, Guyana, Suriname and French Guiana. States or departments in four nations contain 
"Amazonas" in their names. The Amazon represents over half of the planet's remaining rainforests, and comprises the largest and most biodiverse tract 
of tropical rainforest in the world, with an estimated 390 billion individual trees divided into 16,000 species."#,
        ];

        match QaModel::open("../data/qa_model") {
            Ok(model) => {
                let answer = model.run(question, &contexts).unwrap();

                assert!(answer.score > 0.6);
                assert_eq!(
                    &contexts[answer.context_idx][answer.offset],
                    "South America"
                );
            }
            Err(err) => {
                panic!("{err:?}");
            }
        }
    }

    #[test]
    fn no_answer() {
        let question = "Who wrote smells like teen spirit";
        let contexts = [
            r#"The Amazon rainforest (Portuguese: Floresta Amazônica or Amazônia; Spanish: Selva Amazónica, Amazonía or usually Amazonia; French: Forêt amazonienne; Dutch: Amazoneregenwoud), also known in English as Amazonia or the Amazon Jungle, is a moist broadleaf forest that covers most of the Amazon basin of South America. This basin encompasses 7,000,000 square kilometres (2,700,000 sq mi), of 
which 5,500,000 square kilometres (2,100,000 sq mi) are covered by the rainforest. This region includes territory belonging to nine nations. 
The majority of the forest is contained within Brazil, with 60% of the rainforest, followed by Peru with 13%, Colombia with 10%, and with 
minor amounts in Venezuela, Ecuador, Bolivia, Guyana, Suriname and French Guiana. States or departments in four nations contain 
"Amazonas" in their names. The Amazon represents over half of the planet's remaining rainforests, and comprises the largest and most biodiverse tract 
of tropical rainforest in the world, with an estimated 390 billion individual trees divided into 16,000 species.
The Amazon rainforest (Portuguese: Floresta Amazônica or Amazônia; Spanish: Selva Amazónica, Amazonía or usually Amazonia; French: Forêt amazonienne; Dutch: Amazoneregenwoud), also known in English as Amazonia or the Amazon Jungle, is a moist broadleaf forest that covers most of the Amazon basin of South America. This basin encompasses 7,000,000 square kilometres (2,700,000 sq mi), of 
which 5,500,000 square kilometres (2,100,000 sq mi) are covered by the rainforest. This region includes territory belonging to nine nations. 
The majority of the forest is contained within Brazil, with 60% of the rainforest, followed by Peru with 13%, Colombia with 10%, and with 
minor amounts in Venezuela, Ecuador, Bolivia, Guyana, Suriname and French Guiana. States or departments in four nations contain 
"Amazonas" in their names. The Amazon represents over half of the planet's remaining rainforests, and comprises the largest and most biodiverse tract 
of tropical rainforest in the world, with an estimated 390 billion individual trees divided into 16,000 species.
The Amazon rainforest (Portuguese: Floresta Amazônica or Amazônia; Spanish: Selva Amazónica, Amazonía or usually Amazonia; French: Forêt amazonienne; Dutch: Amazoneregenwoud), also known in English as Amazonia or the Amazon Jungle, is a moist broadleaf forest that covers most of the Amazon basin of South America. This basin encompasses 7,000,000 square kilometres (2,700,000 sq mi), of 
which 5,500,000 square kilometres (2,100,000 sq mi) are covered by the rainforest. This region includes territory belonging to nine nations. 
The majority of the forest is contained within Brazil, with 60% of the rainforest, followed by Peru with 13%, Colombia with 10%, and with 
minor amounts in Venezuela, Ecuador, Bolivia, Guyana, Suriname and French Guiana. States or departments in four nations contain 
"Amazonas" in their names. The Amazon represents over half of the planet's remaining rainforests, and comprises the largest and most biodiverse tract 
of tropical rainforest in the world, with an estimated 390 billion individual trees divided into 16,000 species."#,
        ];

        match QaModel::open("../data/qa_model") {
            Ok(model) => {
                let answer = model.run(question, &contexts);

                assert!(answer.is_none());
            }
            Err(err) => {
                panic!("{err:?}");
            }
        }
    }

    #[test]
    fn empty() {
        match QaModel::open("../data/qa_model") {
            Ok(model) => {
                let question = "";
                let contexts = [
                    r#"The Amazon rainforest (Portuguese: Floresta Amazônica or Amazônia; Spanish: Selva Amazónica, Amazonía or usually Amazonia; French: Forêt amazonienne; Dutch: Amazoneregenwoud), also known in English as Amazonia or the Amazon Jungle, is a moist broadleaf forest that covers most of the Amazon basin of South America. This basin encompasses 7,000,000 square kilometres (2,700,000 sq mi), of 
        which 5,500,000 square kilometres (2,100,000 sq mi) are covered by the rainforest. This region includes territory belonging to nine nations. 
        The majority of the forest is contained within Brazil, with 60% of the rainforest, followed by Peru with 13%, Colombia with 10%, and with 
        minor amounts in Venezuela, Ecuador, Bolivia, Guyana, Suriname and French Guiana. States or departments in four nations contain 
        "Amazonas" in their names. The Amazon represents over half of the planet's remaining rainforests, and comprises the largest and most biodiverse tract 
        of tropical rainforest in the world, with an estimated 390 billion individual trees divided into 16,000 species.
        The Amazon rainforest (Portuguese: Floresta Amazônica or Amazônia; Spanish: Selva Amazónica, Amazonía or usually Amazonia; French: Forêt amazonienne; Dutch: Amazoneregenwoud), also known in English as Amazonia or the Amazon Jungle, is a moist broadleaf forest that covers most of the Amazon basin of South America. This basin encompasses 7,000,000 square kilometres (2,700,000 sq mi), of 
        which 5,500,000 square kilometres (2,100,000 sq mi) are covered by the rainforest. This region includes territory belonging to nine nations. 
        The majority of the forest is contained within Brazil, with 60% of the rainforest, followed by Peru with 13%, Colombia with 10%, and with 
        minor amounts in Venezuela, Ecuador, Bolivia, Guyana, Suriname and French Guiana. States or departments in four nations contain 
        "Amazonas" in their names. The Amazon represents over half of the planet's remaining rainforests, and comprises the largest and most biodiverse tract 
        of tropical rainforest in the world, with an estimated 390 billion individual trees divided into 16,000 species.
        The Amazon rainforest (Portuguese: Floresta Amazônica or Amazônia; Spanish: Selva Amazónica, Amazonía or usually Amazonia; French: Forêt amazonienne; Dutch: Amazoneregenwoud), also known in English as Amazonia or the Amazon Jungle, is a moist broadleaf forest that covers most of the Amazon basin of South America. This basin encompasses 7,000,000 square kilometres (2,700,000 sq mi), of 
        which 5,500,000 square kilometres (2,100,000 sq mi) are covered by the rainforest. This region includes territory belonging to nine nations. 
        The majority of the forest is contained within Brazil, with 60% of the rainforest, followed by Peru with 13%, Colombia with 10%, and with 
        minor amounts in Venezuela, Ecuador, Bolivia, Guyana, Suriname and French Guiana. States or departments in four nations contain 
        "Amazonas" in their names. The Amazon represents over half of the planet's remaining rainforests, and comprises the largest and most biodiverse tract 
        of tropical rainforest in the world, with an estimated 390 billion individual trees divided into 16,000 species."#,
                ];
                let answer = model.run(question, &contexts);

                assert!(answer.is_none());

                let question = "test";
                let contexts = [r#""#];
                let answer = model.run(question, &contexts);
                assert!(answer.is_none());

                let question = "";
                let contexts = [r#""#];
                let answer = model.run(question, &contexts);
                assert!(answer.is_none());
            }
            Err(err) => {
                panic!("{err:?}");
            }
        }
    }
}
