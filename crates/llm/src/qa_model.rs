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

use std::{cmp::Ordering, ops::Range, path::Path};

use itertools::Itertools;
use tch::{IValue, Tensor};
use tokenizers::{PaddingParams, TruncationParams};

const TRUNCATE_INPUT: usize = 128;
const DEFAULT_SCORE_THRESHOLD: f64 = 0.7;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Tokenizer error")]
    Tokenizer(#[from] tokenizers::Error),
    #[error("Torch error")]
    Torch(#[from] tch::TchError),
}

type Result<T> = std::result::Result<T, Error>;

pub struct QaModel {
    tokenizer: tokenizers::Tokenizer,
    model: tch::CModule,
    score_threshold: f64,
}

#[derive(Debug)]
pub struct Answer {
    pub offset: Range<usize>,
    pub context_idx: usize,
    pub score: f64,
}

impl QaModel {
    pub fn open(folder: &Path) -> Result<Self> {
        let truncation = TruncationParams {
            max_length: TRUNCATE_INPUT,
            ..Default::default()
        };
        let padding = PaddingParams {
            ..Default::default()
        };

        let mut tokenizer = tokenizers::Tokenizer::from_file(folder.join("tokenizer.json"))?;
        tokenizer.with_truncation(Some(truncation))?;
        tokenizer.with_padding(Some(padding));

        let model = tch::CModule::load(folder.join("model.pt"))?;
        Ok(Self {
            tokenizer,
            model,
            score_threshold: DEFAULT_SCORE_THRESHOLD,
        })
    }

    pub fn run<S: AsRef<str>>(&self, question: &str, contexts: &[S]) -> Option<Answer> {
        if contexts.is_empty() {
            return None;
        }

        let bs = contexts.len();
        let mut batches = Vec::with_capacity(bs);

        for context in contexts {
            batches.push((question.to_string(), context.as_ref().to_string()));
        }
        let encoded = self.tokenizer.encode_batch(batches, true).unwrap();

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

        let ids = Tensor::from_slice(&ids).reshape([bs as i64, num_tokens as i64]);
        let attention_mask =
            Tensor::from_slice(&attention_mask).reshape([bs as i64, num_tokens as i64]);

        let output = self
            .model
            .forward_is(&[IValue::Tensor(ids), IValue::Tensor(attention_mask)])
            .unwrap();

        let (start_logits, end_logits) = if let IValue::Tuple(mut tup) = output {
            if tup.len() != 2 {
                return None;
            }

            let end = tup.pop().unwrap();
            let start = tup.pop().unwrap();

            if let (IValue::Tensor(start), IValue::Tensor(end)) = (start, end) {
                (start, end)
            } else {
                return None;
            }
        } else {
            return None;
        };

        debug_assert_eq!(start_logits.size(), end_logits.size());

        let mut best_answer: Option<Answer> = None;

        #[allow(clippy::needless_range_loop)]
        for idx in 0..bs {
            let start: Vec<f64> = start_logits
                .get(idx as i64)
                .iter::<f64>()
                .unwrap()
                .collect();
            // softmax(&mut start);

            let end: Vec<f64> = end_logits.get(idx as i64).iter::<f64>().unwrap().collect();
            // softmax(&mut end);

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

            let score = start[best_start] * end[best_end];

            if score < self.score_threshold {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn open_qa_model() -> QaModel {
        QaModel::open(
            &std::path::Path::new("../../data/qa_model")
                .canonicalize()
                .expect("QA model not found in data/qa_model"),
        )
        .expect("Failed to find QA model")
    }

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

        let mut model = open_qa_model();
        model.score_threshold = 0.0;

        let answer = model.run(question, &contexts).unwrap();

        assert!(answer.score > 0.6);
        assert_eq!(
            &contexts[answer.context_idx][answer.offset],
            "South America"
        );
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

        let mut model = open_qa_model();
        model.score_threshold = 0.0;

        let answer = model.run(question, &contexts).unwrap();

        assert!(answer.score > 0.6);
        assert_eq!(
            &contexts[answer.context_idx][answer.offset],
            "South America"
        );
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

        let mut model = open_qa_model();
        model.score_threshold = 0.0;

        let answer = model.run(question, &contexts).unwrap();

        assert!(answer.score > 0.6);
        assert_eq!(
            &contexts[answer.context_idx][answer.offset],
            "South America"
        );
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

        let mut model = open_qa_model();
        model.score_threshold = 0.0;

        let answer = model.run(question, &contexts);
        assert!(answer.is_none());
    }

    #[test]
    fn empty() {
        let mut model = open_qa_model();
        model.score_threshold = 0.0;

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
}
