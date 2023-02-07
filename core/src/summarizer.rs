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

use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, VecDeque},
    path::Path,
    sync::Mutex,
};

use itertools::{intersperse, Itertools};
use onnxruntime::{
    ndarray::{Array, ArrayBase, Axis, Dim, IxDynImpl, OwnedRepr},
    tensor::OrtOwnedTensor,
    GraphOptimizationLevel, TypedArray,
};
use tokenizers::{PaddingParams, TruncationParams};

use crate::{softmax, spell::word2vec::Word2Vec};
use crate::{Result, ONNX_ENVIRONMENT};

struct CandidatePassage<'a> {
    passage: &'a str,
    index: usize,
    score: f32,
}

impl<'a> PartialOrd for CandidatePassage<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.score.partial_cmp(&other.score)
    }
}

impl<'a> Ord for CandidatePassage<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl<'a> PartialEq for CandidatePassage<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl<'a> Eq for CandidatePassage<'a> {}

struct OverlappingSents<'a> {
    text: &'a str,
    window_size: usize,
    next_start: VecDeque<usize>,
    overlap: usize,
}

impl<'a> OverlappingSents<'a> {
    fn new(text: &'a str, window_size: usize, overlap: usize) -> Self {
        assert!(
            overlap < window_size,
            "overlap needs to be smaller than window size"
        );

        let next_start = VecDeque::with_capacity(overlap + 1);

        Self {
            text,
            window_size,
            overlap,
            next_start,
        }
    }
}

impl<'a> Iterator for OverlappingSents<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.text.is_empty() {
            return None;
        }

        let mut end = self.text.len();
        let mut missing_words = self.window_size;
        self.next_start.clear();

        for (i, (idx, _)) in self
            .text
            .char_indices()
            .filter(|(_, c)| c.is_whitespace())
            .enumerate()
        {
            if i >= self.window_size {
                break;
            }

            missing_words -= 1;

            if self.next_start.len() > self.overlap {
                self.next_start.pop_front();
            }

            self.next_start.push_back(idx);
            end = idx;
        }

        if missing_words > 0 {
            end = self.text.len();
            for _ in 0..missing_words {
                self.next_start.pop_front();
            }
        }

        let res = &self.text[..end];

        if let Some(next_start) = self.next_start.pop_front() {
            if next_start == 0 {
                self.text = "";
            } else {
                self.text = &self.text[next_start + 1..];
            }
        } else {
            self.text = "";
        }

        Some(res)
    }
}
pub struct Summarizer {
    word2vec: Word2Vec,
    top_n_passages: usize,
    abstractive_model: AbstractiveModel,
}

impl Summarizer {
    // pub fn new() -> Self {
    //     Self {}
    // }

    pub fn extractive_summary(&self, query: &str, text: &str) -> Option<String> {
        let query_vectors: Vec<_> = query
            .split_whitespace()
            .filter_map(|word| self.word2vec.get(word))
            .collect();

        if query_vectors.is_empty() {
            return None;
        }

        let mut best_passages: BinaryHeap<Reverse<CandidatePassage<'_>>> =
            BinaryHeap::with_capacity(self.top_n_passages);

        let overlap_sents = OverlappingSents::new(text, 100, 10);

        for (index, passage) in overlap_sents.enumerate() {
            let mut score = 0.0;
            let mut count = 0;

            for passage_vec in passage
                .split_whitespace()
                .filter_map(|word| self.word2vec.get(word))
            {
                score += query_vectors
                    .iter()
                    .map(|vec| vec.sim(passage_vec))
                    .sum::<f32>();

                count += 1;
            }

            score /= count as f32;

            let candidate = CandidatePassage {
                passage,
                index,
                score,
            };

            if best_passages.len() >= self.top_n_passages {
                if let Some(mut worst) = best_passages.peek_mut() {
                    if worst.0.score < candidate.score {
                        *worst = Reverse(candidate);
                    }
                }
            } else {
                best_passages.push(Reverse(candidate));
            }
        }

        if best_passages.is_empty() {
            return None;
        }

        let mut best_passages: Vec<_> = best_passages.into_iter().map(|r| r.0).collect();
        best_passages.sort_by_key(|a| a.index);

        Some(intersperse(best_passages.into_iter().map(|p| p.passage), ". ").collect())
    }

    pub fn abstractive_summary(&self, _query: &str, text: &str) -> String {
        todo!("use onnx to generate stuff")
    }

    pub fn summarize(&self, query: &str, text: &str) -> Option<String> {
        self.extractive_summary(query, text)
            .map(|summary| self.abstractive_summary(query, &summary))
    }
}

const TRUNCATE_INPUT: usize = 1024;
pub struct AbstractiveModel {
    encoder: Mutex<onnxruntime::session::Session<'static>>,
    decoder: Mutex<onnxruntime::session::Session<'static>>,
    decoder_with_past: Mutex<onnxruntime::session::Session<'static>>,
    tokenizer: tokenizers::Tokenizer,

    bos_token_id: usize,
    pad_token_id: usize,
    eos_token_id: usize,
    begin_decoder_token: usize,
}

impl AbstractiveModel {
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

        let encoder = Mutex::new(
            ONNX_ENVIRONMENT
                .new_session_builder()?
                .with_optimization_level(GraphOptimizationLevel::All)?
                .with_number_threads(1)?
                .with_model_from_file(folder.as_ref().join("encoder_model.onnx"))?,
        );

        let decoder = Mutex::new(
            ONNX_ENVIRONMENT
                .new_session_builder()?
                .with_optimization_level(GraphOptimizationLevel::All)?
                .with_number_threads(1)?
                .with_model_from_file(folder.as_ref().join("decoder_model.onnx"))?,
        );

        let decoder_with_past = Mutex::new(
            ONNX_ENVIRONMENT
                .new_session_builder()?
                .with_optimization_level(GraphOptimizationLevel::All)?
                .with_number_threads(1)?
                .with_model_from_file(folder.as_ref().join("decoder_with_past_model.onnx"))?,
        );

        Ok(Self {
            tokenizer,
            encoder,
            decoder,
            decoder_with_past,

            bos_token_id: 0,
            pad_token_id: 1,
            eos_token_id: 2,
            begin_decoder_token: 2,
        })
    }

    fn encode(
        &self,
        ids: Vec<i64>,
        attention_mask: Vec<i64>,
    ) -> Result<ArrayBase<OwnedRepr<f32>, Dim<IxDynImpl>>> {
        let mut encoder = self
            .encoder
            .lock()
            .expect("Failed to get lock. Maybe another thread crashed?");

        let num_tokens = ids.len();

        let mut encoder_output: Vec<OrtOwnedTensor<f32, _>> = encoder.run(vec![
            TypedArray::I64(
                ArrayBase::from_vec(ids)
                    .into_shape((1, num_tokens))
                    .unwrap(),
            ),
            TypedArray::I64(
                ArrayBase::from_vec(attention_mask)
                    .into_shape((1, num_tokens))
                    .unwrap(),
            ),
        ])?;

        let res = encoder_output.pop().unwrap().clone().into_owned();

        Ok(res)
    }

    pub fn summarize(&self, text: &str, config: GenerationConfig) -> Result<String> {
        let encoded_text = self.tokenizer.encode(text, true)?;

        let mut ids = encoded_text
            .get_ids()
            .iter()
            .map(|i| *i as i64)
            .take(TRUNCATE_INPUT)
            .collect_vec();

        let attention_mask = encoded_text
            .get_attention_mask()
            .iter()
            .take(ids.len())
            .map(|i| *i as i64)
            .collect_vec();

        let encoder_num_tokens = ids.len();

        let mut decoder = self
            .decoder
            .lock()
            .expect("Failed to get lock. Maybe another thread crashed?");

        let mut decoder_with_past = self
            .decoder_with_past
            .lock()
            .expect("Failed to get lock. Maybe another thread crashed?");

        let arr_encoder_hidden_states = self.encode(ids.clone(), attention_mask.clone())?;

        let arr_attention_mask = ArrayBase::from_vec(attention_mask.clone())
            .into_shape((1, encoder_num_tokens))
            .unwrap()
            .into_dyn();

        let mut beams = Vec::new();

        ids = vec![self.begin_decoder_token as i64, self.bos_token_id as i64];
        let mut generated_tokens = 0;

        let mut caches = Vec::new();

        loop {
            generated_tokens += 1;

            if beams.is_empty() {
                let num_tokens = ids.len();
                let arr_ids = ArrayBase::from_vec(ids.clone())
                    .into_shape((1, num_tokens))
                    .unwrap()
                    .into_dyn();

                let decoder_input = vec![
                    TypedArray::I64(arr_attention_mask.clone()),
                    TypedArray::I64(arr_ids),
                    TypedArray::F32(arr_encoder_hidden_states.clone()),
                ];

                let decoder_output = decoder.run(decoder_input)?;
                let mut next_scores = BinaryHeap::with_capacity(config.num_beams as usize + 1);

                let all_logits = &decoder_output[0];
                let vocab_size = all_logits.shape()[2];
                let mut scores = Vec::with_capacity(vocab_size);

                for token_id in 0..vocab_size {
                    let score: f32 = all_logits[[0, num_tokens - 1, token_id]];
                    scores.push(score);
                }

                softmax(&mut scores);

                for (token_id, score) in scores.into_iter().enumerate() {
                    if token_id == self.bos_token_id {
                        continue;
                    }

                    let scored_token = Reverse(ScoredToken {
                        token: token_id as i64,
                        score: score.log2(),
                    });

                    if next_scores.len() < config.num_beams as usize {
                        next_scores.push(scored_token);
                    } else if let Some(mut worst) = next_scores.peek_mut() {
                        if worst.0.score < scored_token.0.score {
                            *worst = scored_token
                        }
                    }
                }

                let num_outputs = decoder_output.len();

                caches = decoder_output
                    .into_iter()
                    .map(|t| t.clone().into_owned())
                    .map(|mut t| {
                        let orig = t.clone();

                        for _ in 0..config.num_beams - 1 {
                            t.append(Axis(0), orig.view()).unwrap();
                        }

                        t
                    })
                    .skip(1)
                    .take(num_outputs - 2) // skip 'encoder_hidden_states' since this is not changed by the decoder model
                    .collect::<Vec<_>>();

                for scored_token in next_scores.into_iter().map(|t| t.0) {
                    let mut ids = ids.clone();
                    ids.push(scored_token.token);

                    beams.push(Beam {
                        input_ids: ids,
                        score: scored_token.score,
                    })
                }
            } else {
                let mut combined_attention_mask = Vec::new();
                let mut combined_encoder_hidden_states = Vec::with_capacity(beams.len());
                let mut combined_ids = Vec::new();

                // TODO: Only advance non-finished beams

                for beam in &beams {
                    combined_ids.push(*beam.input_ids.last().unwrap());

                    for mask in &attention_mask {
                        combined_attention_mask.push(*mask);
                    }

                    for state in &arr_encoder_hidden_states {
                        combined_encoder_hidden_states.push(*state);
                    }
                }

                let combined_ids = ArrayBase::<OwnedRepr<_>, _>::from_vec(combined_ids)
                    .into_shape((beams.len(), 1))
                    .unwrap()
                    .into_dyn();

                let mut attention_shape = arr_attention_mask.shape().to_vec();
                attention_shape[0] = beams.len();

                let combined_attention_mask =
                    ArrayBase::<OwnedRepr<_>, _>::from_vec(combined_attention_mask)
                        .into_shape(attention_shape.as_slice())
                        .unwrap();

                let mut state_shape = arr_encoder_hidden_states.shape().to_vec();
                state_shape[0] = beams.len();

                let combined_encoder_hidden_states =
                    ArrayBase::<OwnedRepr<_>, _>::from_vec(combined_encoder_hidden_states)
                        .into_shape(state_shape.as_slice())
                        .unwrap();

                let mut decoder_input = vec![
                    TypedArray::I64(combined_attention_mask),
                    TypedArray::I64(combined_ids),
                    TypedArray::F32(combined_encoder_hidden_states),
                ];

                for tensor in caches.drain(0..) {
                    decoder_input.push(TypedArray::F32(tensor));
                }

                let decoder_output: Vec<OrtOwnedTensor<f32, Dim<IxDynImpl>>> =
                    decoder_with_past.run(decoder_input)?;

                let all_logits = &decoder_output[0];
                let vocab_size = all_logits.shape()[2];

                let mut beam_tok_scores = Vec::with_capacity(beams.len());
                for beam_idx in 0..beams.len() {
                    let mut scores = Vec::with_capacity(vocab_size);

                    for token_id in 0..vocab_size {
                        scores.push(all_logits[[beam_idx, 0, token_id]]);
                    }

                    softmax(&mut scores);
                    beam_tok_scores.push(scores)
                }

                let num_outputs = decoder_output.len();

                // skip(num_outputs-2) to skip 'encoder_hidden_states' since this is not changed by the decoder model
                let output_caches: Vec<_> = decoder_output
                    .iter()
                    .skip(1)
                    .take(num_outputs - 2)
                    .collect();

                let mut new_beams = BinaryHeap::with_capacity(beams.len());

                for beam_idx in 0..beams.len() {
                    for new_token in 0..vocab_size {
                        let tok_score = beam_tok_scores[beam_idx][new_token];

                        let possible_beam = PossibleBeam {
                            beam_idx,
                            new_token: new_token as i64,
                            new_score: beams[beam_idx].score + tok_score.log2(),
                        };

                        if new_beams.len() < beams.len() {
                            new_beams.push(Reverse(possible_beam));
                        } else if let Some(mut worst) = new_beams.peek_mut() {
                            if possible_beam.new_score > worst.0.new_score {
                                *worst = Reverse(possible_beam);
                            }
                        }
                    }
                }

                let mut new_caches = Vec::with_capacity(output_caches.len());

                for c in &output_caches {
                    let shape = c.shape();
                    new_caches.push(Array::zeros((0, shape[1], shape[2], shape[3])).into_dyn());
                }

                let new_beams: Vec<_> = new_beams
                    .into_iter()
                    .map(|r| r.0)
                    .map(|possible_beam| {
                        let input_ids = if beams[possible_beam.beam_idx]
                            .is_finished(self.eos_token_id as i64)
                        {
                            beams[possible_beam.beam_idx].input_ids.clone()
                        } else {
                            let mut ids = beams[possible_beam.beam_idx].input_ids.clone();
                            ids.push(possible_beam.new_token);
                            ids
                        };

                        for (new_cache, old_cache) in
                            new_caches.iter_mut().zip_eq(output_caches.iter())
                        {
                            let shape = old_cache.shape();

                            new_cache
                                .append(
                                    Axis(0),
                                    old_cache
                                        .index_axis(Axis(0), possible_beam.beam_idx)
                                        .into_shape((1, shape[1], shape[2], shape[3]))
                                        .unwrap()
                                        .into_dyn(),
                                )
                                .unwrap();
                        }
                        Beam {
                            input_ids,
                            score: possible_beam.new_score,
                        }
                    })
                    .collect();

                beams = new_beams;
                caches = new_caches;

                if beams
                    .iter()
                    .all(|beam| beam.is_finished(self.eos_token_id as i64))
                {
                    break;
                }
            }

            if let Some(min_tokens) = config.force_min_tokens {
                if generated_tokens < min_tokens {
                    continue;
                }
            }

            if let Some(early_stopping) = &config.early_stopping {
                match early_stopping {
                    EarlyStopping::MaxTokens { max_new_tokens } => {
                        if generated_tokens >= *max_new_tokens {
                            break;
                        }
                    }
                }
            }
        }

        let best_beam = beams
            .into_iter()
            .max_by(|a, b| a.score.total_cmp(&b.score))
            .unwrap();

        let res: Vec<_> = best_beam.input_ids.into_iter().map(|i| i as u32).collect();
        Ok(self.tokenizer.decode(res, true).unwrap())
    }
}

struct ScoredToken {
    token: i64,
    score: f32,
}

impl PartialOrd for ScoredToken {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.score.partial_cmp(&other.score)
    }
}

impl Ord for ScoredToken {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl PartialEq for ScoredToken {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl Eq for ScoredToken {}

struct Beam {
    input_ids: Vec<i64>,
    score: f32,
}

impl Beam {
    fn is_finished(&self, eos_tok: i64) -> bool {
        self.input_ids
            .last()
            .map(|tok| *tok == eos_tok)
            .unwrap_or(false)
    }
}

struct PossibleBeam {
    beam_idx: usize,
    new_token: i64,
    new_score: f32,
}

impl PartialOrd for PossibleBeam {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.new_score.partial_cmp(&other.new_score)
    }
}

impl Ord for PossibleBeam {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl PartialEq for PossibleBeam {
    fn eq(&self, other: &Self) -> bool {
        self.new_score == other.new_score
    }
}

impl Eq for PossibleBeam {}

pub struct GenerationConfig {
    pub early_stopping: Option<EarlyStopping>,
    pub force_min_tokens: Option<u32>,
    pub num_beams: u32,
    pub length_penalty: f32,
}

impl Default for GenerationConfig {
    fn default() -> Self {
        Self {
            early_stopping: Some(EarlyStopping::MaxTokens {
                max_new_tokens: 256,
            }),
            force_min_tokens: None,
            num_beams: 5,
            length_penalty: 1.5,
        }
    }
}

pub enum EarlyStopping {
    MaxTokens { max_new_tokens: u32 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlapping_sentences() {
        let mut it = OverlappingSents::new("this is a test sentence", 3, 1);

        assert_eq!(it.next(), Some("this is a"));
        assert_eq!(it.next(), Some("a test sentence"));
        assert_eq!(it.next(), Some("sentence"));
        assert_eq!(it.next(), None);

        let mut it = OverlappingSents::new("this is a test sentence", 3, 0);

        assert_eq!(it.next(), Some("this is a"));
        assert_eq!(it.next(), Some("test sentence"));
        assert_eq!(it.next(), None);

        let mut it = OverlappingSents::new("this is a test sentence", 3, 2);

        assert_eq!(it.next(), Some("this is a"));
        assert_eq!(it.next(), Some("is a test"));
        assert_eq!(it.next(), Some("a test sentence"));
        assert_eq!(it.next(), Some("sentence"));
        assert_eq!(it.next(), None);

        let mut it = OverlappingSents::new("this", 3, 1);

        assert_eq!(it.next(), Some("this"));
        assert_eq!(it.next(), None);

        let mut it = OverlappingSents::new("this ", 3, 0);

        assert_eq!(it.next(), Some("this ")); // this is not really great, but close enough. At least no panic
        assert_eq!(it.next(), None);
    }

    #[test]
    fn abstractive_summary() {
        let model = AbstractiveModel::open("../data/abstractive_summary")
            .expect("abstractive summary model not found");

        let text = r#"Aristotle (/ˈærɪstɒtəl/;[1] Greek: Ἀριστοτέλης Aristotélēs, pronounced [aristotélɛːs]; 384–322 BC) was an Ancient Greek philosopher and polymath. His writings cover a broad range of subjects including physics, biology, zoology, metaphysics, logic, ethics, aesthetics, poetry, drama, music, rhetoric, psychology, linguistics, economics, politics, meteorology, geology, and government. As the founder of the Peripatetic school of philosophy in the Lyceum in Athens, he began the wider Aristotelian tradition that followed, which set the groundwork for the development of modern science.
        Little is known about Aristotle's life. He was born in the city of Stagira in Northern Greece during the Classical period. His father, Nicomachus, died when Aristotle was a child, and he was brought up by a guardian. At seventeen or eighteen years of age he joined Plato's Academy in Athens and remained there until the age of thirty-seven (c. 347 BC). Shortly after Plato died, Aristotle left Athens and, at the request of Philip II of Macedon, tutored his son Alexander the Great beginning in 343 BC. He established a library in the Lyceum which helped him to produce many of his hundreds of books on papyrus scrolls.
        Though Aristotle wrote many elegant treatises and dialogues for publication, only around a third of his original output has survived, none of it intended for publication. Aristotle provided a complex synthesis of the various philosophies existing prior to him. It was above all from his teachings that the West inherited its intellectual lexicon, as well as problems and methods of inquiry. As a result, his philosophy has exerted a unique influence on almost every form of knowledge in the West and it continues to be a subject of contemporary philosophical discussion.
        Aristotle's views profoundly shaped medieval scholarship. The influence of physical science extended from Late Antiquity and the Early Middle Ages into the Renaissance, and were not replaced systematically until the Enlightenment and theories such as classical mechanics were developed. Some of Aristotle's zoological observations found in his biology, such as on the hectocotyl (reproductive) arm of the octopus, were disbelieved until the 19th century. He also influenced Judeo-Islamic philosophies during the Middle Ages, as well as Christian theology, especially the Neoplatonism of the Early Church and the scholastic tradition of the Catholic Church. Aristotle was revered among medieval Muslim scholars as "The First Teacher", and among medieval Christians like Thomas Aquinas as simply "The Philosopher", while the poet Dante called him "the master of those who know". His works contain the earliest known formal study of logic, and were studied by medieval scholars such as Peter Abelard and John Buridan. Aristotle's influence on logic continued well into the 19th century. In addition, his ethics, though always influential, gained renewed interest with the modern advent of virtue ethics."#;

        let start = std::time::Instant::now();
        let config = GenerationConfig {
            num_beams: 1,
            ..Default::default()
        };

        let summary = model.summarize(text, config).unwrap();
        dbg!(start.elapsed());

        assert!(summary.len() > 50);
        assert!(summary.contains("Aristotle"));
        dbg!(&summary);

        // todo!()
    }
}
