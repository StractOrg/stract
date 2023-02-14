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
    ops::Range,
    path::Path,
    sync::{Arc, Mutex},
};

use itertools::Itertools;
use onnxruntime::{
    ndarray::{ArrayBase, Axis, Dim, IxDynImpl, OwnedRepr},
    tensor::OrtOwnedTensor,
    GraphOptimizationLevel, TypedArray,
};
use tokenizers::{PaddingParams, TruncationParams};

use crate::{ceil_char_boundary, softmax, spell::word2vec::Word2Vec};
use crate::{Result, ONNX_ENVIRONMENT};

#[derive(Clone)]
struct CandidatePassage<'a> {
    passage: &'a str,
    range: Range<usize>,
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
    prev_end: usize,
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
            prev_end: 0,
        }
    }
}

impl<'a> Iterator for OverlappingSents<'a> {
    type Item = (&'a str, Range<usize>);

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
        let range = self.prev_end..self.prev_end + end;

        if let Some(next_start) = self.next_start.pop_front() {
            if next_start == 0 {
                self.text = "";
                self.prev_end += end;
            } else {
                let next_start = ceil_char_boundary(self.text, next_start + 1);

                self.text = &self.text[next_start..];
                self.prev_end += next_start;
            }
        } else {
            self.text = "";
            self.prev_end += end;
        }

        Some((res, range))
    }
}
pub struct Summarizer {
    word2vec: Word2Vec,
    top_n_extractive_passages: usize,
    abstractive_summarizer: AbstractiveSummarizer,
}

impl Summarizer {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Self {
            word2vec: Word2Vec::open(path.as_ref().join("word2vec.bin.gz").as_path())?,
            top_n_extractive_passages: 10,
            abstractive_summarizer: AbstractiveSummarizer {
                model: Arc::new(AbstractiveModel::open(
                    path.as_ref().join("abstractive").as_path(),
                )?),
            },
        })
    }

    pub fn extractive_summary(&self, query: &str, text: &str) -> Option<String> {
        let query_vectors: Vec<_> = query
            .split_whitespace()
            .filter_map(|word| self.word2vec.get(word))
            .collect();

        if query_vectors.is_empty() {
            return None;
        }

        let mut best_passages: BinaryHeap<Reverse<CandidatePassage<'_>>> =
            BinaryHeap::with_capacity(self.top_n_extractive_passages);

        let overlap_sents = OverlappingSents::new(text, 100, 10);

        for (index, (passage, range)) in overlap_sents.enumerate() {
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
                range,
            };

            if best_passages.len() >= self.top_n_extractive_passages {
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

        let mut new_best_passages = Vec::with_capacity(best_passages.len());

        new_best_passages.push(best_passages[0].clone());

        for (a, mut b) in best_passages.into_iter().tuple_windows() {
            if a.range.end > b.range.start {
                b.range.start = ceil_char_boundary(text, a.range.end);
                b.passage = &text[b.range.clone()];
            }

            new_best_passages.push(b);
        }

        let mut res = String::new();

        res.push_str(new_best_passages[0].passage);

        for (a, b) in new_best_passages.into_iter().tuple_windows() {
            if b.index == a.index + 1 {
                res.push_str(b.passage);
            } else {
                res.push_str(". \n");
                res.push_str(b.passage);
            }
        }

        Some(res)
    }

    pub fn abstractive_summary(&self, _query: &str, text: &str) -> String {
        self.abstractive_summarizer
            .summarize(text, GenerationConfig::default())
    }

    pub fn summarize(&self, query: &str, text: &str) -> Option<String> {
        self.extractive_summary(query, text)
            .map(|summary| self.abstractive_summary(query, &summary))
    }

    pub fn summarize_iter(&self, query: &str, text: &str) -> Option<impl Iterator<Item = String>> {
        dbg!(self.extractive_summary(query, text)).map(|summary| {
            self.abstractive_summarizer
                .summarize_iter(&summary, GenerationConfig::default())
        })
    }
}

const TRUNCATE_INPUT: usize = 1024;
pub struct AbstractiveModel {
    encoder: Mutex<onnxruntime::session::Session<'static>>,
    decoder: Mutex<onnxruntime::session::Session<'static>>,
    decoder_with_past: Mutex<onnxruntime::session::Session<'static>>,
    tokenizer: tokenizers::Tokenizer,

    bos_token_id: usize,
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
                .with_number_threads(5)?
                .with_model_from_file(folder.as_ref().join("encoder_model.onnx"))?,
        );

        let decoder = Mutex::new(
            ONNX_ENVIRONMENT
                .new_session_builder()?
                .with_optimization_level(GraphOptimizationLevel::All)?
                .with_number_threads(5)?
                .with_model_from_file(folder.as_ref().join("decoder_model.onnx"))?,
        );

        let decoder_with_past = Mutex::new(
            ONNX_ENVIRONMENT
                .new_session_builder()?
                .with_optimization_level(GraphOptimizationLevel::All)?
                .with_number_threads(5)?
                .with_model_from_file(folder.as_ref().join("decoder_with_past_model.onnx"))?,
        );

        Ok(Self {
            tokenizer,
            encoder,
            decoder,
            decoder_with_past,

            bos_token_id: 0,
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
}

pub struct AbstractiveSummarizer {
    model: Arc<AbstractiveModel>,
}

impl AbstractiveSummarizer {
    pub fn new(model: AbstractiveModel) -> Self {
        Self {
            model: Arc::new(model),
        }
    }
    pub fn summarize(&self, text: &str, config: GenerationConfig) -> String {
        BeamGenerator {
            model: Arc::clone(&self.model),
            config,
            state: Some(BeamState::Encoding {
                text: text.to_string(),
            }),
        }
        .skip(2) // the first 2 tokens are BeginDecoder and BOS
        .collect()
    }

    pub fn summarize_iter(
        &self,
        text: &str,
        config: GenerationConfig,
    ) -> impl Iterator<Item = String> {
        BeamGenerator {
            model: Arc::clone(&self.model),
            config,
            state: Some(BeamState::Encoding {
                text: text.to_string(),
            }),
        }
        .skip(2) // the first 2 tokens are BeginDecoder and BOS
    }
}

struct BeamGenerator {
    model: Arc<AbstractiveModel>,
    config: GenerationConfig,
    state: Option<BeamState>,
}

impl Iterator for BeamGenerator {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        self.state.take().and_then(|state| {
            match state.step(Arc::clone(&self.model), &self.config) {
                Ok((state, res)) => {
                    self.state = Some(state);
                    res
                }
                Err(err) => {
                    tracing::error!("Encountered an error while generating summary: {err}");
                    None
                }
            }
        })
    }
}

enum BeamState {
    Encoding {
        text: String,
    },
    Decoding {
        encoder_hidden_states: ArrayBase<OwnedRepr<f32>, Dim<IxDynImpl>>,
        attention_mask: ArrayBase<OwnedRepr<i64>, Dim<IxDynImpl>>,
    },
    DecodingWithPast {
        encoder_hidden_states: ArrayBase<OwnedRepr<f32>, Dim<IxDynImpl>>,
        attention_mask: ArrayBase<OwnedRepr<i64>, Dim<IxDynImpl>>,
        caches: Vec<ArrayBase<OwnedRepr<f32>, Dim<IxDynImpl>>>,
        beams: Vec<Beam>,
        generated_tokens: u32,
        /// all beams agree on the tokens up to this index
        beam_token_agreement_idx: usize,
    },
    Remaining {
        terms: VecDeque<String>,
    },
}

impl BeamState {
    fn encode_step(model: &AbstractiveModel, text: String) -> Result<Self> {
        let encoded_text = model.tokenizer.encode(text.as_str(), true)?;

        let ids = encoded_text
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

        let encoder_hidden_states = model.encode(ids, attention_mask.clone())?;

        let attention_mask: ArrayBase<OwnedRepr<i64>, Dim<IxDynImpl>> =
            ArrayBase::from_vec(attention_mask)
                .into_shape((1, encoder_num_tokens))
                .unwrap()
                .into_dyn();

        Ok(BeamState::Decoding {
            encoder_hidden_states,
            attention_mask,
        })
    }

    fn run_decoder_model(
        model: &AbstractiveModel,
        ids: &[i64],
        encoder_hidden_states: ArrayBase<OwnedRepr<f32>, Dim<IxDynImpl>>,
        attention_mask: ArrayBase<OwnedRepr<i64>, Dim<IxDynImpl>>,
    ) -> Result<Vec<ArrayBase<OwnedRepr<f32>, Dim<IxDynImpl>>>> {
        let mut decoder = model
            .decoder
            .lock()
            .expect("Failed to get lock. Maybe another thread crashed?");

        let num_tokens = ids.len();
        let arr_ids = ArrayBase::from_vec(ids.to_vec())
            .into_shape((1, num_tokens))
            .unwrap()
            .into_dyn();

        let decoder_input = vec![
            TypedArray::I64(attention_mask),
            TypedArray::I64(arr_ids),
            TypedArray::F32(encoder_hidden_states),
        ];

        let output = decoder.run(decoder_input)?;

        Ok(output.into_iter().map(|t| t.clone().into_owned()).collect())
    }

    fn create_beams(
        decoder_output: &[ArrayBase<OwnedRepr<f32>, Dim<IxDynImpl>>],
        ids: Vec<i64>,
        num_beams: u32,
        bos_token: usize,
    ) -> Vec<Beam> {
        let mut next_scores = BinaryHeap::with_capacity(num_beams as usize + 1);

        let all_logits = &decoder_output[0];
        let vocab_size = all_logits.shape()[2];
        let mut scores = Vec::with_capacity(vocab_size);

        for token_id in 0..vocab_size {
            let score: f32 = all_logits[[
                all_logits.shape()[0] - 1,
                all_logits.shape()[1] - 1,
                token_id,
            ]];
            scores.push(score);
        }

        softmax(&mut scores);

        for (token_id, score) in scores.into_iter().enumerate() {
            if token_id == bos_token {
                continue;
            }

            let scored_token = Reverse(ScoredToken {
                token: token_id as i64,
                score: score.log2(),
            });

            if next_scores.len() < num_beams as usize {
                next_scores.push(scored_token);
            } else if let Some(mut worst) = next_scores.peek_mut() {
                if worst.0.score < scored_token.0.score {
                    *worst = scored_token
                }
            }
        }

        let mut beams = Vec::new();
        for scored_token in next_scores.into_iter().map(|t| t.0) {
            let mut ids = ids.clone();
            ids.push(scored_token.token);

            beams.push(Beam {
                input_ids: ids,
                score: scored_token.score,
            })
        }

        beams
    }

    fn create_caches(
        decoder_output: Vec<ArrayBase<OwnedRepr<f32>, Dim<IxDynImpl>>>,
        num_beams: u32,
    ) -> Vec<ArrayBase<OwnedRepr<f32>, Dim<IxDynImpl>>> {
        let num_outputs = decoder_output.len();

        decoder_output
            .into_iter()
            .map(|t| t.into_owned())
            .map(|mut t| {
                let orig = t.clone();

                for _ in 0..num_beams - 1 {
                    t.append(Axis(0), orig.view()).unwrap();
                }

                t
            })
            .skip(1)
            .take(num_outputs - 2) // skip 'encoder_hidden_states' since this is not changed by the decoder model
            .collect::<Vec<_>>()
    }

    fn run_decoder_model_with_past(
        model: &AbstractiveModel,
        beams: &[Beam],
        encoder_hidden_states: &ArrayBase<OwnedRepr<f32>, Dim<IxDynImpl>>,
        attention_mask: &ArrayBase<OwnedRepr<i64>, Dim<IxDynImpl>>,
        mut caches: Vec<ArrayBase<OwnedRepr<f32>, Dim<IxDynImpl>>>,
    ) -> Result<Vec<ArrayBase<OwnedRepr<f32>, Dim<IxDynImpl>>>> {
        let mut decoder_with_past = model
            .decoder_with_past
            .lock()
            .expect("Failed to get lock. Maybe another thread crashed?");

        let mut combined_attention_mask = Vec::new();
        let mut combined_encoder_hidden_states = Vec::with_capacity(beams.len());
        let mut combined_ids = Vec::new();

        // TODO: Only advance non-finished beams

        for beam in beams {
            combined_ids.push(*beam.input_ids.last().unwrap());

            for mask in attention_mask {
                combined_attention_mask.push(*mask);
            }

            for state in encoder_hidden_states {
                combined_encoder_hidden_states.push(*state);
            }
        }

        let combined_ids = ArrayBase::<OwnedRepr<_>, _>::from_vec(combined_ids)
            .into_shape((beams.len(), 1))
            .unwrap()
            .into_dyn();

        let mut attention_shape = attention_mask.shape().to_vec();
        attention_shape[0] = beams.len();

        let combined_attention_mask =
            ArrayBase::<OwnedRepr<_>, _>::from_vec(combined_attention_mask)
                .into_shape(attention_shape.as_slice())
                .unwrap();

        let mut state_shape = encoder_hidden_states.shape().to_vec();
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

        let res = decoder_with_past.run(decoder_input)?;

        Ok(res.into_iter().map(|t| t.clone().into_owned()).collect())
    }

    fn new_possible_beams(
        decoder_output: &[ArrayBase<OwnedRepr<f32>, Dim<IxDynImpl>>],
        beams: &[Beam],
        force_min_tokens: Option<u32>,
        generated_tokens: u32,
        eos_token_id: usize,
        length_penalty: f32,
    ) -> Vec<PossibleBeam> {
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

        let mut new_beams = BinaryHeap::with_capacity(beams.len());

        let mut skip_eos = false;

        if let Some(min_tokens) = force_min_tokens {
            if generated_tokens < min_tokens {
                skip_eos = true;
            }
        }

        for beam_idx in 0..beams.len() {
            for new_token in 0..vocab_size {
                if skip_eos && new_token == eos_token_id {
                    continue;
                }

                let tok_score = beam_tok_scores[beam_idx][new_token];

                let tok_score = tok_score.log2();

                let new_score = beams[beam_idx].score + tok_score;

                let possible_beam = PossibleBeam {
                    beam_idx,
                    beam_length: beams[beam_idx].len(),
                    new_token: new_token as i64,
                    length_penalty,
                    eos_token: eos_token_id as i64,
                    new_score,
                };

                if new_beams.len() < beams.len() {
                    new_beams.push(Reverse(possible_beam));
                } else if let Some(mut worst) = new_beams.peek_mut() {
                    if possible_beam > worst.0 {
                        *worst = Reverse(possible_beam);
                    }
                }
            }
        }

        new_beams.into_iter().map(|r| r.0).collect()
    }

    fn new_caches(
        decoder_output: &[ArrayBase<OwnedRepr<f32>, Dim<IxDynImpl>>],
        new_beams: &[PossibleBeam],
    ) -> Vec<ArrayBase<OwnedRepr<f32>, Dim<IxDynImpl>>> {
        let num_outputs = decoder_output.len();
        // skip(num_outputs-2) to skip 'encoder_hidden_states' since this is not changed by the decoder model
        let output_caches: Vec<_> = decoder_output
            .iter()
            .skip(1)
            .take(num_outputs - 2)
            .collect();

        let mut new_caches = Vec::with_capacity(output_caches.len());

        for _ in &output_caches {
            new_caches.push(Vec::with_capacity(100));
        }

        for possible_beam in new_beams {
            for (new_cache, old_cache) in new_caches.iter_mut().zip_eq(output_caches.iter()) {
                let shape = old_cache.shape();

                new_cache.push(
                    old_cache
                        .index_axis(Axis(0), possible_beam.beam_idx)
                        .into_shape((1, shape[1], shape[2], shape[3]))
                        .unwrap()
                        .into_dyn(),
                );
            }
        }

        new_caches
            .into_iter()
            .map(|cache| onnxruntime::ndarray::concatenate(Axis(0), cache.as_slice()).unwrap())
            .collect()
    }

    fn cement_beams(
        beams: Vec<Beam>,
        possible_beams: Vec<PossibleBeam>,
        eos_token_id: i64,
    ) -> Vec<Beam> {
        possible_beams
            .into_iter()
            .map(|possible_beam| {
                if beams[possible_beam.beam_idx].is_finished(eos_token_id) {
                    beams[possible_beam.beam_idx].clone()
                } else {
                    let mut input_ids = beams[possible_beam.beam_idx].input_ids.clone();
                    input_ids.push(possible_beam.new_token);
                    Beam {
                        input_ids,
                        score: possible_beam.new_score,
                    }
                }
            })
            .collect()
    }

    fn output_from_best(
        model: &AbstractiveModel,
        new_beams: Vec<Beam>,
        beam_agreement_index: usize,
    ) -> BeamState {
        let best_beam = new_beams
            .into_iter()
            .max_by(|a, b| a.score.total_cmp(&b.score))
            .unwrap();

        let terms: VecDeque<_> = best_beam
            .input_ids
            .into_iter()
            .skip(beam_agreement_index)
            .map(|i| i as u32)
            .map(|tok| model.tokenizer.decode(vec![tok], true).unwrap())
            .collect();

        BeamState::Remaining { terms }
    }

    fn check_early_stopping(early_stopping: Option<&EarlyStopping>, generated_tokens: u32) -> bool {
        early_stopping
            .map(|early_stopping| match early_stopping {
                EarlyStopping::MaxTokens { max_new_tokens } => generated_tokens >= *max_new_tokens,
            })
            .unwrap_or(false)
    }

    fn all_beams_agree(beams: &[Beam], beam_token_agreement_idx: usize) -> bool {
        if beams
            .iter()
            .any(|beam| beam_token_agreement_idx >= beam.len())
        {
            return false;
        }

        beams
            .iter()
            .map(|beam| beam.input_ids[beam_token_agreement_idx])
            .all_equal()
    }

    fn step(
        mut self,
        model: Arc<AbstractiveModel>,
        config: &GenerationConfig,
    ) -> Result<(Self, Option<<BeamGenerator as Iterator>::Item>)> {
        loop {
            match self {
                BeamState::Encoding { text } => {
                    self = Self::encode_step(model.as_ref(), text)?;
                }
                BeamState::Decoding {
                    encoder_hidden_states,
                    attention_mask,
                } => {
                    let ids = vec![model.begin_decoder_token as i64, model.bos_token_id as i64];
                    let decoder_output = Self::run_decoder_model(
                        model.as_ref(),
                        &ids,
                        encoder_hidden_states.clone(),
                        attention_mask.clone(),
                    )?;

                    let beams = Self::create_beams(
                        &decoder_output,
                        ids,
                        config.num_beams,
                        model.bos_token_id,
                    );

                    let caches = Self::create_caches(decoder_output, config.num_beams);

                    self = BeamState::DecodingWithPast {
                        encoder_hidden_states,
                        attention_mask,
                        caches,
                        beams,
                        generated_tokens: 1,
                        beam_token_agreement_idx: 0,
                    };
                }

                BeamState::DecodingWithPast {
                    encoder_hidden_states,
                    attention_mask,
                    caches,
                    mut beams,
                    generated_tokens,
                    beam_token_agreement_idx,
                } => {
                    let shortest_beam = beams
                        .iter()
                        .min_by(|a, b| a.input_ids.len().cmp(&b.input_ids.len()))
                        .unwrap();

                    if (shortest_beam.input_ids.len() - beam_token_agreement_idx) as u32
                        > config.max_beam_divergence_tokens
                    {
                        let best_beam = beams
                            .iter()
                            .max_by(|a, b| a.score.partial_cmp(&b.score).unwrap_or(Ordering::Equal))
                            .unwrap()
                            .clone();

                        let num_beams = beams.len();
                        beams = vec![best_beam; num_beams];
                    }

                    if Self::all_beams_agree(&beams, beam_token_agreement_idx) {
                        let tok = beams[0].input_ids[beam_token_agreement_idx];
                        let decoded = model.tokenizer.decode(vec![tok as u32], true)?;

                        self = BeamState::DecodingWithPast {
                            encoder_hidden_states,
                            attention_mask,
                            caches,
                            beams,
                            generated_tokens,
                            beam_token_agreement_idx: beam_token_agreement_idx + 1,
                        };

                        return Ok((self, Some(decoded)));
                    }

                    let decoder_output = Self::run_decoder_model_with_past(
                        model.as_ref(),
                        &beams,
                        &encoder_hidden_states,
                        &attention_mask,
                        caches,
                    )?;

                    let possible_beams = Self::new_possible_beams(
                        &decoder_output,
                        &beams,
                        config.force_min_tokens,
                        generated_tokens,
                        model.eos_token_id,
                        config.length_penalty,
                    );

                    let new_caches = Self::new_caches(&decoder_output, &possible_beams);

                    let new_beams =
                        Self::cement_beams(beams, possible_beams, model.eos_token_id as i64);

                    if Self::check_early_stopping(config.early_stopping.as_ref(), generated_tokens)
                        || new_beams
                            .iter()
                            .all(|beam| beam.is_finished(model.eos_token_id as i64))
                    {
                        self = Self::output_from_best(
                            model.as_ref(),
                            new_beams,
                            beam_token_agreement_idx,
                        );
                    } else {
                        self = BeamState::DecodingWithPast {
                            encoder_hidden_states,
                            attention_mask,
                            caches: new_caches,
                            beams: new_beams,
                            generated_tokens: generated_tokens + 1,
                            beam_token_agreement_idx,
                        };
                    }
                }
                BeamState::Remaining { mut terms } => {
                    let term = terms.pop_front();

                    return Ok((BeamState::Remaining { terms }, term));
                }
            }
        }
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

#[derive(Clone, Debug)]
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

    fn len(&self) -> usize {
        self.input_ids.len()
    }
}

#[derive(Debug)]
struct PossibleBeam {
    beam_idx: usize,
    beam_length: usize,
    new_token: i64,
    length_penalty: f32,
    new_score: f32,

    eos_token: i64,
}

impl PossibleBeam {
    fn normalized_score(&self) -> f32 {
        if self.length_penalty == 1.0 {
            self.new_score
        } else {
            let length = if self.new_token == self.eos_token {
                self.beam_length as f32
            } else {
                (self.beam_length + 1) as f32
            };

            self.new_score / length.powf(self.length_penalty)
        }
    }
}

impl PartialOrd for PossibleBeam {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.normalized_score()
            .partial_cmp(&other.normalized_score())
    }
}

impl Ord for PossibleBeam {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl PartialEq for PossibleBeam {
    fn eq(&self, other: &Self) -> bool {
        self.normalized_score() == other.normalized_score()
    }
}

impl Eq for PossibleBeam {}

pub struct GenerationConfig {
    pub early_stopping: Option<EarlyStopping>,
    pub force_min_tokens: Option<u32>,
    /// Exponential penalty to the length that is used with beam-based generation.
    /// It is applied as an exponent to the sequence length, which in turn is used to divide the score of the sequence.
    /// Since the score is the log likelihood of the sequence (i.e. negative),
    /// length_penalty > 0.0 promotes longer sequences, while length_penalty < 0.0 encourages shorter sequences
    pub length_penalty: f32,
    pub num_beams: u32,
    pub max_beam_divergence_tokens: u32,
}

impl Default for GenerationConfig {
    fn default() -> Self {
        Self {
            early_stopping: Some(EarlyStopping::MaxTokens {
                max_new_tokens: 128,
            }),
            force_min_tokens: None,
            length_penalty: 1.0,
            num_beams: 10,
            max_beam_divergence_tokens: 20,
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
        let mut it = OverlappingSents::new("this is a test sentence", 3, 1).map(|(p, _)| p);

        assert_eq!(it.next(), Some("this is a"));
        assert_eq!(it.next(), Some("a test sentence"));
        assert_eq!(it.next(), Some("sentence"));
        assert_eq!(it.next(), None);

        let mut it = OverlappingSents::new("this is a test sentence", 3, 0).map(|(p, _)| p);

        assert_eq!(it.next(), Some("this is a"));
        assert_eq!(it.next(), Some("test sentence"));
        assert_eq!(it.next(), None);

        let mut it = OverlappingSents::new("this is a test sentence", 3, 2).map(|(p, _)| p);

        assert_eq!(it.next(), Some("this is a"));
        assert_eq!(it.next(), Some("is a test"));
        assert_eq!(it.next(), Some("a test sentence"));
        assert_eq!(it.next(), Some("sentence"));
        assert_eq!(it.next(), None);

        let mut it = OverlappingSents::new("this", 3, 1).map(|(p, _)| p);

        assert_eq!(it.next(), Some("this"));
        assert_eq!(it.next(), None);

        let mut it = OverlappingSents::new("this ", 3, 0).map(|(p, _)| p);

        assert_eq!(it.next(), Some("this ")); // this is not really great, but close enough. At least no panic
        assert_eq!(it.next(), None);

        let text = "this is a test sentence";
        let it = OverlappingSents::new(text, 3, 1);

        for (p, range) in it {
            dbg!(p, &range, &text[range.clone()]);
            assert_eq!(p, &text[range]);
        }
    }

    #[test]
    fn abstractive_summary() {
        let summarizer = AbstractiveSummarizer {
            model: Arc::new(
                AbstractiveModel::open("../data/summarizer/abstractive")
                    .expect("abstractive summary model not found"),
            ),
        };

        let text = r#"Aristotle (/ˈærɪstɒtəl/;[1] Greek: Ἀριστοτέλης Aristotélēs, pronounced [aristotélɛːs]; 384–322 BC) was an Ancient Greek philosopher and polymath. His writings cover a broad range of subjects including physics, biology, zoology, metaphysics, logic, ethics, aesthetics, poetry, drama, music, rhetoric, psychology, linguistics, economics, politics, meteorology, geology, and government. As the founder of the Peripatetic school of philosophy in the Lyceum in Athens, he began the wider Aristotelian tradition that followed, which set the groundwork for the development of modern science.
        Little is known about Aristotle's life. He was born in the city of Stagira in Northern Greece during the Classical period. His father, Nicomachus, died when Aristotle was a child, and he was brought up by a guardian. At seventeen or eighteen years of age he joined Plato's Academy in Athens and remained there until the age of thirty-seven (c. 347 BC). Shortly after Plato died, Aristotle left Athens and, at the request of Philip II of Macedon, tutored his son Alexander the Great beginning in 343 BC. He established a library in the Lyceum which helped him to produce many of his hundreds of books on papyrus scrolls.
        Though Aristotle wrote many elegant treatises and dialogues for publication, only around a third of his original output has survived, none of it intended for publication. Aristotle provided a complex synthesis of the various philosophies existing prior to him. It was above all from his teachings that the West inherited its intellectual lexicon, as well as problems and methods of inquiry. As a result, his philosophy has exerted a unique influence on almost every form of knowledge in the West and it continues to be a subject of contemporary philosophical discussion.
        Aristotle's views profoundly shaped medieval scholarship. The influence of physical science extended from Late Antiquity and the Early Middle Ages into the Renaissance, and were not replaced systematically until the Enlightenment and theories such as classical mechanics were developed. Some of Aristotle's zoological observations found in his biology, such as on the hectocotyl (reproductive) arm of the octopus, were disbelieved until the 19th century. He also influenced Judeo-Islamic philosophies during the Middle Ages, as well as Christian theology, especially the Neoplatonism of the Early Church and the scholastic tradition of the Catholic Church. Aristotle was revered among medieval Muslim scholars as "The First Teacher", and among medieval Christians like Thomas Aquinas as simply "The Philosopher", while the poet Dante called him "the master of those who know". His works contain the earliest known formal study of logic, and were studied by medieval scholars such as Peter Abelard and John Buridan. Aristotle's influence on logic continued well into the 19th century. In addition, his ethics, though always influential, gained renewed interest with the modern advent of virtue ethics."#;

        let config = GenerationConfig {
            num_beams: 2,
            length_penalty: 1.0,
            early_stopping: Some(EarlyStopping::MaxTokens { max_new_tokens: 16 }),
            ..Default::default()
        };

        let start = std::time::Instant::now();
        let summary = summarizer.summarize(text, config);
        println!("Elapsed: {:?}", start.elapsed());
        println!("{:?}", &summary);

        assert!(summary.len() > 50);
        assert!(summary.contains("Aristotle"));
    }
}
