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

use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, VecDeque},
    ops::Range,
    path::Path,
    sync::Arc,
};

use itertools::{intersperse, Itertools};
use tch::{IValue, Kind, Tensor};
use tokenizers::{PaddingParams, TruncationParams};

use crate::{ceil_char_boundary, spell::word2vec::Word2Vec};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Torch")]
    Tch(#[from] tch::TchError),

    #[error("Tokenizer")]
    Tokenizer(#[from] tokenizers::Error),

    #[error("IO")]
    Io(#[from] std::io::Error),

    #[error("Word2vec")]
    Word2Vec(#[from] crate::spell::word2vec::Error),

    #[error("Unexpected output type")]
    UnexpectedOutputType,
}

type Result<T> = std::result::Result<T, Error>;

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

    fn extractive_query_specific(&self, query: &str, text: &str) -> Option<String> {
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

    pub fn extractive_summary(&self, query: &str, text: &str) -> String {
        self.extractive_query_specific(query, text)
            .unwrap_or_else(|| intersperse(text.split_whitespace().take(1000), " ").collect())
    }

    pub fn abstractive_summary(&self, _query: &str, text: &str) -> String {
        self.abstractive_summarizer
            .summarize(text, GenerationConfig::default())
    }

    pub fn summarize(&self, query: &str, text: &str) -> String {
        let summary = self.extractive_summary(query, text);
        self.abstractive_summary(query, &summary)
    }
}

const TRUNCATE_INPUT: usize = 1024;
pub struct AbstractiveModel {
    encoder: tch::CModule,
    decoder: tch::CModule,
    decoder_with_past: tch::CModule,
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

        let encoder = tch::CModule::load(folder.as_ref().join("traced_encoder.pt"))?;
        let decoder = tch::CModule::load(folder.as_ref().join("traced_decoder.pt"))?;
        let decoder_with_past = tch::CModule::load(folder.as_ref().join("traced_decoder_wp.pt"))?;

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
    pub fn summarize(&self, text: &str, mut config: GenerationConfig) -> String {
        let ids = self
            .model
            .tokenizer
            .encode(text, false)
            .unwrap()
            .get_ids()
            .iter()
            .map(|id| *id as i64)
            .collect_vec();

        let ids = Tensor::of_slice(&ids).reshape(&[1, -1]);

        if !config
            .banned_tokens
            .contains(&(self.model.bos_token_id as i64))
        {
            config.banned_tokens.push(self.model.bos_token_id as i64);
        }

        if !config
            .end_tokens
            .contains(&(self.model.eos_token_id as i64))
        {
            config.end_tokens.push(self.model.eos_token_id as i64);
        }

        match self.model.beam_search(
            &ids,
            &Tensor::of_slice(&[
                self.model.begin_decoder_token as i64,
                self.model.bos_token_id as i64,
            ])
            .reshape(&[1, -1]),
            config,
        ) {
            Ok(res) => {
                let tokens = res.tokens.into_iter().map(|tok| tok as u32).collect_vec();
                self.model
                    .tokenizer
                    .decode(tokens, true)
                    .unwrap_or_default()
            }
            Err(err) => {
                tracing::error!("Error while summarizing: {:?}", err);
                String::new()
            }
        }
    }
}

struct ClonableTensor(Tensor);

impl Clone for ClonableTensor {
    fn clone(&self) -> Self {
        let out = Tensor::empty(&self.0.size(), (Kind::Float, self.0.device()));
        ClonableTensor(self.0.clone(&out))
    }
}

#[derive(Clone)]
struct BartMemory {
    encoder_hidden_states: ClonableTensor,
    past_key_values: Vec<Vec<ClonableTensor>>,
}

fn parse_decoder_output(output: IValue) -> Result<(Tensor, Vec<Vec<ClonableTensor>>)> {
    let mut output = if let IValue::Tuple(tup) = output {
        Ok(tup)
    } else {
        Err(Error::UnexpectedOutputType)
    }?;

    if output.len() != 2 {
        return Err(Error::UnexpectedOutputType);
    }

    let logits = if let IValue::Tensor(t) = output.remove(0) {
        Ok(t)
    } else {
        Err(Error::UnexpectedOutputType)
    }?;

    let caches = if let IValue::Tuple(caches) = output.remove(0) {
        Ok(caches)
    } else {
        Err(Error::UnexpectedOutputType)
    }?;

    let past_key_values = if caches.len() == 12 {
        let mut new_caches = Vec::with_capacity(caches.len());

        for cache in caches {
            if let IValue::Tuple(cache) = cache {
                let mut c = Vec::with_capacity(2);
                for cache in cache.into_iter().take(2) {
                    if let IValue::Tensor(cache) = cache {
                        c.push(ClonableTensor(cache));
                    } else {
                        return Err(Error::UnexpectedOutputType);
                    }
                }

                new_caches.push(c);
            } else {
                return Err(Error::UnexpectedOutputType);
            }
        }

        Ok(new_caches)
    } else {
        Err(Error::UnexpectedOutputType)
    }?;

    Ok((logits, past_key_values))
}

impl BeamSearch for AbstractiveModel {
    type Memory = BartMemory;

    fn initial(
        &self,
        encoder_ids: &Tensor,
        decoder_ids: &Tensor,
    ) -> Result<DecoderOutput<Self::Memory>> {
        let encoder_hidden_states = self.encoder.forward_ts(&[encoder_ids])?;

        let decoder_output = self.decoder.forward_is(&[
            IValue::Tensor(decoder_ids.shallow_clone()),
            IValue::Tensor(encoder_hidden_states.shallow_clone()),
        ])?;

        let (logits, past_key_values) = parse_decoder_output(decoder_output)?;

        // logits is [batch_size, seq_len, vocab_size]
        // get last token logits
        let next_token_logits = logits.select(1, logits.size()[1] - 1);

        Ok(DecoderOutput {
            next_token_logits,
            memory: BartMemory {
                encoder_hidden_states: ClonableTensor(encoder_hidden_states),
                past_key_values,
            },
        })
    }

    fn step(
        &self,
        prev_output: &DecoderInput<Self::Memory>,
    ) -> Result<DecoderOutput<Self::Memory>> {
        let ids = Tensor::of_slice(&[prev_output.last_token]).reshape(&[1, 1]);

        let past_key_value: IValue = IValue::Tuple(
            prev_output
                .memory
                .past_key_values
                .iter()
                .map(|c| {
                    IValue::Tuple(
                        c.iter()
                            .map(|c| IValue::Tensor(c.0.shallow_clone()))
                            .collect_vec(),
                    )
                })
                .collect_vec(),
        );

        let decoder_output = self.decoder_with_past.forward_is(&[
            IValue::Tensor(ids),
            IValue::Tensor(prev_output.memory.encoder_hidden_states.clone().0),
            past_key_value,
        ])?;

        let (logits, past_key_values) = parse_decoder_output(decoder_output)?;

        // logits is [batch_size, seq_len, vocab_size]
        // get last token logits
        let next_token_logits = logits.select(1, logits.size()[1] - 1).squeeze();

        Ok(DecoderOutput {
            next_token_logits,
            memory: BartMemory {
                encoder_hidden_states: prev_output.memory.encoder_hidden_states.clone(),
                past_key_values,
            },
        })
    }
}

struct DecoderOutput<M: Clone> {
    next_token_logits: Tensor,
    memory: M,
}

struct DecoderInput<M: Clone> {
    last_token: i64,
    memory: M,
}

#[derive(Clone)]
struct Beam<M: Clone> {
    log_score: f64,
    length_penalty: f64,
    tokens: Vec<i64>,
    end_tokens: Vec<i64>,
    memory: M,
}

impl<M: Clone> Beam<M> {
    fn is_finished(&self, config: &GenerationConfig) -> bool {
        if let Some(forced) = &config.force_min_tokens {
            if self.tokens.len() < *forced as usize {
                return false;
            }
        }

        if let Some(early_stopping) = &config.early_stopping {
            match early_stopping {
                EarlyStopping::MaxTokens { max_new_tokens } => {
                    if self.tokens.len() >= *max_new_tokens as usize {
                        return true;
                    }
                }
            }
        }

        if let Some(tok) = self.tokens.last() {
            if config.end_tokens.contains(tok) {
                return true;
            }
        }

        false
    }

    fn score(&self) -> f64 {
        let length = match self.tokens.last() {
            Some(tok) => {
                if self.end_tokens.contains(tok) {
                    self.tokens.len() - 1
                } else {
                    self.tokens.len()
                }
            }
            None => 0,
        };

        self.log_score / ((length as f64).powf(self.length_penalty))
    }
}

impl<M: Clone> PartialOrd for Beam<M> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.score().partial_cmp(&other.score())
    }
}

impl<M: Clone> PartialEq for Beam<M> {
    fn eq(&self, other: &Self) -> bool {
        self.score() == other.score()
    }
}

impl<M: Clone> Ord for Beam<M> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl<M: Clone> Eq for Beam<M> {}

trait BeamSearch {
    type Memory: Clone;

    fn initial(
        &self,
        encoder_ids: &Tensor,
        decoder_ids: &Tensor,
    ) -> Result<DecoderOutput<Self::Memory>>;

    fn step(&self, prev_output: &DecoderInput<Self::Memory>)
        -> Result<DecoderOutput<Self::Memory>>;

    fn beam_search(
        &self,
        encoder_ids: &Tensor,
        decoder_start_tokens: &Tensor,
        config: GenerationConfig,
    ) -> Result<Beam<Self::Memory>> {
        let encoded = self.initial(encoder_ids, decoder_start_tokens)?;
        let decoder_start_tokens = decoder_start_tokens.squeeze().iter::<i64>()?.collect_vec();

        let scores = encoded.next_token_logits.softmax(-1, Kind::Float).squeeze();

        let (next_scores, next_tokens) = scores.topk(config.num_beams as i64, -1, true, true);

        let mut beams = BinaryHeap::with_capacity(config.num_beams as usize);

        for (score, token) in next_scores.iter::<f64>()?.zip(next_tokens.iter::<i64>()?) {
            if config.banned_tokens.contains(&token) {
                continue;
            }

            let mut toks = decoder_start_tokens.clone();
            toks.push(token);

            let beam = Reverse(Beam {
                log_score: score.log2(),
                tokens: toks,
                memory: encoded.memory.clone(),
                end_tokens: config.end_tokens.clone(),
                length_penalty: config.length_penalty,
            });

            if beams.len() < config.num_beams as usize {
                beams.push(beam);
            } else {
                let mut worst = beams.peek_mut().unwrap();
                if beam.0 > worst.0 {
                    *worst = beam;
                }
            }
        }

        loop {
            let mut next_beams = BinaryHeap::with_capacity(config.num_beams as usize);

            for beam in beams.iter() {
                if beam.0.is_finished(&config) {
                    if next_beams.len() < config.num_beams as usize {
                        next_beams.push(beam.clone());
                    } else {
                        let mut worst = next_beams.peek_mut().unwrap();
                        if beam.0 > worst.0 {
                            *worst = beam.clone();
                        }
                    }

                    continue;
                }

                let prev_output = DecoderInput {
                    last_token: *beam.0.tokens.last().unwrap(),
                    memory: beam.0.memory.clone(),
                };

                let decoded = self.step(&prev_output)?;

                let scores = decoded.next_token_logits.softmax(-1, Kind::Float);

                let (next_scores, next_tokens) =
                    scores.topk(config.num_beams as i64, -1, true, true);

                for (score, token) in next_scores.iter::<f64>()?.zip(next_tokens.iter::<i64>()?) {
                    let mut new_beam = beam.clone().0;
                    new_beam.log_score += score.log2();
                    new_beam.tokens.push(token);
                    new_beam.memory = decoded.memory.clone();

                    if next_beams.len() < config.num_beams as usize {
                        next_beams.push(Reverse(new_beam));
                    } else {
                        let mut worst = next_beams.peek_mut().unwrap();
                        if new_beam > worst.0 {
                            *worst = Reverse(new_beam);
                        }
                    }
                }
            }

            if next_beams.iter().all(|b| b.0.is_finished(&config)) {
                break;
            }

            beams = next_beams;
        }

        let mut beams = beams
            .into_sorted_vec()
            .into_iter()
            .map(|b| b.0)
            .collect_vec();

        beams.sort();

        Ok(beams.pop().unwrap())
    }
}

pub struct GenerationConfig {
    pub early_stopping: Option<EarlyStopping>,
    pub force_min_tokens: Option<u32>,
    pub end_tokens: Vec<i64>,
    pub banned_tokens: Vec<i64>,
    /// Exponential penalty to the length that is used with beam-based generation.
    /// It is applied as an exponent to the sequence length, which in turn is used to divide the score of the sequence.
    /// Since the score is the log likelihood of the sequence (i.e. negative),
    /// length_penalty > 0.0 promotes longer sequences, while length_penalty < 0.0 encourages shorter sequences
    pub length_penalty: f64,
    pub num_beams: u32,
}

impl Default for GenerationConfig {
    fn default() -> Self {
        Self {
            early_stopping: Some(EarlyStopping::MaxTokens {
                max_new_tokens: 128,
            }),
            end_tokens: vec![],
            banned_tokens: vec![],
            force_min_tokens: None,
            length_penalty: 1.0,
            num_beams: 10,
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
            early_stopping: Some(EarlyStopping::MaxTokens { max_new_tokens: 16 }),
            length_penalty: 1.0,
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
