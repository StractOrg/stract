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
    cmp::Reverse,
    collections::{BinaryHeap, VecDeque},
    ops::Range,
    path::Path,
    sync::Arc,
};

use itertools::{intersperse, Itertools};
use tch::{IValue, Kind, Tensor};
use tokenizers::{PaddingParams, TruncationParams};

use crate::{
    llm_utils::{self, ClonableTensor},
    word2vec::{Word2Vec, WordVec},
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Torch")]
    Tch(#[from] tch::TchError),

    #[error("Tokenizer")]
    Tokenizer(#[from] tokenizers::Error),

    #[error("IO")]
    Io(#[from] std::io::Error),

    #[error("Word2vec")]
    Word2Vec(#[from] crate::word2vec::Error),

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
        Some(self.cmp(other))
    }
}

impl<'a> Ord for CandidatePassage<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score.total_cmp(&other.score)
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
                let next_start = stdx::ceil_char_boundary(self.text, next_start + 1);

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

trait PassageScorer {
    type QueryEmbedding;
    type PassageEmbedding;

    fn embed_query(&self, query: &str) -> Option<Self::QueryEmbedding>;
    fn embed_passage(&self, passage: &str) -> Option<Self::PassageEmbedding>;

    fn score(&self, query: &Self::QueryEmbedding, passage: &Self::PassageEmbedding) -> f32;
}

impl PassageScorer for Word2Vec {
    type QueryEmbedding = Vec<WordVec>;

    type PassageEmbedding = Self::QueryEmbedding;

    fn embed_query(&self, query: &str) -> Option<Self::QueryEmbedding> {
        let res: Self::QueryEmbedding = query
            .split_whitespace()
            .filter_map(|word| self.get(word).cloned())
            .collect();

        if res.is_empty() {
            return None;
        }

        Some(res)
    }

    fn embed_passage(&self, passage: &str) -> Option<Self::PassageEmbedding> {
        let res: Self::PassageEmbedding = passage
            .split_whitespace()
            .filter_map(|word| self.get(word).cloned())
            .collect();

        if res.is_empty() {
            return None;
        }

        Some(res)
    }

    fn score(&self, query: &Self::QueryEmbedding, passage: &Self::PassageEmbedding) -> f32 {
        let mut score = 0.0;
        let mut count = 0;

        for passage_vec in passage {
            score += query.iter().map(|vec| vec.sim(passage_vec)).sum::<f32>();

            count += 1;
        }

        score / count as f32
    }
}

pub struct ExtractiveSummarizer {
    passage_scorer: DualEncoder,
    top_n_passages: usize,
    window_size: usize,
    overlap: usize,
}

impl ExtractiveSummarizer {
    pub fn open<P: AsRef<Path>>(path: P, top_n_passages: usize) -> Result<Self> {
        Ok(Self {
            passage_scorer: DualEncoder::open(path)?,
            top_n_passages,
            window_size: 100,
            overlap: 10,
        })
    }

    pub fn set_window_size(&mut self, window_size: usize) {
        self.window_size = window_size;
    }

    fn query_specific(&self, query: &str, text: &str) -> Option<String> {
        let query_vectors = self.passage_scorer.embed_query(query)?;

        let mut best_passages: BinaryHeap<Reverse<CandidatePassage<'_>>> =
            BinaryHeap::with_capacity(self.top_n_passages);

        let overlap_sents = OverlappingSents::new(text, self.window_size, self.overlap);

        for (index, (passage, range)) in overlap_sents.enumerate() {
            if let Some(passage_vec) = self.passage_scorer.embed_passage(passage) {
                let score = self.passage_scorer.score(&query_vectors, &passage_vec);

                let candidate = CandidatePassage {
                    passage,
                    index,
                    score,
                    range,
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
                b.range.start = stdx::ceil_char_boundary(text, a.range.end);
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

    pub fn summarize(&self, query: &str, text: &str) -> String {
        self.query_specific(query, text)
            .unwrap_or_else(|| intersperse(text.split_whitespace().take(1000), " ").collect())
    }
}

pub struct Summarizer {
    extractive: ExtractiveSummarizer,
    abstractive: AbstractiveSummarizer,
}

impl Summarizer {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Self {
            extractive: ExtractiveSummarizer::open(
                path.as_ref().join("dual_encoder").as_path(),
                50,
            )?,
            abstractive: AbstractiveSummarizer {
                model: Arc::new(AbstractiveModel::open(
                    path.as_ref().join("abstractive").as_path(),
                )?),
            },
        })
    }

    pub fn summarize(&self, query: &str, text: &str) -> String {
        let summary = self.extractive.summarize(query, text);
        match self.abstractive.summarize(summary.as_str()) {
            Ok(stream) => stream.collect(),
            Err(err) => {
                tracing::error!("Abstractive summarization failed: {}", err);
                summary
            }
        }
    }

    pub fn summarize_iter(&self, query: &str, text: &str) -> Result<impl Iterator<Item = String>> {
        let summary = self.extractive.summarize(query, text);
        self.abstractive.summarize(&summary)
    }
}

pub struct DualEncoder {
    model: tch::CModule,
    tokenizer: tokenizers::Tokenizer,
}

impl DualEncoder {
    pub fn open<P: AsRef<Path>>(folder: P) -> Result<Self> {
        let truncation = TruncationParams {
            max_length: 256,
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
        Ok(Self { model, tokenizer })
    }

    fn embed(&self, text: &str) -> Result<Tensor> {
        let query = self.tokenizer.encode(text, false).unwrap();

        let ids = query
            .get_ids()
            .iter()
            .map(|&id| id as i64)
            .collect::<Vec<_>>();

        let types = query
            .get_type_ids()
            .iter()
            .map(|&id| id as i64)
            .collect::<Vec<_>>();

        let mask = query
            .get_attention_mask()
            .iter()
            .map(|&id| id as i64)
            .collect::<Vec<_>>();

        let ids = Tensor::from_slice(&ids).reshape([1, -1]);
        let types = Tensor::from_slice(&types).reshape([1, -1]);
        let mask = Tensor::from_slice(&mask).reshape([1, -1]);

        Ok(self.model.forward_ts(&[ids, types, mask])?.squeeze())
    }
}

impl PassageScorer for DualEncoder {
    type QueryEmbedding = Tensor;

    type PassageEmbedding = Tensor;

    fn embed_query(&self, query: &str) -> Option<Self::QueryEmbedding> {
        self.embed(query).ok()
    }

    fn embed_passage(&self, passage: &str) -> Option<Self::PassageEmbedding> {
        self.embed(passage).ok()
    }

    fn score(&self, query: &Self::QueryEmbedding, passage: &Self::PassageEmbedding) -> f32 {
        let score = query.dot(&passage.transpose(0, 0));

        score.double_value(&[]) as f32
    }
}

const TRUNCATE_INPUT_ABSTRACTIVE: usize = 1024;
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
            max_length: TRUNCATE_INPUT_ABSTRACTIVE,
            ..Default::default()
        };

        let padding = PaddingParams {
            ..Default::default()
        };

        let mut tokenizer =
            tokenizers::Tokenizer::from_file(folder.as_ref().join("tokenizer.json"))?;

        tokenizer.with_truncation(Some(truncation))?;
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

    fn parse_decoder_output(
        &self,
        output: IValue,
    ) -> std::result::Result<(Tensor, Vec<Vec<ClonableTensor>>), Error> {
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

    pub fn summarize(&self, text: &str) -> Result<impl Iterator<Item = String>> {
        self.summarize_with_max(text, Some(128))
    }

    pub fn summarize_with_max(
        &self,
        text: &str,
        max_summary_tokens: Option<usize>,
    ) -> Result<impl Iterator<Item = String>> {
        let ids = self
            .model
            .tokenizer
            .encode(text, false)
            .unwrap()
            .get_ids()
            .iter()
            .map(|id| *id as i64)
            .collect_vec();

        let ids = Tensor::from_slice(&ids).reshape([1, -1]);

        let decoder_tokens = Tensor::from_slice(&[
            self.model.begin_decoder_token as i64,
            self.model.bos_token_id as i64,
        ])
        .reshape([1, -1]);

        self.generate(&ids, &decoder_tokens, max_summary_tokens)
    }

    fn generate(
        &self,
        encoder_ids: &Tensor,
        decoder_start_tokens: &Tensor,
        max_new_tokens: Option<usize>,
    ) -> Result<impl Iterator<Item = String>> {
        let tau = 0.8;
        let temp = 1.0;

        let encoded = self.model.initial(encoder_ids, decoder_start_tokens)?;

        let token_it = TokenStreamingGenerator {
            model: Arc::clone(&self.model),
            tau,
            temp,
            banned_tokens: vec![self.model.bos_token_id as i64],
            end_tokens: vec![self.model.eos_token_id as i64],
            next_token_logits: Some(encoded.next_token_logits),
            memory: encoded.memory,
            num_tokens_generated: 0,
            max_new_tokens,
        };

        let it = StringStreamingGenerator {
            token_streamer: token_it,
            tokens: Vec::new(),
        };

        Ok(it)
    }
}

#[derive(Clone)]
pub struct BartMemory {
    encoder_hidden_states: ClonableTensor,
    past_key_values: Vec<Vec<ClonableTensor>>,
}

impl AbstractiveModel {
    fn initial(
        &self,
        encoder_ids: &Tensor,
        decoder_ids: &Tensor,
    ) -> std::result::Result<DecoderOutput, Error> {
        let encoder_hidden_states = self.encoder.forward_ts(&[encoder_ids])?;

        let decoder_output = self.decoder.forward_is(&[
            IValue::Tensor(decoder_ids.shallow_clone()),
            IValue::Tensor(encoder_hidden_states.shallow_clone()),
        ])?;

        let (logits, past_key_values) = self.parse_decoder_output(decoder_output)?;

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
        last_token: i64,
        memory: &BartMemory,
    ) -> std::result::Result<DecoderOutput, Error> {
        let ids = Tensor::from_slice(&[last_token]).reshape([1, 1]);

        let past_key_value: IValue = IValue::Tuple(
            memory
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
            IValue::Tensor(memory.encoder_hidden_states.clone().0),
            past_key_value,
        ])?;

        let (logits, past_key_values) = self.parse_decoder_output(decoder_output)?;

        // logits is [batch_size, seq_len, vocab_size]
        // get last token logits
        let next_token_logits = logits.select(1, logits.size()[1] - 1).squeeze();

        Ok(DecoderOutput {
            next_token_logits,
            memory: BartMemory {
                encoder_hidden_states: memory.encoder_hidden_states.clone(),
                past_key_values,
            },
        })
    }
}

pub struct TokenStreamingGenerator {
    model: Arc<AbstractiveModel>,
    tau: f64,
    temp: f64,
    banned_tokens: Vec<i64>,
    end_tokens: Vec<i64>,
    next_token_logits: Option<Tensor>,
    memory: BartMemory,
    num_tokens_generated: usize,
    max_new_tokens: Option<usize>,
}

impl Iterator for TokenStreamingGenerator {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.next_token_logits {
            Some(next_token_logits) => {
                if let Some(max_new_tokens) = self.max_new_tokens {
                    if self.num_tokens_generated >= max_new_tokens {
                        self.next_token_logits = None;
                        return None;
                    }
                }

                let mut probs = next_token_logits.softmax(-1, Kind::Float).squeeze();

                // remove banned tokens
                for token in &self.banned_tokens {
                    let _ = probs.index_fill_(0, &Tensor::from_slice(&[*token]), 0.0);
                }

                let next_token = llm_utils::sample_typical(probs, self.temp, self.tau);

                if self.end_tokens.contains(&next_token) {
                    self.next_token_logits = None;
                    return None;
                }

                let encoded = self.model.step(next_token, &self.memory).ok()?;

                self.next_token_logits = Some(encoded.next_token_logits);
                self.memory = encoded.memory;

                self.num_tokens_generated += 1;

                Some(next_token)
            }
            None => None,
        }
    }
}

pub struct StringStreamingGenerator {
    token_streamer: TokenStreamingGenerator,
    tokens: Vec<u32>,
}

impl Iterator for StringStreamingGenerator {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.token_streamer.next() {
                Some(token) => {
                    self.tokens.push(token as u32);

                    if let Some(s) = self
                        .token_streamer
                        .model
                        .tokenizer
                        .decode(&self.tokens, true)
                        .ok()
                        .and_then(|s| {
                            if !s.contains('\u{fffd}') {
                                // valid utf-8 string
                                Some(s)
                            } else {
                                None
                            }
                        })
                    {
                        self.tokens.clear();
                        return Some(s);
                    }
                }
                None => {
                    if self.tokens.is_empty() {
                        return None;
                    } else {
                        match self
                            .token_streamer
                            .model
                            .tokenizer
                            .decode(&self.tokens, true)
                        {
                            Ok(s) => {
                                self.tokens.clear();

                                if !s.contains('\u{fffd}') {
                                    return Some(s);
                                } else {
                                    return None;
                                }
                            }
                            Err(_) => {
                                self.tokens.clear();
                                return None;
                            }
                        }
                    }
                }
            }
        }
    }
}

struct DecoderOutput {
    next_token_logits: Tensor,
    memory: BartMemory,
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
                AbstractiveModel::open("../../data/summarizer/abstractive")
                    .expect("abstractive summary model not found"),
            ),
        };

        let text = r#"Aristotle (/ˈærɪstɒtəl/;[1] Greek: Ἀριστοτέλης Aristotélēs, pronounced [aristotélɛːs]; 384–322 BC) was an Ancient Greek philosopher and polymath. His writings cover a broad range of subjects including physics, biology, zoology, metaphysics, logic, ethics, aesthetics, poetry, drama, music, rhetoric, psychology, linguistics, economics, politics, meteorology, geology, and government. As the founder of the Peripatetic school of philosophy in the Lyceum in Athens, he began the wider Aristotelian tradition that followed, which set the groundwork for the development of modern science.
        Little is known about Aristotle's life. He was born in the city of Stagira in Northern Greece during the Classical period. His father, Nicomachus, died when Aristotle was a child, and he was brought up by a guardian. At seventeen or eighteen years of age he joined Plato's Academy in Athens and remained there until the age of thirty-seven (c. 347 BC). Shortly after Plato died, Aristotle left Athens and, at the request of Philip II of Macedon, tutored his son Alexander the Great beginning in 343 BC. He established a library in the Lyceum which helped him to produce many of his hundreds of books on papyrus scrolls.
        Though Aristotle wrote many elegant treatises and dialogues for publication, only around a third of his original output has survived, none of it intended for publication. Aristotle provided a complex synthesis of the various philosophies existing prior to him. It was above all from his teachings that the West inherited its intellectual lexicon, as well as problems and methods of inquiry. As a result, his philosophy has exerted a unique influence on almost every form of knowledge in the West and it continues to be a subject of contemporary philosophical discussion.
        Aristotle's views profoundly shaped medieval scholarship. The influence of physical science extended from Late Antiquity and the Early Middle Ages into the Renaissance, and were not replaced systematically until the Enlightenment and theories such as classical mechanics were developed. Some of Aristotle's zoological observations found in his biology, such as on the hectocotyl (reproductive) arm of the octopus, were disbelieved until the 19th century. He also influenced Judeo-Islamic philosophies during the Middle Ages, as well as Christian theology, especially the Neoplatonism of the Early Church and the scholastic tradition of the Catholic Church. Aristotle was revered among medieval Muslim scholars as "The First Teacher", and among medieval Christians like Thomas Aquinas as simply "The Philosopher", while the poet Dante called him "the master of those who know". His works contain the earliest known formal study of logic, and were studied by medieval scholars such as Peter Abelard and John Buridan. Aristotle's influence on logic continued well into the 19th century. In addition, his ethics, though always influential, gained renewed interest with the modern advent of virtue ethics."#;

        let start = std::time::Instant::now();
        let summary = summarizer
            .summarize_with_max(text, Some(16))
            .unwrap()
            .collect::<String>();

        println!("Elapsed: {:?}", start.elapsed());
        println!("{:?}", &summary);

        assert!(summary.len() > 50);
    }
}
