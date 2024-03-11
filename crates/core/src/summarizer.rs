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

use candle_core::Tensor;
use futures::stream::Stream;
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, VecDeque},
    ops::Range,
    path::Path,
};
use tokio_stream::StreamExt;

use crate::{llm_utils::OpenAiApi, models::dual_encoder::DualEncoder, Result};
use itertools::{intersperse, Itertools};

use crate::ceil_char_boundary;

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

trait PassageScorer {
    type QueryEmbedding;
    type PassageEmbedding;

    fn embed_query(&self, query: &str) -> Option<Self::QueryEmbedding>;
    fn embed_passage(&self, passage: &str) -> Option<Self::PassageEmbedding>;

    fn score(&self, query: &Self::QueryEmbedding, passage: &Self::PassageEmbedding) -> f32;
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
            window_size: 64,
            overlap: 0,
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
    pub fn new<P: AsRef<Path>>(
        path: P,
        llm_api_base: String,
        model_name: String,
        api_key: Option<String>,
    ) -> Result<Self> {
        Ok(Self {
            extractive: ExtractiveSummarizer::open(
                path.as_ref().join("dual_encoder").as_path(),
                16,
            )?,
            abstractive: AbstractiveSummarizer::new(llm_api_base, model_name, api_key),
        })
    }

    pub async fn summarize(&self, query: &str, text: &str) -> Result<impl Stream<Item = String>> {
        let words = text.split_whitespace().count();

        if words < 2000 {
            self.abstractive.summarize(text).await
        } else {
            let summary = self.extractive.summarize(query, text);
            self.abstractive.summarize(&summary).await
        }
    }
}

impl PassageScorer for DualEncoder {
    type QueryEmbedding = Tensor;

    type PassageEmbedding = Tensor;

    fn embed_query(&self, query: &str) -> Option<Self::QueryEmbedding> {
        self.embed(&[query.to_string()]).ok()
    }

    fn embed_passage(&self, passage: &str) -> Option<Self::PassageEmbedding> {
        self.embed(&[passage.to_string()]).ok()
    }

    fn score(&self, query: &Self::QueryEmbedding, passage: &Self::PassageEmbedding) -> f32 {
        query
            .matmul(&passage.t().unwrap())
            .unwrap()
            .get(0)
            .unwrap()
            .squeeze(0)
            .unwrap()
            .to_dtype(candle_core::DType::F32)
            .unwrap()
            .to_vec0()
            .unwrap()
    }
}

const TRUNCATE_WORDS_ABSTRACTIVE: usize = 1024;

pub struct AbstractiveSummarizer {
    api: String,
    model: String,
    api_key: Option<String>,
}

impl AbstractiveSummarizer {
    pub fn new(api: String, model: String, api_key: Option<String>) -> Self {
        Self {
            api,
            model,
            api_key,
        }
    }

    fn client(&self, max_tokens: Option<u64>) -> OpenAiApi {
        let mut builder = OpenAiApi::builder(self.api.clone(), self.model.clone())
            .top_p(0.9)
            .temp(0.0)
            .stop(vec!["</s>", "<|endoftext|>"]);

        if let Some(api_key) = &self.api_key {
            builder = builder.api_key(api_key.clone());
        }

        if let Some(max_tokens) = max_tokens {
            builder = builder.max_tokens(max_tokens);
        }

        builder.build()
    }

    fn first_prompt(&self, text: &str) -> String {
        let wc = 100;
        format!(
            r##"[INST] I will provide you with piece of content (e.g. articles, papers, documentation, etc.)

You will generate summaries of the content. Refer to the content using words as "this article discusses" etc.
The summaries should be -{wc} words.

After you have generated the summary, identify 1-3 informative entities from the content which are missing from the summary that can be used to make a more concise summary.

A Missing Entity is:

Relevant: to the main story.
Specific: descriptive yet concise (5 words or fewer).
Novel: not in the previous summary.
Faithful: present in the content piece.
Anywhere: located anywhere in the Article.

Finaly, give guidance on how to improve the summary.

Content to summarize:
{text}
[/INST]"##,
        )
    }

    fn final_prompt(&self, text: &str, prev_summary: &str) -> String {
        let wc = 100;
        format!(
            r##"[INST] I will provide you with piece of content (e.g. articles, papers, documentation, etc.) and a summary of the content.

You will improve upon the previous summary to generate a highly consice summary of the content. The summary should be -{wc} words.

Content to summarize:
{text}

Previous summary:
{prev_summary}
[/INST] Summary:"##,
        )
    }

    pub async fn summarize(&self, text: &str) -> Result<impl Stream<Item = String>> {
        let text = text
            .split_ascii_whitespace()
            .take(TRUNCATE_WORDS_ABSTRACTIVE)
            .join(" ");

        let prompt = self.first_prompt(&text);
        let first_summary = self.client(Some(2048)).generate(&prompt).await?;

        let prompt = self.final_prompt(&text, &first_summary);

        Ok(self
            .client(Some(128))
            .stream(&prompt)
            .await?
            .filter_map(|tok| tok.ok()))
    }
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
    fn test_dual_encoder() {
        let data_path = Path::new("../../data/summarizer/dual_encoder");
        if !data_path.exists() {
            // Skip the test if the test data is not available
            return;
        }
        let model = DualEncoder::open(data_path).expect("Failed to load model");
        let query = "What is the capital of France?";
        let pos = "The capital of France is Paris.";
        let neg = "The best baguette in Paris can be found at Boulangerie Pichard.";

        let query_emb = model.embed_query(query).unwrap();
        let pos_emb = model.embed_passage(pos).unwrap();
        let neg_emb = model.embed_passage(neg).unwrap();

        assert!(model.score(&query_emb, &pos_emb) > 0.0);
        assert!(model.score(&query_emb, &pos_emb) > model.score(&query_emb, &neg_emb));
    }
}
