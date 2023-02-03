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
    collections::BinaryHeap,
};

use itertools::intersperse;

use crate::spell::word2vec::Word2Vec;

pub struct Summarizer {
    word2vec: Word2Vec,
    top_n_passages: usize,
}

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

impl Summarizer {
    pub fn summarize(&self, query: &str, text: &str) -> Option<String> {
        let query_vectors: Vec<_> = query
            .split_whitespace()
            .filter_map(|word| self.word2vec.get(word))
            .collect();

        if query_vectors.is_empty() {
            return None;
        }

        let mut best_passages: BinaryHeap<Reverse<CandidatePassage<'_>>> =
            BinaryHeap::with_capacity(self.top_n_passages);

        // for (index, passage) in text.split_whitespace().windows(100).enumerate() {
        for (index, passage) in text.split_whitespace().enumerate() {
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

        let extractive_summary: String =
            intersperse(best_passages.into_iter().map(|p| p.passage), ". ").collect();

        todo!("run abstractive summary model");
    }
}
