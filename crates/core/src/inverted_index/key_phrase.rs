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

use itertools::Itertools;
use lending_iter::LendingIterator;
use std::str;
use std::{cmp::Reverse, collections::HashMap};

use crate::{
    schema::text_field::{self, TextField},
    SortableFloat,
};

const DONT_SCORE_TOP_PERCENT_OF_WORDS: f64 = 0.002;
const NON_ALPHABETIC_CHAR_THRESHOLD: f64 = 0.25;

struct Scorer {
    word_freq: HashMap<String, u64>,
    word_docs: HashMap<String, u64>,
    total_doc_freq: f64,
    num_docs: u64,
    word_freq_threshold: u64,
}

impl Scorer {
    fn new(searcher: &tantivy::Searcher, field: tantivy::schema::Field) -> Self {
        let mut word_freq = HashMap::new();
        let mut word_docs = HashMap::new();
        let mut total_doc_freq = 0.0;
        let mut num_docs = 0;

        for seg_reader in searcher.segment_readers() {
            let inv_index = seg_reader.inverted_index(field).unwrap();
            let mut stream = inv_index.terms().stream().unwrap();

            while let Some((term, _)) = stream.next() {
                let term_str = str::from_utf8(term).unwrap().to_string();

                let words = term_str.split_whitespace().collect::<Vec<_>>();
                if words.is_empty() {
                    continue;
                }

                for word in &words {
                    *word_freq.entry(word.to_string()).or_insert(0) += 1;
                }

                for word in words.into_iter().unique() {
                    *word_docs.entry(word.to_string()).or_insert(0) += 1;
                }
            }

            total_doc_freq += seg_reader.num_docs() as f64;
            num_docs += inv_index.terms().num_terms() as u64;
        }

        let num_words = word_freq.len() as f64;
        let word_freq_threshold = *word_freq
            .values()
            .sorted_by(|a, b| b.cmp(a))
            .nth((num_words * DONT_SCORE_TOP_PERCENT_OF_WORDS).ceil() as usize)
            .unwrap_or(&0);

        Self {
            word_freq,
            word_docs,
            total_doc_freq,
            num_docs,
            word_freq_threshold,
        }
    }

    #[inline]
    fn word_freq(&self) -> &HashMap<String, u64> {
        &self.word_freq
    }

    #[inline]
    fn word_docs(&self) -> &HashMap<String, u64> {
        &self.word_docs
    }

    #[inline]
    fn total_doc_freq(&self) -> f64 {
        self.total_doc_freq
    }

    #[inline]
    fn num_docs(&self) -> u64 {
        self.num_docs
    }

    #[inline]
    fn word_freq_threshold(&self) -> u64 {
        self.word_freq_threshold
    }

    fn score(&self, words: &[&str], doc_freq: u32) -> f64 {
        let word_freq_threshold = self.word_freq_threshold();
        let mut score = 0.0;
        let num_words = words.len();
        for word in words.iter().unique() {
            let word_chars = word.chars().count();
            if word.chars().filter(|c| !c.is_alphabetic()).count() as f64 / word_chars as f64
                > NON_ALPHABETIC_CHAR_THRESHOLD
            {
                continue;
            }

            let word_docs = self.word_docs().get(*word).unwrap_or(&0);
            let wf = *self.word_freq().get(*word).unwrap_or(&0);

            if wf > word_freq_threshold {
                continue;
            }

            let tf = (wf as f64) / num_words as f64;
            let idf = ((self.num_docs() as f64) / (*word_docs as f64) + 1.0).ln();

            score += tf * idf;
        }

        let cf = doc_freq as f64 / (self.total_doc_freq() + 1.0);

        score * (1.0 - cf)
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct KeyPhrase {
    phrase: String,
    score: f64,
}

impl KeyPhrase {
    pub fn new(phrase: String, score: f64) -> Self {
        Self { phrase, score }
    }

    pub fn compute_top(reader: &tantivy::IndexReader, top_n: usize) -> Vec<Self> {
        let searcher = reader.searcher();
        let field = searcher
            .schema()
            .get_field(text_field::KeyPhrases.name())
            .unwrap();

        let scorer = Scorer::new(&searcher, field);

        let mut keywords: HashMap<String, f64> = HashMap::new();

        for seg_reader in searcher.segment_readers() {
            let inv_index = seg_reader.inverted_index(field).unwrap();
            let mut stream = inv_index.terms().stream().unwrap();
            while let Some((term, info)) = stream.next() {
                let term_str = str::from_utf8(term).unwrap().to_string();
                let num_chars = term_str.chars().count();

                if term_str.chars().filter(|c| !c.is_alphabetic()).count() as f64 / num_chars as f64
                    > NON_ALPHABETIC_CHAR_THRESHOLD
                {
                    continue;
                }

                let left_paren = term_str.chars().filter(|c| c == &'(').count();
                let right_paren = term_str.chars().filter(|c| c == &')').count();

                if left_paren != right_paren {
                    continue;
                }

                let words = term_str.split_whitespace().collect::<Vec<_>>();
                let score = scorer.score(&words, info.doc_freq);

                if score.is_normal() {
                    let term_str = words.join(" ");
                    *keywords.entry(term_str).or_default() += score;
                }
            }
        }

        crate::sorted_k(
            keywords
                .into_iter()
                .map(|(phrase, score)| (Reverse(SortableFloat::from(score)), phrase)),
            top_n,
        )
        .into_iter()
        .map(|(Reverse(score), phrase)| KeyPhrase::new(phrase, score.into()))
        .collect()
    }

    pub fn score(&self) -> f64 {
        self.score
    }

    pub fn text(&self) -> &str {
        &self.phrase
    }
}
