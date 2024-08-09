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

use lending_iter::LendingIterator;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::str;

use crate::{
    schema::text_field::{self, TextField},
    SortableFloat,
};

const NON_ALPHABETIC_CHAR_THRESHOLD: f64 = 0.25;

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

        let mut keywords: BinaryHeap<(Reverse<SortableFloat>, String)> =
            BinaryHeap::with_capacity(top_n);

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

                if words.is_empty() {
                    continue;
                }

                let score = info.doc_freq as f64;

                if score.is_normal() {
                    let term_str = words.join(" ");

                    if keywords.len() >= top_n {
                        if let Some(mut min) = keywords.peek_mut() {
                            if score > min.0 .0.into() {
                                *min = (Reverse(score.into()), term_str);
                            }
                        }
                    } else {
                        keywords.push((Reverse(score.into()), term_str));
                    }
                }
            }
        }

        keywords
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
