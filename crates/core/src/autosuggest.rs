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

//! Autosuggest provides the functionality for the little dropdown that appears
//! when you type something into the search bar and queries are suggested.
//! It uses a finite state transducer (fst) to store popular queries
//! and performs a prefix search on the fst to find suggestions.

use std::collections::HashMap;

use fst::{automaton::Str, Automaton, IntoStreamer};
use itertools::Itertools;

use crate::{inverted_index::KeyPhrase, Result};

pub struct Autosuggest {
    queries: fst::Set<Vec<u8>>,
    scores: HashMap<String, f64>,
}

impl Autosuggest {
    pub fn from_key_phrases(key_phrases: Vec<KeyPhrase>) -> Result<Self> {
        let mut queries: Vec<String> = Vec::new();
        let mut scores: HashMap<String, f64> = HashMap::new();

        for key_phrase in key_phrases {
            queries.push(key_phrase.text().to_string());
            scores.insert(key_phrase.text().to_string(), key_phrase.score());
        }

        queries.sort();
        queries.dedup();

        let queries = fst::Set::from_iter(queries)?;

        Ok(Self { queries, scores })
    }

    pub fn suggestions(&self, query: &str) -> Result<Vec<String>> {
        let query = query.to_ascii_lowercase();
        let q = Str::new(query.as_str()).starts_with();

        let mut candidates: Vec<(String, f64)> = self
            .queries
            .search(q)
            .into_stream()
            .into_strs()?
            .into_iter()
            .take(64)
            .map(|s| {
                let score = self.scores.get(&s).unwrap_or(&0.0);
                (s, *score)
            })
            .collect();

        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

        Ok(candidates
            .into_iter()
            .map(|(s, _)| s)
            .take(10)
            .sorted()
            .collect())
    }

    pub fn scores(&self) -> &HashMap<String, f64> {
        &self.scores
    }
}
