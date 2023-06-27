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

use fst::{automaton::Str, Automaton, IntoStreamer};

use crate::Result;
use std::path::Path;

pub struct Autosuggest {
    queries: fst::Set<Vec<u8>>,
}

impl Autosuggest {
    pub fn load_csv<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut queries: Vec<String> = Vec::new();

        let mut rdr = csv::Reader::from_path(path)?;
        for result in rdr.records() {
            let record = result?;
            if let Some(query) = record.get(0) {
                queries.push(query.to_string());
            }
        }

        queries.sort();

        let queries = fst::Set::from_iter(queries)?;

        Ok(Self { queries })
    }

    pub fn suggestions(&self, query: &str) -> Result<Vec<String>> {
        let query = query.to_ascii_lowercase();
        let q = Str::new(query.as_str()).starts_with();

        Ok(self
            .queries
            .search(q)
            .into_stream()
            .into_strs()?
            .into_iter()
            .take(10)
            .collect())
    }
}
