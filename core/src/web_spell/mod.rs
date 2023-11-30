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

//! This module contains the spell checker. It is based on the paper
//! http://static.googleusercontent.com/media/research.google.com/en/us/pubs/archive/36180.pdf
//! from google.
mod error_model;
pub mod spell_checker;
mod stupid_backoff;
mod term_freqs;
mod trainer;

pub use error_model::ErrorModel;
pub use stupid_backoff::StupidBackoff;
pub use term_freqs::TermDict;
pub use trainer::FirstTrainer;
pub use trainer::FirstTrainerResult;
pub use trainer::SecondTrainer;

use fst::Streamer;
use std::ops::Range;

use crate::floor_char_boundary;
use itertools::intersperse;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("FST error: {0}")]
    Fst(#[from] fst::Error),

    #[error("Serde error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
pub struct Correction {
    original: String,
    pub terms: Vec<CorrectionTerm>,
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Clone)]
pub enum CorrectionTerm {
    Corrected(String),
    NotCorrected(String),
}

impl From<Correction> for String {
    fn from(correction: Correction) -> Self {
        intersperse(
            correction.terms.into_iter().map(|term| match term {
                CorrectionTerm::Corrected(correction) => correction,
                CorrectionTerm::NotCorrected(orig) => orig,
            }),
            " ".to_string(),
        )
        .collect()
    }
}

impl Correction {
    pub fn empty(original: String) -> Self {
        Self {
            original,
            terms: Vec::new(),
        }
    }

    pub fn push(&mut self, term: CorrectionTerm) {
        self.terms.push(term);
    }

    pub fn is_all_orig(&self) -> bool {
        self.terms
            .iter()
            .all(|term| matches!(term, CorrectionTerm::NotCorrected(_)))
    }
}

pub fn sentence_ranges(text: &str) -> Vec<Range<usize>> {
    let mut res = Vec::new();
    let mut last_start = 0;

    // We should really do something more clever than this.
    // Tried using `SRX`[https://docs.rs/srx/latest/srx/] but it was a bit too slow.
    for (end, _) in text
        .char_indices()
        .filter(|(_, c)| matches!(c, '.' | '\n' | '?' | '!'))
    {
        res.push(last_start..end + 1);
        last_start = floor_char_boundary(text, end + 2);
    }

    res
}

pub fn tokenize(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split_whitespace()
        .filter(|s| {
            !s.chars()
                .any(|c| !c.is_ascii_alphanumeric() && c != '-' && c != '_')
        })
        .map(|s| s.to_string())
        .collect()
}
pub struct MergePointer<'a> {
    pub term: String,
    pub value: u64,
    pub stream: fst::map::Stream<'a>,
    pub is_finished: bool,
}

impl<'a> MergePointer<'a> {
    pub fn advance(&mut self) -> bool {
        self.is_finished = self
            .stream
            .next()
            .map(|(term, value)| {
                self.term = std::str::from_utf8(term).unwrap().to_string();
                self.value = value;
            })
            .is_none();

        !self.is_finished
    }
}
