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
pub use spell_checker::SpellChecker;
pub use stupid_backoff::StupidBackoff;
pub use term_freqs::TermDict;
pub use trainer::FirstTrainer;
pub use trainer::FirstTrainerResult;
pub use trainer::SecondTrainer;

use fst::Streamer;
use std::ops::Range;

use crate::ceil_char_boundary;
use crate::floor_char_boundary;
use itertools::intersperse;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("FST error: {0}")]
    Fst(#[from] fst::Error),

    #[error("Serde error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Encode error: {0}")]
    Encode(#[from] bincode::error::EncodeError),

    #[error("Decode error: {0}")]
    Decode(#[from] bincode::error::DecodeError),

    #[error("Checker not found")]
    CheckerNotFound,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(
    PartialEq,
    Eq,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    Clone,
)]
pub struct Correction {
    original: String,
    pub terms: Vec<CorrectionTerm>,
}

#[derive(
    PartialEq,
    Eq,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    Clone,
)]
pub enum CorrectionTerm {
    Corrected { orig: String, correction: String },
    NotCorrected(String),
}

impl From<Correction> for String {
    fn from(correction: Correction) -> Self {
        intersperse(
            correction.terms.into_iter().map(|term| match term {
                CorrectionTerm::Corrected {
                    orig: _,
                    correction,
                } => correction,
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
    let skip = ["mr.", "ms.", "dr."];

    let mut res = Vec::new();
    let mut last_start = 0;

    let text = text.to_ascii_lowercase();

    // We should really do something more clever than this.
    // Tried using `SRX`[https://docs.rs/srx/latest/srx/] but it was a bit too slow.
    for (end, _) in text
        .char_indices()
        .filter(|(_, c)| matches!(c, '.' | '\n' | '?' | '!'))
    {
        let end = ceil_char_boundary(&text, end + 1);

        if skip.iter().any(|p| text[last_start..end].ends_with(p)) {
            continue;
        }

        // skip 'site.com', '...', '!!!' etc.
        if !text[end..].starts_with(|c: char| c.is_ascii_whitespace()) {
            continue;
        }

        let mut start = last_start;

        while start < end && text[start..].starts_with(|c: char| c.is_whitespace()) {
            start = ceil_char_boundary(&text, start + 1);
        }

        // just a precaution
        if start > end {
            continue;
        }

        res.push(start..end);

        last_start = end;
    }

    let mut start = last_start;

    while start < text.len() && text[start..].starts_with(|c: char| c.is_whitespace()) {
        start = floor_char_boundary(&text, start + 1);
    }

    res.push(start..text.len());

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sentence_ranges() {
        let text = "This is a sentence. This is another sentence. This is a third sentence.";
        let ranges = sentence_ranges(text);
        assert_eq!(ranges.len(), 3);

        assert_eq!(&text[ranges[0].clone()], "This is a sentence.");
        assert_eq!(&text[ranges[1].clone()], "This is another sentence.");
        assert_eq!(&text[ranges[2].clone()], "This is a third sentence.");

        let text = "This is a sentence. This is another sentence. This is a third sentence";
        let ranges = sentence_ranges(text);
        assert_eq!(ranges.len(), 3);

        assert_eq!(&text[ranges[0].clone()], "This is a sentence.");
        assert_eq!(&text[ranges[1].clone()], "This is another sentence.");
        assert_eq!(&text[ranges[2].clone()], "This is a third sentence");

        let text = "mr. roberts";

        let ranges = sentence_ranges(text);

        assert_eq!(ranges.len(), 1);
        assert_eq!(&text[ranges[0].clone()], "mr. roberts");

        let text = "site.com is the best";

        let ranges = sentence_ranges(text);

        assert_eq!(ranges.len(), 1);
        assert_eq!(&text[ranges[0].clone()], "site.com is the best");
    }
}
