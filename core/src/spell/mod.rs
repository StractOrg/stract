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
pub mod dictionary;
pub mod distance;
pub mod spell_checker;
pub mod splitter;
pub mod word2vec;

use itertools::intersperse;
use serde::{Deserialize, Serialize};

pub use self::dictionary::{Dictionary, DictionaryResult, EditStrategy, LogarithmicEdit};
pub use self::spell_checker::SpellChecker;
pub use self::splitter::TermSplitter;

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
