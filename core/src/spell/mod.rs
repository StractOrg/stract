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
pub mod dictionary;
pub mod distance;
pub mod spell_checker;
pub mod splitter;
pub mod word2vec;

use std::ops::Range;

use itertools::intersperse;
use serde::{Deserialize, Serialize};
use tantivy::tokenizer::Tokenizer as _;
use tracing::info;

use crate::index::Index;
use crate::query::parser::Term;
use crate::schema::TextField;
use crate::searcher::SearchQuery;
use crate::tokenizer::Tokenizer;
use crate::{floor_char_boundary, query};

use self::dictionary::DictionaryBuilder;
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

pub struct Spell {
    dict: Dictionary,
    spell_checker: SpellChecker<LogarithmicEdit>,
}

impl Spell {
    pub fn for_index(index: &Index) -> Self {
        let dict = Self::build_dict(index);
        let spell_checker = SpellChecker::new(dict.clone(), LogarithmicEdit::new(4));

        Self {
            dict,
            spell_checker,
        }
    }
    fn build_dict(index: &Index) -> Dictionary {
        info!("Building spell correction dictionary");
        let searcher = index.inverted_index.tv_searcher();
        let schema = searcher.schema();
        let mut dict = DictionaryBuilder::new(100_000);
        #[allow(unused_assignments)]
        let mut limit_terms: Option<usize> = None;
        #[cfg(debug_assertions)]
        {
            limit_terms = Some(100);
        }

        for segment in searcher.segment_readers() {
            let inv_index = segment
                .inverted_index(schema.get_field(TextField::CleanBody.name()).unwrap())
                .unwrap();
            let term_dict = inv_index.terms();
            let mut stream = term_dict.stream().unwrap();
            let mut count = 0;
            while let Some((term, info)) = stream.next() {
                if let Some(limit) = limit_terms {
                    if count > limit {
                        break;
                    }
                }

                let term = std::str::from_utf8(term).unwrap();

                if !term.is_empty()
                    && term
                        .chars()
                        .all(|c| c.is_ascii_alphabetic() || c.is_whitespace())
                {
                    dict.add_monogram(term.to_ascii_lowercase(), info.doc_freq as u64);
                    count += 1;
                }
            }
        }

        for segment in searcher.segment_readers() {
            let inv_index = segment
                .inverted_index(
                    schema
                        .get_field(TextField::CleanBodyBigrams.name())
                        .unwrap(),
                )
                .unwrap();
            let term_dict = inv_index.terms();
            let mut stream = term_dict.stream().unwrap();
            let mut count = 0;

            while let Some((term, info)) = stream.next() {
                if let Some(limit) = limit_terms {
                    if count > limit {
                        break;
                    }
                }

                let term = std::str::from_utf8(term).unwrap();
                if !term.is_empty()
                    && term
                        .chars()
                        .all(|c| c.is_ascii_alphabetic() || c.is_whitespace())
                {
                    let mut tokenizer = Tokenizer::default().token_stream(term);
                    let first = tokenizer.next().unwrap().text.to_ascii_lowercase();
                    let second = tokenizer.next().unwrap().text.to_ascii_lowercase();
                    dict.add_bigram((first, second), info.doc_freq as u64);
                    count += 1;
                }
            }
        }

        for segment in searcher.segment_readers() {
            let inv_index = segment
                .inverted_index(
                    schema
                        .get_field(TextField::CleanBodyTrigrams.name())
                        .unwrap(),
                )
                .unwrap();
            let term_dict = inv_index.terms();
            let mut stream = term_dict.stream().unwrap();
            let mut count = 0;

            while let Some((term, info)) = stream.next() {
                if let Some(limit) = limit_terms {
                    if count > limit {
                        break;
                    }
                }

                let term = std::str::from_utf8(term).unwrap();
                if !term.is_empty()
                    && term
                        .chars()
                        .all(|c| c.is_ascii_alphabetic() || c.is_whitespace())
                {
                    let mut tokenizer = Tokenizer::default().token_stream(term);
                    let first = tokenizer.next().unwrap().text.to_ascii_lowercase();
                    let second = tokenizer.next().unwrap().text.to_ascii_lowercase();
                    let third = tokenizer.next().unwrap().text.to_ascii_lowercase();
                    dict.add_trigram((first, second, third), info.doc_freq as u64);
                    count += 1;
                }
            }
        }

        dict.build()
    }

    fn spell_check(&self, terms: &[String]) -> Option<Correction> {
        let mut original = String::new();
        let num_terms = terms.len();
        let terms = terms.iter().map(|s| s.as_str()).collect::<Vec<_>>();

        for term in &terms {
            original.push_str(term);
            original.push(' ');
        }
        original = original.trim_end().to_string();

        let mut possible_correction = Correction::empty(original);

        for i in 0..num_terms {
            let before = if i > 0 { &terms[..i] } else { &[] };
            let term = &terms[i];
            let after = if i < num_terms - 1 {
                &terms[i + 1..]
            } else {
                &[]
            };

            match self.spell_checker.correct(before, term, after) {
                Some(correction) => possible_correction
                    .push(CorrectionTerm::Corrected(correction.to_ascii_lowercase())),
                None => possible_correction
                    .push(CorrectionTerm::NotCorrected(term.to_ascii_lowercase())),
            }
        }

        if possible_correction.is_all_orig() {
            None
        } else {
            Some(possible_correction)
        }
    }

    fn split_words(&self, terms: &[String]) -> Option<Correction> {
        let splitter = TermSplitter::new(&self.dict);

        let mut original = String::new();

        for term in terms {
            original.push_str(term);
            original.push(' ');
        }
        original = original.trim_end().to_string();

        let mut possible_correction = Correction::empty(original);

        for term in terms {
            let split = splitter.split(term.as_str());
            if split.is_empty() {
                possible_correction.push(CorrectionTerm::NotCorrected(term.to_string()));
            } else {
                for s in split {
                    possible_correction.push(CorrectionTerm::Corrected(s.to_string()))
                }
            }
        }

        if possible_correction.is_all_orig() {
            None
        } else {
            Some(possible_correction)
        }
    }

    pub fn correction(&self, query: &SearchQuery) -> Option<Correction> {
        let terms: Vec<_> = query::parser::parse(&query.query)
            .into_iter()
            .filter_map(|term| match *term {
                Term::Simple(s) => Some(s),
                _ => None,
            })
            .map(|s| s.to_ascii_lowercase())
            .collect();

        self.spell_check(&terms)
            .or_else(|| self.split_words(&terms))
    }
}
