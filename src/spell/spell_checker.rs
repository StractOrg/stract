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
use crate::spell::dictionary::EditStrategy;
use crate::spell::{Dictionary, DictionaryResult};
use std::iter::FromIterator;

pub struct SpellChecker<'a, T: EditStrategy, const DICT_N: usize> {
    dict: &'a Dictionary<DICT_N>,
    edit_strategy: T,
}

impl<'a, T: EditStrategy, const DICT_N: usize> SpellChecker<'a, T, DICT_N> {
    pub fn new(dict: &'a Dictionary<DICT_N>, edit_strategy: T) -> Self {
        SpellChecker {
            dict,
            edit_strategy,
        }
    }

    pub fn correct_top(&self, term: &str, top_n: usize) -> Vec<String> {
        let mut res = Vec::new();

        // this is sorted by increasing edit distance
        for corrections in self
            .dict
            .all_probabilities(term, self.edit_strategy.distance_for_string(term))
            .iter_mut()
        {
            let mut corrections: Vec<&DictionaryResult> = Vec::from_iter(corrections.iter());
            corrections.sort_by(|a, b| b.prob.partial_cmp(&a.prob).unwrap());

            res.extend(
                corrections
                    .into_iter()
                    .map(|dict_result| dict_result.correction.clone())
                    .take(top_n - res.len()),
            );

            if res.len() >= top_n {
                return res;
            }
        }

        res
    }

    pub fn correct(&self, term: &str) -> Option<String> {
        if let Some(correction) = self.correct_top(term, 1).into_iter().next() {
            if correction.to_ascii_lowercase() == term.to_ascii_lowercase() {
                None
            } else {
                Some(correction)
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spell::{dictionary::MaxEdit, LogarithmicEdit};

    #[test]
    fn simple_corrections() {
        let mut dict = Dictionary::default();

        dict.insert("this");
        dict.insert("is");
        dict.insert("a");
        dict.insert("test");
        dict.insert("test");
        dict.insert("pest");

        dict.commit().unwrap();

        let spell_checker = SpellChecker::new(&dict, MaxEdit::new(1));

        assert_eq!(spell_checker.correct("tst"), Some("test".to_string()));
        assert_eq!(spell_checker.correct("ths"), Some("this".to_string()));
        assert_eq!(spell_checker.correct("thes"), Some("this".to_string()));
        assert_eq!(spell_checker.correct("is"), None);
        assert_eq!(
            spell_checker.correct_top("thes", 3),
            vec!["this".to_string()]
        );
        assert_eq!(
            spell_checker.correct_top("dest", 3),
            vec!["test".to_string(), "pest".to_string()]
        );
        assert_eq!(
            spell_checker.correct_top("dest", 1),
            vec!["test".to_string()]
        );
    }

    #[test]
    fn correct_uncontained_word() {
        let mut dict = Dictionary::default();

        dict.insert("this");
        dict.insert("is");
        dict.insert("a");
        dict.insert("test");
        dict.insert("test");

        dict.commit().unwrap();

        let spell_checker = SpellChecker::new(&dict, MaxEdit::new(1));

        assert_eq!(spell_checker.correct("what"), None);
        assert!(spell_checker.correct_top("what", 2).is_empty());
    }

    #[test]
    fn prioritise_low_distance_words() {
        let mut dict = Dictionary::default();

        dict.insert("this");
        dict.insert("is");
        dict.insert("a");
        dict.insert("test");
        dict.insert("contest");
        dict.insert("contest");

        dict.commit().unwrap();

        let spell_checker = SpellChecker::new(&dict, MaxEdit::new(4));

        assert_eq!(spell_checker.correct("dest"), Some("test".to_string()));
        assert_eq!(
            spell_checker.correct_top("dest", 3),
            vec!["test".to_string(), "is".to_string(), "contest".to_string()]
        );
    }

    #[test]
    fn correct_sorting_multiple_hits() {
        let mut dict = Dictionary::default();

        dict.insert("the");
        dict.insert("the");
        dict.insert("the");
        dict.insert("he");

        dict.commit().unwrap();

        let spell_checker = SpellChecker::new(&dict, LogarithmicEdit::new(4));

        assert_eq!(spell_checker.correct("fhe"), Some("the".to_string()));
        assert_eq!(
            spell_checker.correct_top("fhe", 3),
            vec!["the".to_string(), "he".to_string()]
        );
    }
}
