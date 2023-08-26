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
use crate::spell::dictionary::EditStrategy;
use crate::spell::Dictionary;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet, VecDeque};

use super::dictionary::TermId;
use super::distance::LevenshteinDistance;

pub struct SpellChecker<T: EditStrategy> {
    dict: Dictionary,
    edit_strategy: T,
    deletes: BTreeMap<String, Vec<TermId>>,
}

fn deletes_rec(
    word: &str,
    edit_distance: usize,
    max_edit_distance: usize,
    delete_words: &mut HashSet<String>,
) {
    let edit_distance = edit_distance + 1;
    if !word.is_empty() {
        for (i, _) in word.char_indices() {
            let delete = word
                .chars()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(_, c)| c)
                .collect::<String>();

            if !delete_words.contains(&delete) {
                delete_words.insert(delete.clone());

                if edit_distance < max_edit_distance {
                    deletes_rec(&delete, edit_distance, max_edit_distance, delete_words);
                }
            }
        }
    }
}

fn deletes(term: &str, edit_strategy: &impl EditStrategy) -> Vec<String> {
    let mut res = HashSet::new();
    deletes_rec(term, 0, edit_strategy.dist().distance(), &mut res);

    res.into_iter().collect()
}

#[derive(Debug)]
struct Suggestion {
    term: String,
    distance: usize,
    score: f64,
}

impl Ord for Suggestion {
    fn cmp(&self, other: &Suggestion) -> Ordering {
        let distance_cmp = self.distance.cmp(&other.distance);
        if distance_cmp == Ordering::Equal {
            return self
                .score
                .partial_cmp(&other.score)
                .unwrap_or(Ordering::Equal)
                .reverse();
        }
        distance_cmp
    }
}

impl PartialOrd for Suggestion {
    fn partial_cmp(&self, other: &Suggestion) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Suggestion {
    fn eq(&self, other: &Suggestion) -> bool {
        self.distance == other.distance && self.score == other.score
    }
}
impl Eq for Suggestion {}

impl<T: EditStrategy> SpellChecker<T> {
    pub fn new(dict: Dictionary, edit_strategy: T) -> Self {
        let mut deletes_map = BTreeMap::new();
        for (term, id) in dict.terms() {
            let term = term.to_ascii_lowercase();
            for delete in deletes(&term, &edit_strategy) {
                deletes_map.entry(delete).or_insert_with(Vec::new).push(*id);
            }
        }

        SpellChecker {
            dict,
            edit_strategy,
            deletes: deletes_map,
        }
    }

    fn suggestions(&self, before: &[&str], term: &str, after: &[&str]) -> Vec<Suggestion> {
        let mut suggestions = Vec::new();
        let mut suggestion_terms = HashSet::new();

        let mut candidates = VecDeque::new();
        candidates.push_back(term.to_ascii_lowercase());
        for delete in deletes(term, &self.edit_strategy) {
            candidates.push_back(delete.to_ascii_lowercase());
        }
        let max_dist = self.edit_strategy.distance_for_string(term);

        while let Some(candidate) = candidates.pop_front() {
            if suggestion_terms.contains(&candidate) {
                continue;
            }

            let distance = LevenshteinDistance::compare(term, &candidate);
            if distance <= max_dist {
                if let Some(score) = self.dict.score(before, &candidate, after) {
                    suggestions.push(Suggestion {
                        term: candidate.clone(),
                        distance,
                        score,
                    });
                    suggestion_terms.insert(candidate.clone());
                }

                if let Some(deletes) = self.deletes.get(&candidate) {
                    for delete in deletes {
                        candidates.push_back(self.dict.term(delete).unwrap().to_string());
                    }
                }
            }
        }

        suggestions.sort();
        suggestions
    }

    pub fn correct(&self, before: &[&str], term: &str, after: &[&str]) -> Option<String> {
        if self.dict.term_id(term).is_some() {
            return None;
        }

        if term.chars().any(|c| !c.is_ascii_alphabetic()) {
            return None;
        }

        self.suggestions(before, term, after)
            .into_iter()
            .map(|suggestion| suggestion.term)
            .next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spell::{
        dictionary::{self, MaxEdit},
        LogarithmicEdit,
    };

    #[test]
    fn simple_corrections() {
        let dict = dictionary::build_from_str("this is a test test pest");

        let spell_checker = SpellChecker::new(dict, MaxEdit::new(1));

        assert_eq!(
            spell_checker.correct(&[], "tst", &[]),
            Some("test".to_string())
        );
        assert_eq!(
            spell_checker.correct(&[], "ths", &[]),
            Some("this".to_string())
        );
        assert_eq!(
            spell_checker.correct(&[], "thes", &[]),
            Some("this".to_string())
        );
        assert_eq!(spell_checker.correct(&[], "is", &[]), None);
        assert_eq!(
            spell_checker.correct(&[], "dest", &[]),
            Some("test".to_string())
        );
    }

    #[test]
    fn correct_uncontained_word() {
        let dict = dictionary::build_from_str("this is a test test");

        let spell_checker = SpellChecker::new(dict, MaxEdit::new(1));

        assert_eq!(spell_checker.correct(&[], "what", &[]), None);
    }

    #[test]
    fn prioritise_low_distance_words() {
        let dict = dictionary::build_from_str("this is a test test contest contest");

        let spell_checker = SpellChecker::new(dict, MaxEdit::new(4));

        assert_eq!(
            spell_checker.correct(&[], "dest", &[]),
            Some("test".to_string())
        );
    }

    #[test]
    fn correct_sorting_multiple_hits() {
        let dict = dictionary::build_from_str("the the the he");

        let spell_checker = SpellChecker::new(dict, LogarithmicEdit::new(4));

        assert_eq!(
            spell_checker.correct(&[], "fhe", &[]),
            Some("the".to_string())
        );
    }

    #[test]
    fn dont_correct_non_alphabet() {
        let dict = dictionary::build_from_str("this is a test c");

        let spell_checker = SpellChecker::new(dict, MaxEdit::new(1));

        assert_eq!(spell_checker.correct(&[], "c++", &[]), None);
        assert_eq!(spell_checker.correct(&[], "c#", &[]), None);
    }

    #[test]
    fn context_correction() {
        let dict = dictionary::build_from_str("abraham lincoln was the 16th president of the united states. A wrong way to spell hist last name would be linculn linculn");

        let spell_checker = SpellChecker::new(dict, MaxEdit::new(1));

        assert_eq!(
            spell_checker.correct(&[], "lincln", &[]),
            Some("linculn".to_string())
        );
        assert_eq!(
            spell_checker.correct(&["abraham"], "lincln", &[]),
            Some("lincoln".to_string())
        );
    }
}
