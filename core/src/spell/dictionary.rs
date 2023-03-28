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
use crate::spell::distance::LevenshteinDistance;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;
use std::{cmp, io};
use thiserror::Error;

pub trait EditStrategy: Send + Sync {
    fn distance_for_string(&self, string: &str) -> usize;
    fn dist(&self) -> LevenshteinDistance;
}

pub struct LogarithmicEdit {
    max_edit_distance: usize,
}

impl LogarithmicEdit {
    pub fn new(max_edit_distance: usize) -> Self {
        Self { max_edit_distance }
    }
}

impl EditStrategy for LogarithmicEdit {
    fn distance_for_string(&self, string: &str) -> usize {
        let log_value: usize = (string.len() as f32).log2() as usize;
        cmp::max(1, cmp::min(log_value, self.max_edit_distance))
    }

    fn dist(&self) -> LevenshteinDistance {
        LevenshteinDistance::new(self.max_edit_distance)
    }
}

#[cfg(test)]
pub struct MaxEdit {
    max_edit_distance: usize,
}

#[cfg(test)]
impl MaxEdit {
    pub fn new(max_edit_distance: usize) -> Self {
        Self { max_edit_distance }
    }
}

#[cfg(test)]
impl EditStrategy for MaxEdit {
    fn distance_for_string(&self, _: &str) -> usize {
        self.max_edit_distance
    }

    fn dist(&self) -> LevenshteinDistance {
        LevenshteinDistance::new(self.max_edit_distance)
    }
}

pub struct DictionaryResult {
    pub prob: f64,
    pub correction: String,
}

impl Hash for DictionaryResult {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.correction.hash(state);
    }
}

impl PartialEq for DictionaryResult {
    fn eq(&self, other: &Self) -> bool {
        self.correction == other.correction
    }
}

impl Eq for DictionaryResult {}

#[derive(Error, Debug)]
pub enum DictionaryError {
    #[error("Underlying error from FST")]
    Fst(#[from] fst::Error),

    #[error("An IO error has occured")]
    Io(#[from] io::Error),
}

pub type Result<T> = std::result::Result<T, DictionaryError>;

pub type TermId = u64;

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Monogram(TermId);

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Bigram(TermId, TermId);

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Trigram(TermId, TermId, TermId);

type HeavyMonogram = String;
type HeavyBigram = (String, String);
type HeavyTrigram = (String, String, String);

pub struct DictionaryBuilder {
    monograms: BinaryHeap<Reverse<(u64, HeavyMonogram)>>,
    bigrams: BinaryHeap<Reverse<(u64, HeavyBigram)>>,
    trigrams: BinaryHeap<Reverse<(u64, HeavyTrigram)>>,
    top_n: usize,
}

impl DictionaryBuilder {
    pub fn new(top_n: usize) -> Self {
        Self {
            monograms: BinaryHeap::new(),
            bigrams: BinaryHeap::new(),
            trigrams: BinaryHeap::new(),
            top_n,
        }
    }

    pub fn add_monogram(&mut self, term: HeavyMonogram, freq: u64) {
        if self.monograms.len() < self.top_n {
            self.monograms.push(Reverse((freq, term)));
        } else if self.monograms.peek().unwrap().0 .0 < freq {
            let mut worst = self.monograms.peek_mut().unwrap();
            *worst = Reverse((freq, term));
        }
    }

    pub fn add_bigram(&mut self, term: HeavyBigram, freq: u64) {
        if self.bigrams.len() < self.top_n {
            self.bigrams.push(Reverse((freq, term)));
        } else if self.bigrams.peek().unwrap().0 .0 < freq {
            let mut worst = self.bigrams.peek_mut().unwrap();
            *worst = Reverse((freq, term));
        }
    }

    pub fn add_trigram(&mut self, term: HeavyTrigram, freq: u64) {
        if self.trigrams.len() < self.top_n {
            self.trigrams.push(Reverse((freq, term)));
        } else if self.trigrams.peek().unwrap().0 .0 < freq {
            let mut worst = self.trigrams.peek_mut().unwrap();
            *worst = Reverse((freq, term));
        }
    }

    pub fn build(self) -> Dictionary {
        Dictionary::build(
            self.monograms
                .into_iter()
                .map(|Reverse((freq, term))| (term, freq)),
            self.bigrams
                .into_iter()
                .map(|Reverse((freq, term))| (term, freq)),
            self.trigrams
                .into_iter()
                .map(|Reverse((freq, term))| (term, freq)),
        )
    }
}

#[derive(Clone)]
pub struct Dictionary {
    inner: Arc<InnerDictionary>,
}

impl Dictionary {
    pub fn build(
        monograms: impl Iterator<Item = (String, u64)>,
        bigrams: impl Iterator<Item = ((String, String), u64)>,
        trigrams: impl Iterator<Item = ((String, String, String), u64)>,
    ) -> Self {
        let inner = InnerDictionary::build(monograms, bigrams, trigrams);
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl Deref for Dictionary {
    type Target = InnerDictionary;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Dictionary that contains term frequency information
pub struct InnerDictionary {
    total_freq: u64,
    monograms: BTreeMap<Monogram, u64>,
    bigrams: BTreeMap<Bigram, u64>,
    trigrams: BTreeMap<Trigram, u64>,
    terms: BTreeMap<String, TermId>,
    rev_terms: BTreeMap<TermId, String>,
}

impl InnerDictionary {
    fn build(
        monograms: impl Iterator<Item = (String, u64)>,
        bigrams: impl Iterator<Item = ((String, String), u64)>,
        trigrams: impl Iterator<Item = ((String, String, String), u64)>,
    ) -> Self {
        let mut dict = InnerDictionary {
            total_freq: 0,
            monograms: BTreeMap::new(),
            bigrams: BTreeMap::new(),
            trigrams: BTreeMap::new(),
            terms: BTreeMap::new(),
            rev_terms: BTreeMap::new(),
        };

        dict.load_monograms(monograms);
        dict.load_bigrams(bigrams);
        dict.load_trigrams(trigrams);

        dict
    }

    fn load_monograms(&mut self, monograms: impl Iterator<Item = (String, u64)>) {
        for (term, freq) in monograms {
            let l = self.terms.len();
            let id = *self.terms.entry(term.clone()).or_insert_with(|| l as u64);
            self.rev_terms.insert(id, term);
            *self.monograms.entry(Monogram(id)).or_insert(0) += freq;
            self.total_freq += freq;
        }
    }

    fn load_bigrams(&mut self, bigrams: impl Iterator<Item = ((String, String), u64)>) {
        for ((a, b), freq) in bigrams {
            let l = self.terms.len();
            let id_a = *self.terms.entry(a.clone()).or_insert_with(|| l as u64);
            if self.rev_terms.get(&id_a).is_none() {
                self.rev_terms.insert(id_a, a);
            }

            let l = self.terms.len();
            let id_b = *self.terms.entry(b.clone()).or_insert_with(|| l as u64);
            if self.rev_terms.get(&id_b).is_none() {
                self.rev_terms.insert(id_b, b);
            }

            *self.bigrams.entry(Bigram(id_a, id_b)).or_insert(0) += freq;
        }
    }

    fn load_trigrams(&mut self, trigrams: impl Iterator<Item = ((String, String, String), u64)>) {
        for ((a, b, c), freq) in trigrams {
            let l = self.terms.len();
            let id_a = *self.terms.entry(a.clone()).or_insert_with(|| l as u64);
            if self.rev_terms.get(&id_a).is_none() {
                self.rev_terms.insert(id_a, a);
            }

            let l = self.terms.len();
            let id_b = *self.terms.entry(b.clone()).or_insert_with(|| l as u64);
            if self.rev_terms.get(&id_b).is_none() {
                self.rev_terms.insert(id_b, b);
            }

            let l = self.terms.len();
            let id_c = *self.terms.entry(c.clone()).or_insert_with(|| l as u64);
            if self.rev_terms.get(&id_c).is_none() {
                self.rev_terms.insert(id_c, c);
            }

            *self.trigrams.entry(Trigram(id_a, id_b, id_c)).or_insert(0) += freq;
        }
    }

    #[inline]
    pub fn score(&self, before: &[&str], term: &str, after: &[&str]) -> Option<f64> {
        self.terms.get(term).map(|id| {
            let mut d = 1;
            let mut total_score = self.monograms[&Monogram(*id)] as f64 / self.total_freq as f64;

            if !before.is_empty() {
                if let Some(id_before) = self.terms.get(before[before.len() - 1]) {
                    if let Some(bigram_freq) = self.bigrams.get(&Bigram(*id_before, *id)) {
                        total_score +=
                            *bigram_freq as f64 / self.monograms[&Monogram(*id_before)] as f64;
                        d += 1;
                    }

                    if before.len() > 1 {
                        if let Some(id_before_before) = self.terms.get(before[before.len() - 2]) {
                            if let Some(trigram_freq) =
                                self.trigrams
                                    .get(&Trigram(*id_before_before, *id_before, *id))
                            {
                                if let Some(c_freq) =
                                    self.bigrams.get(&Bigram(*id_before_before, *id_before))
                                {
                                    total_score += *trigram_freq as f64 / *c_freq as f64;
                                    d += 1;
                                }
                            }
                        }
                    }
                }
            }

            if !after.is_empty() {
                if let Some(id_after) = self.terms.get(after[0]) {
                    if let Some(bigram_freq) = self.bigrams.get(&Bigram(*id, *id_after)) {
                        total_score += *bigram_freq as f64 / self.monograms[&Monogram(*id)] as f64;
                        d += 1;
                    }

                    if after.len() > 1 {
                        if let Some(id_after_after) = self.terms.get(after[1]) {
                            if let Some(trigram_freq) =
                                self.trigrams.get(&Trigram(*id, *id_after, *id_after_after))
                            {
                                if let Some(c_freq) = self.bigrams.get(&Bigram(*id, *id_after)) {
                                    total_score += *trigram_freq as f64 / *c_freq as f64;
                                    d += 1;
                                }
                            }
                        }
                    }
                }
            }

            if !after.is_empty() && !before.is_empty() {
                if let Some(id_after) = self.terms.get(after[after.len() - 1]) {
                    if let Some(id_before) = self.terms.get(before[before.len() - 1]) {
                        if let Some(trigram_freq) =
                            self.trigrams.get(&Trigram(*id_before, *id, *id_after))
                        {
                            if let Some(c_freq) = self.bigrams.get(&Bigram(*id_before, *id)) {
                                total_score += *trigram_freq as f64 / *c_freq as f64;
                                d += 1;
                            }
                        }
                    }
                }
            }

            total_score / d as f64
        })
    }

    pub fn terms(&self) -> impl Iterator<Item = (&String, &TermId)> {
        self.terms.iter()
    }

    pub fn term_id(&self, term: &str) -> Option<&TermId> {
        self.terms.get(term)
    }

    pub fn term(&self, term_id: &u64) -> Option<&str> {
        self.rev_terms.get(term_id).map(|s| s.as_str())
    }
}

#[cfg(test)]
pub fn build_from_str(text: &str) -> Dictionary {
    use itertools::Itertools;
    use std::collections::HashMap;
    let mut mono_freqs = HashMap::new();
    let mut bi_freqs = HashMap::new();
    let mut tri_freqs = HashMap::new();

    for word in text.split_ascii_whitespace() {
        *mono_freqs.entry(word.to_string()).or_insert(0) += 1;
    }

    for (a, b) in text.split_ascii_whitespace().tuple_windows() {
        *bi_freqs.entry((a.to_string(), b.to_string())).or_insert(0) += 1;
    }

    for (a, b, c) in text.split_ascii_whitespace().tuple_windows() {
        *tri_freqs
            .entry((a.to_string(), b.to_string(), c.to_string()))
            .or_insert(0) += 1;
    }

    Dictionary::build(
        text.split_ascii_whitespace()
            .map(|word| (word.to_string(), mono_freqs[word])),
        text.split_ascii_whitespace().tuple_windows().map(|(a, b)| {
            (
                (a.to_string(), b.to_string()),
                bi_freqs[&(a.to_string(), b.to_string())],
            )
        }),
        text.split_ascii_whitespace()
            .tuple_windows()
            .map(|(a, b, c)| {
                (
                    (a.to_string(), b.to_string(), c.to_string()),
                    tri_freqs[&(a.to_string(), b.to_string(), c.to_string())],
                )
            }),
    )
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_probability() {
        let dict = build_from_str("this is a test test");

        assert_eq!(
            dict.score(&[], "this", &[]).unwrap(),
            dict.score(&[], "is", &[]).unwrap()
        );
        assert!(dict.score(&[], "test", &[]).unwrap() > dict.score(&[], "this", &[]).unwrap());
    }
}
