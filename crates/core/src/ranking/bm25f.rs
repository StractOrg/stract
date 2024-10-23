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

use std::collections::HashMap;

use itertools::Itertools;
use tantivy::{Score, Searcher, Term};

use crate::schema::text_field::{AllBody, TextField};

use super::bm25::{compute_tf_cache, idf, Bm25Constants};

#[derive(Default)]
pub struct WeightCache {
    cache: HashMap<String, u64>,
}

impl WeightCache {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    fn compute(term: &str, searcher: &Searcher) -> u64 {
        // use 'AllBody' field as an approximation of the term doc freq across all fields
        let field = searcher.schema().get_field(AllBody.name()).unwrap();
        let term = Term::from_field_text(field, term);
        searcher.doc_freq(&term).unwrap_or_default()
    }

    pub fn get(&mut self, term: &str, searcher: &Searcher) -> u64 {
        *self
            .cache
            .entry(term.to_string())
            .or_insert_with(|| Self::compute(term, searcher))
    }
}

pub struct PreparedTerm {
    term: Term,
    doc_freq: u64,
}

impl PreparedTerm {
    pub fn new(term: Term, doc_freq: u64) -> Self {
        Self { term, doc_freq }
    }
}

/// A BM25F weight that uses the same IDF weight for all fields.
/// The idea is that the term 'the' might not appear very frequently e.g. in the title field,
/// but it should still be considered a common term.
///
/// Note that this is a simplification of the BM25F algorithm
/// and not a direct implementation of the paper.
/// The main difference lies in the fact that the paper sums the TF component
/// across all fields for each term before multiplying it with the IDF component.
/// The way we iterate over the fields during ranking makes this approach infeasible
/// and we simply take the main idea of re-using the same IDF weight for all fields instead.
///
/// Papers:
/// https://trec.nist.gov/pubs/trec13/papers/microsoft-cambridge.web.hard.pdf
/// http://www.staff.city.ac.uk/~sbrp622/papers/foundations_bm25_review.pdf
#[derive(Clone)]
pub struct MultiBm25FWeight {
    weights: Vec<Bm25FWeight>,
}

impl MultiBm25FWeight {
    pub fn for_terms(
        searcher: &Searcher,
        terms: &[PreparedTerm],
        constants: Bm25Constants,
    ) -> Self {
        if terms.is_empty() {
            return Self {
                weights: Vec::new(),
            };
        }

        let field = terms[0].term.field();
        for term in terms.iter().skip(1) {
            assert_eq!(
                term.term.field(),
                field,
                "All terms must belong to the same field."
            );
        }

        let mut total_num_tokens = 0u64;
        let mut total_num_docs = 0u64;

        for segment_reader in searcher.segment_readers() {
            let inverted_index = segment_reader.inverted_index(field).unwrap();
            total_num_tokens += inverted_index.total_num_tokens();
            total_num_docs += u64::from(segment_reader.max_doc());
        }

        let average_fieldnorm = total_num_tokens as f32 / total_num_docs as f32;

        let mut weights = Vec::new();

        for term in terms {
            weights.push(Bm25FWeight::for_one_term(
                term.doc_freq,
                total_num_docs,
                average_fieldnorm,
                constants,
            ));
        }

        Self { weights }
    }

    #[inline]
    pub fn score(&self, coefficient: Score, stats: impl Iterator<Item = (u8, u32)>) -> Score {
        stats
            .zip_eq(self.weights.iter())
            .map(|((fieldnorm_id, term_freq), weight)| {
                weight.score(coefficient, fieldnorm_id, term_freq)
            })
            .sum()
    }
}

#[derive(Clone)]
pub struct Bm25FWeight {
    weight: Score,
    cache: [Score; 256],
    constants: Bm25Constants,
}

impl Bm25FWeight {
    pub fn for_one_term(
        term_doc_freq: u64,
        total_num_docs: u64,
        avg_fieldnorm: Score,
        constants: Bm25Constants,
    ) -> Bm25FWeight {
        let idf = idf(term_doc_freq, total_num_docs);
        Bm25FWeight::new(idf, avg_fieldnorm, constants)
    }

    pub fn new(weight: Score, average_fieldnorm: Score, constants: Bm25Constants) -> Bm25FWeight {
        Bm25FWeight {
            weight,
            cache: compute_tf_cache(average_fieldnorm, constants),
            constants,
        }
    }

    #[inline]
    pub fn score(&self, coefficient: Score, fieldnorm_id: u8, term_freq: u32) -> Score {
        self.weight * (self.tf_factor(coefficient, fieldnorm_id, term_freq))
    }

    #[inline]
    pub fn tf_factor(&self, coefficient: Score, fieldnorm_id: u8, term_freq: u32) -> Score {
        if term_freq == 0 {
            return 0.0;
        }

        let term_freq = term_freq as Score * coefficient;
        let norm = self.cache[fieldnorm_id as usize];
        (term_freq * (self.constants.k1 + 1.0)) / (term_freq + norm)
    }
}
