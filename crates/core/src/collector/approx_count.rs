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

use std::ops::Add;

use itertools::Itertools;
use tantivy::{
    collector::{Collector, SegmentCollector},
    tokenizer::Tokenizer,
};

use crate::schema::text_field::{self, TextField};

#[derive(
    Debug,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    Clone,
    Copy,
    PartialEq,
    Eq,
    utoipa::ToSchema,
)]
#[serde(rename_all = "camelCase", tag = "_type", content = "value")]
pub enum Count {
    Exact(u64),
    Approximate(u64),
}

impl Count {
    pub fn is_exact(&self) -> bool {
        match self {
            Count::Exact(_) => true,
            Count::Approximate(_) => false,
        }
    }

    pub fn is_approximate(&self) -> bool {
        match self {
            Count::Exact(_) => false,
            Count::Approximate(_) => true,
        }
    }

    pub fn compose(&self, other: &Self) -> Self {
        match (self, other) {
            (Count::Exact(a), Count::Exact(b)) => Count::Exact(a + b),
            (Count::Exact(a), Count::Approximate(b))
            | (Count::Approximate(a), Count::Exact(b))
            | (Count::Approximate(a), Count::Approximate(b)) => Count::Approximate(a + b),
        }
    }
}

impl Add for Count {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        self.compose(&other)
    }
}

pub struct ApproxCount {
    max_docs_per_segment: u64,
    terms: Vec<String>,
}

impl ApproxCount {
    pub fn new(max_docs_per_segment: u64, terms: Vec<String>) -> Self {
        Self {
            max_docs_per_segment,
            terms: terms
                .into_iter()
                .map(|term| term.to_lowercase())
                .sorted()
                .dedup()
                .collect(),
        }
    }
}

impl Collector for ApproxCount {
    type Fruit = Count;
    type Child = SegmentApproxCount;

    fn for_segment(
        &self,
        _: tantivy::SegmentOrdinal,
        segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let field = text_field::AllBody.tantivy_field(segment.schema()).unwrap();
        let mut tokenizer = text_field::AllBody.query_tokenizer(None);

        let inverted_index = segment.inverted_index(field)?;
        let num_docs = segment.max_doc() as u64;
        let mut term_freqs = Vec::with_capacity(self.terms.len());

        for term in &self.terms {
            let mut term_freq = 0;
            let mut stream = tokenizer.token_stream(term.as_str());

            while let Some(term) = stream.next() {
                term_freq += inverted_index
                    .doc_freq(&tantivy::Term::from_field_text(field, term.text.as_str()))?;
            }

            term_freqs.push(term_freq as u64);
        }

        let approx_count = approx_results_assuming_term_independence(&term_freqs, num_docs);

        Ok(SegmentApproxCount {
            max_docs_per_segment: self.max_docs_per_segment,
            approx_count,
            exact_count: 0,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as tantivy::collector::SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        Ok(segment_fruits
            .into_iter()
            .fold(Count::Exact(0), |acc, fruit| acc.compose(&fruit)))
    }
}

pub struct SegmentApproxCount {
    max_docs_per_segment: u64,
    approx_count: u64,
    exact_count: u64,
}

impl SegmentCollector for SegmentApproxCount {
    type Fruit = Count;

    fn collect(&mut self, _doc: u32, _score: f32) {
        self.exact_count += 1;
    }

    fn harvest(self) -> Self::Fruit {
        if self.exact_count >= self.max_docs_per_segment {
            if self.exact_count > self.approx_count {
                Count::Approximate(self.exact_count)
            } else {
                Count::Approximate(self.approx_count)
            }
        } else {
            Count::Exact(self.exact_count)
        }
    }
}

/// Approximate the number of results for a query assuming each term is independent.
/// The idea is to estiate the probability that a document contains all terms in the query
/// as the product of the probabilities that it contains each term.
///
/// P(A and B) = P(A) * P(B) if A and B are independent.
///
/// returns 0 if term_freqs is empty or num_docs is 0.
fn approx_results_assuming_term_independence(term_freqs: &[u64], num_docs: u64) -> u64 {
    if term_freqs.is_empty() || num_docs == 0 {
        return 0;
    }

    let mut res = None;

    for term_freq in term_freqs {
        let prob = num_rational::BigRational::new((*term_freq).into(), num_docs.into());

        res = Some(res.map_or(prob.clone(), move |res| res * prob));
    }

    let approx_results = res.unwrap() * num_rational::BigRational::new(num_docs.into(), 1.into());

    let (sign, digits) = approx_results.to_integer().to_u64_digits();

    if matches!(sign, num_bigint::Sign::Minus | num_bigint::Sign::NoSign) || digits.is_empty() {
        0
    } else {
        digits[0]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_approx_results_assuming_term_independence() {
        let term_freqs = vec![40, 25];
        let num_docs = 100;

        let approx_results = approx_results_assuming_term_independence(&term_freqs, num_docs);

        assert_eq!(approx_results, 10);
    }
}
