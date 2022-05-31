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

use crate::query::Query;
use crate::schema::Field;
use std::sync::Arc;
use tantivy::collector::{ScoreSegmentTweaker, ScoreTweaker};
use tantivy::fastfield::{BytesFastFieldReader, DynamicFastFieldReader, FastFieldReader};
use tantivy::{DocId, Score, SegmentReader};

pub(crate) struct InitialScoreTweaker {
    query: Arc<Query>,
}

impl InitialScoreTweaker {
    pub fn new(query: Arc<Query>) -> Self {
        Self { query }
    }
}

pub(crate) struct InitialSegmentScoreTweaker {
    centrality_reader: DynamicFastFieldReader<f64>,
    domain_reader: BytesFastFieldReader,
    sorted_dedupped_terms: Vec<String>,
}

impl InitialSegmentScoreTweaker {
    fn navigational_score(&self, sorted_dedupped_terms: &[String], doc: DocId) -> f64 {
        let bytes = self.domain_reader.get_bytes(doc);
        let domain = bincode::deserialize(bytes).expect("Failed to deserialize domain");

        if is_navigational(sorted_dedupped_terms, domain) {
            1.0
        } else {
            0.0
        }
    }
}

impl ScoreTweaker<f64> for InitialScoreTweaker {
    type Child = InitialSegmentScoreTweaker;

    fn segment_tweaker(&self, segment_reader: &SegmentReader) -> tantivy::Result<Self::Child> {
        let centrality_field = segment_reader
            .schema()
            .get_field(Field::Centrality.as_str())
            .expect("Faild to load centrality field");
        let centrality_reader = segment_reader
            .fast_fields()
            .f64(centrality_field)
            .expect("Failed to get centrality fast-field reader");

        let domain_field = segment_reader
            .schema()
            .get_field(Field::Domain.as_str())
            .expect("Faild to load domain field");
        let domain_reader = segment_reader
            .fast_fields()
            .bytes(domain_field)
            .expect("Failed to get domain fast-field reader");

        let mut sorted_dedupped_terms = self.query.terms.clone();

        sorted_dedupped_terms.sort();
        sorted_dedupped_terms.dedup();

        Ok(InitialSegmentScoreTweaker {
            centrality_reader,
            domain_reader,
            sorted_dedupped_terms,
        })
    }
}

impl ScoreSegmentTweaker<f64> for InitialSegmentScoreTweaker {
    fn score(&mut self, doc: DocId, score: Score) -> f64 {
        let centrality = self.centrality_reader.get(doc);
        let navigational = self.navigational_score(&self.sorted_dedupped_terms, doc);
        score as f64 + 1000.0 * centrality + 100.0 * navigational
    }
}

fn jaccard_sim(sorted_dedupped_terms: &[String], domain_parts: &[&str]) -> f64 {
    debug_assert_eq!(domain_parts.len(), 2);

    let intersection_size: f64 = sorted_dedupped_terms
        .iter()
        .map(|term| {
            if domain_parts.contains(&term.as_str()) {
                1.0
            } else {
                0.0
            }
        })
        .sum();

    intersection_size
        / (sorted_dedupped_terms.len() as f64 + domain_parts.len() as f64 - intersection_size)
}

fn is_navigational(sorted_dedupped_terms: &[String], domain: &str) -> bool {
    let (name, remaining) = domain.split_once('.').unwrap();
    jaccard_sim(sorted_dedupped_terms, &[name, remaining]) >= 0.5
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        index::Index,
        query::Query,
        webpage::{Link, Webpage},
    };

    #[test]
    fn test_jaccard_sim() {
        assert_eq!(
            jaccard_sim(&["dr".to_string(), "dk".to_string()], &["dr", "dk"]),
            1.0
        );

        assert_eq!(jaccard_sim(&["dr".to_string()], &["dr", "dk"]), 0.5);
    }

    #[test]
    fn navigational() {
        assert!(is_navigational(&["dr".to_string()], "dr.dk"));
        assert!(is_navigational(
            &["dk".to_string(), "dr".to_string()],
            "dr.dk"
        ));
    }
}
