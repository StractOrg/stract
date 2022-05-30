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
use crate::search_index::FastWebpage;
use std::sync::Arc;
use tantivy::collector::{ScoreSegmentTweaker, ScoreTweaker};
use tantivy::fastfield::{BytesFastFieldReader, DynamicFastFieldReader, FastFieldReader};
use tantivy::{
    collector::{Collector, TopDocs},
    DocId, Score, SegmentReader,
};

struct InitialScoreTweaker {
    query: Arc<Query>,
}

impl InitialScoreTweaker {
    pub fn new(query: Query) -> Self {
        Self {
            query: Arc::new(query),
        }
    }
}

struct InitialSegmentScoreTweaker {
    query: Arc<Query>,
    centrality_reader: DynamicFastFieldReader<f64>,
    domain_reader: BytesFastFieldReader,
}

impl InitialSegmentScoreTweaker {
    fn navigational_score(&self, terms: Vec<String>, doc: DocId) -> f64 {
        let bytes = self.domain_reader.get_bytes(doc);
        let domain = bincode::deserialize(bytes).expect("Failed to deserialize domain");

        if is_navigational(terms, domain) {
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

        Ok(InitialSegmentScoreTweaker {
            query: self.query.clone(),
            centrality_reader,
            domain_reader,
        })
    }
}

impl ScoreSegmentTweaker<f64> for InitialSegmentScoreTweaker {
    fn score(&mut self, doc: DocId, score: Score) -> f64 {
        let centrality = self.centrality_reader.get(doc);
        let navigational = self.navigational_score(self.query.terms.clone(), doc);
        score as f64 + 1000.0 * centrality + 100.0 * navigational
    }
}

fn jaccard_sim(mut terms: Vec<String>, domain_parts: &[&str]) -> f64 {
    debug_assert_eq!(domain_parts.len(), 2);

    terms.sort();
    terms.dedup();

    let intersection_size: f64 = terms
        .iter()
        .map(|term| {
            if domain_parts.contains(&term.as_str()) {
                1.0
            } else {
                0.0
            }
        })
        .sum();

    intersection_size / (terms.len() as f64 + domain_parts.len() as f64 - intersection_size)
}

fn is_navigational(mut terms: Vec<String>, domain: &str) -> bool {
    let (name, remaining) = domain.split_once(".").unwrap();
    jaccard_sim(terms, &[name, remaining]) >= 0.5
}

pub(crate) fn initial_collector(
    query: Query,
) -> impl Collector<Fruit = Vec<(f64, tantivy::DocAddress)>> {
    let score_tweaker = InitialScoreTweaker::new(query);
    TopDocs::with_limit(20).tweak_score(score_tweaker)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        query::Query,
        search_index::Index,
        webpage::{Link, Webpage},
    };

    #[test]
    fn test_jaccard_sim() {
        assert_eq!(
            jaccard_sim(vec!["dr".to_string(), "dk".to_string()], &["dr", "dk"]),
            1.0
        );

        assert_eq!(
            jaccard_sim(
                vec![
                    "dr".to_string(),
                    "dk".to_string(),
                    "dr".to_string(),
                    "dr".to_string()
                ],
                &["dr", "dk"]
            ),
            1.0
        );
        assert_eq!(jaccard_sim(vec!["dr".to_string()], &["dr", "dk"]), 0.5);
    }

    #[test]
    fn harmonic_ranking() {
        let query = Query::parse("great site").expect("Failed to parse query");

        for _ in 0..10 {
            let mut index = Index::temporary().expect("Unable to open index");

            index
                .insert(Webpage::new(
                    r#"
                        <html>
                            <head>
                                <title>Website A</title>
                            </head>
                            <a href="https://www.b.com">B site is great</a>
                        </html>
                    "#,
                    "https://www.a.com",
                    vec![],
                    0.0,
                ))
                .expect("failed to parse webpage");
            index
                .insert(Webpage::new(
                    r#"
                        <html>
                            <head>
                                <title>Website B</title>
                            </head>
                        </html>
                    "#,
                    "https://www.b.com",
                    vec![Link {
                        source: "https://www.a.com".to_string(),
                        destination: "https://www.b.com".to_string(),
                        text: "B site is great".to_string(),
                    }],
                    5.0,
                ))
                .expect("failed to parse webpage");

            index.commit().expect("failed to commit index");
            let result = index.search(&query).expect("Search failed");
            assert_eq!(result.documents.len(), 2);
            assert_eq!(result.documents[0].url, "https://www.b.com");
            assert_eq!(result.documents[1].url, "https://www.a.com");
        }
    }

    #[test]
    fn navigational() {
        assert!(is_navigational(vec!["dr".to_string()], "dr.dk"));
        assert!(is_navigational(
            vec!["dr".to_string(), "dk".to_string()],
            "dr.dk"
        ));
    }

    #[test]
    fn navigational_search() {
        let query = Query::parse("dr dk").expect("Failed to parse query");

        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(Webpage::new(
                r#"
                    <html>
                        <head>
                            <title>Website A</title>
                        </head>
                    </html>
                "#,
                "https://www.dr.dk",
                vec![],
                0.0,
            ))
            .expect("failed to parse webpage");
        index
            .insert(Webpage::new(
                r#"
                    <html>
                        <head>
                            <title>Website B</title>
                            dr dk dr dk dr dk dr dk
                        </head>
                    </html>
                "#,
                "https://www.b.com",
                vec![],
                0.0003,
            ))
            .expect("failed to parse webpage");

        index.commit().expect("failed to commit index");
        let result = index.search(&query).expect("Search failed");
        assert_eq!(result.documents.len(), 2);
        assert_eq!(result.documents[0].url, "https://www.dr.dk");
        assert_eq!(result.documents[1].url, "https://www.b.com");
    }
}
