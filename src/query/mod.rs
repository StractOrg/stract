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

use crate::{ranking::signal_aggregator::SignalAggregator, Result};
use std::{collections::HashMap, sync::Arc};
use tantivy::{
    query::{AllQuery, BooleanQuery, Occur},
    schema::Schema,
    tokenizer::TokenizerManager,
};

pub mod parser;
use parser::Term;

const MAX_SIMILAR_TERMS: usize = 10;

#[derive(Clone, Debug)]
pub struct Query {
    #[allow(clippy::vec_box)]
    terms: Vec<Box<Term>>,
    tantivy_query: Box<BooleanQuery>,
}

impl Query {
    pub fn parse(
        query: &str,
        schema: Arc<Schema>,
        tokenizer_manager: &TokenizerManager,
        aggregator: &SignalAggregator,
    ) -> Result<Query> {
        let parsed_terms = parser::parse(query);

        let mut term_count = HashMap::new();
        let mut terms = Vec::new();

        for term in parsed_terms {
            let count = term_count.entry(term.clone()).or_insert(0);

            if *count < MAX_SIMILAR_TERMS {
                terms.push(term);
            }

            *count += 1;
        }

        let fields: Vec<(tantivy::schema::Field, &tantivy::schema::FieldEntry)> =
            schema.fields().collect();

        let field_boost = aggregator.field_boosts();

        let mut queries: Vec<(Occur, Box<dyn tantivy::query::Query + 'static>)> = terms
            .iter()
            .flat_map(|term| term.as_tantivy_query(&fields, tokenizer_manager, field_boost))
            .collect();

        queries.push((Occur::Should, Box::new(AllQuery)));

        let tantivy_query = Box::new(BooleanQuery::new(queries));

        Ok(Query {
            terms,
            tantivy_query,
        })
    }

    pub fn simple_terms(&self) -> Vec<String> {
        self.terms
            .iter()
            .filter_map(|term| {
                if let Term::Simple(term) = term.as_ref() {
                    Some(term.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn terms(&self) -> &[Box<Term>] {
        &self.terms
    }

    pub fn is_empty(&self) -> bool {
        self.terms.is_empty()
    }
}

impl tantivy::query::Query for Query {
    fn weight(
        &self,
        searcher: &tantivy::Searcher,
        scoring_enabled: bool,
    ) -> tantivy::Result<Box<dyn tantivy::query::Weight>> {
        self.tantivy_query.weight(searcher, scoring_enabled)
    }

    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a tantivy::Term, bool)) {
        self.tantivy_query.query_terms(visitor)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        inverted_index::InvertedIndex,
        ranking::Ranker,
        schema::create_schema,
        webpage::{region::RegionCount, Webpage},
    };

    use super::*;

    #[test]
    fn simple_parse() {
        let schema = Arc::new(create_schema());

        let query = Query::parse(
            "this is a simple query the the the the the the the the the the the the the",
            Arc::clone(&schema),
            &TokenizerManager::new(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query");

        assert_eq!(
            query.simple_terms(),
            vec![
                "this".to_string(),
                "is".to_string(),
                "a".to_string(),
                "simple".to_string(),
                "query".to_string(),
                "the".to_string(),
                "the".to_string(),
                "the".to_string(),
                "the".to_string(),
                "the".to_string(),
                "the".to_string(),
                "the".to_string(),
                "the".to_string(),
                "the".to_string(),
                "the".to_string(),
            ]
        );
    }

    #[test]
    fn parse_trailing_leading_whitespace() {
        let schema = Arc::new(create_schema());

        let query = Query::parse(
            "   this is a simple query   ",
            Arc::clone(&schema),
            &TokenizerManager::new(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query");

        assert_eq!(
            query.simple_terms(),
            vec![
                "this".to_string(),
                "is".to_string(),
                "a".to_string(),
                "simple".to_string(),
                "query".to_string(),
            ]
        );
    }

    #[test]
    fn parse_weird_characters() {
        let schema = Arc::new(create_schema());

        let terms = Query::parse(
            "123",
            Arc::clone(&schema),
            &TokenizerManager::new(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query")
        .simple_terms();
        assert_eq!(terms, vec!["123".to_string()]);

        let terms = Query::parse(
            "123 33",
            Arc::clone(&schema),
            &TokenizerManager::new(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query")
        .simple_terms();
        assert_eq!(terms, vec!["123".to_string(), "33".to_string()]);

        let terms = Query::parse(
            "term! term# $",
            Arc::clone(&schema),
            &TokenizerManager::new(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query")
        .simple_terms();
        assert_eq!(
            terms,
            vec!["term!".to_string(), "term#".to_string(), "$".to_string()]
        );
    }

    #[test]
    fn not_query() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");
        let query = Query::parse(
            "test -website",
            index.schema(),
            index.tokenizers(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(RegionCount::default(), SignalAggregator::default());

        index
            .insert(Webpage::new(
                r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                This is a test website
                            </body>
                        </html>
                    "#,
                "https://www.first.com",
                vec![],
                1.0,
                0,
            ))
            .expect("failed to parse webpage");
        index
            .insert(Webpage::new(
                r#"
                        <html>
                            <head>
                                <title>Test test</title>
                            </head>
                            <body>
                                This test page does not contain the forbidden word
                            </body>
                        </html>
                    "#,
                "https://www.second.com",
                vec![],
                1.0,
                0,
            ))
            .expect("failed to parse webpage");
        index.commit().expect("failed to commit index");

        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.num_docs, 1);
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.second.com");
    }

    #[test]
    fn site_query() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");

        index
            .insert(Webpage::new(
                r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                This is a test website
                            </body>
                        </html>
                    "#,
                "https://www.first.com",
                vec![],
                1.0,
                0,
            ))
            .expect("failed to parse webpage");
        index
            .insert(Webpage::new(
                r#"
                        <html>
                            <head>
                                <title>Test test</title>
                            </head>
                            <body>
                                This test page does not contain the forbidden word
                            </body>
                        </html>
                    "#,
                "https://www.second.com",
                vec![],
                1.0,
                0,
            ))
            .expect("failed to parse webpage");
        index.commit().expect("failed to commit index");

        let query = Query::parse(
            "test site:first.com",
            index.schema(),
            index.tokenizers(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(RegionCount::default(), SignalAggregator::default());
        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.num_docs, 1);
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.first.com");

        let query = Query::parse(
            "test site:www.first.com",
            index.schema(),
            index.tokenizers(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(RegionCount::default(), SignalAggregator::default());
        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.num_docs, 1);
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.first.com");

        let query = Query::parse(
            "test -site:first.com",
            index.schema(),
            index.tokenizers(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(RegionCount::default(), SignalAggregator::default());
        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.num_docs, 1);
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.second.com");
    }

    #[test]
    fn title_query() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");

        index
            .insert(Webpage::new(
                r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                This is a test website
                            </body>
                        </html>
                    "#,
                "https://www.first.com",
                vec![],
                1.0,
                0,
            ))
            .expect("failed to parse webpage");
        index
            .insert(Webpage::new(
                r#"
                        <html>
                            <head>
                                <title>Test test</title>
                            </head>
                            <body>
                                This is a test website
                            </body>
                        </html>
                    "#,
                "https://www.second.com",
                vec![],
                1.0,
                0,
            ))
            .expect("failed to parse webpage");
        index.commit().expect("failed to commit index");

        let query = Query::parse(
            "intitle:website",
            index.schema(),
            index.tokenizers(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(RegionCount::default(), SignalAggregator::default());
        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.num_docs, 1);
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.first.com");
    }

    #[test]
    fn url_query() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");

        index
            .insert(Webpage::new(
                r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                This is a test website
                            </body>
                        </html>
                    "#,
                "https://www.first.com/forum",
                vec![],
                1.0,
                0,
            ))
            .expect("failed to parse webpage");
        index
            .insert(Webpage::new(
                r#"
                        <html>
                            <head>
                                <title>Test test</title>
                            </head>
                            <body>
                                This is a test website
                            </body>
                        </html>
                    "#,
                "https://www.second.com",
                vec![],
                1.0,
                0,
            ))
            .expect("failed to parse webpage");
        index.commit().expect("failed to commit index");

        let query = Query::parse(
            "test inurl:forum",
            index.schema(),
            index.tokenizers(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(RegionCount::default(), SignalAggregator::default());
        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.num_docs, 1);
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.first.com/forum");
    }

    #[test]
    fn empty_query() {
        let schema = Arc::new(create_schema());

        let query = Query::parse(
            "",
            Arc::clone(&schema),
            &TokenizerManager::new(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query");

        assert!(query.is_empty())
    }
}
