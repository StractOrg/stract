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

use crate::{schema::ALL_FIELDS, tokenizer::NormalTokenizer, Result};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tantivy::{
    query::{BooleanQuery, BoostQuery, Occur, TermQuery},
    schema::{FieldType, IndexRecordOption, Schema},
    tokenizer::{TextAnalyzer, Tokenizer},
};

use lalrpop_util::lalrpop_mod;
lalrpop_mod!(pub parser, "/query/parser.rs"); // synthesized by LALRPOP

static PARSER: once_cell::sync::Lazy<parser::TermsParser> =
    once_cell::sync::Lazy::new(parser::TermsParser::new);

const MAX_SIMILAR_TERMS: usize = 10;

#[derive(Debug)]
pub enum Term<'a> {
    Simple(&'a str),
}

#[derive(Clone, Debug)]
pub struct Query {
    terms: Vec<String>,
    schema: Arc<Schema>,
}

impl Query {
    pub fn parse(query: &str, schema: Arc<Schema>) -> Result<Query> {
        let mut raw_terms = Vec::new();

        let terms = PARSER.parse(query)?;
        for term in terms {
            match term {
                Term::Simple(term) => {
                    let mut stream = NormalTokenizer::default().token_stream(term);

                    while let Some(token) = stream.next() {
                        raw_terms.push(token.text.to_string());
                    }
                }
            }
        }

        let mut term_count = HashMap::new();
        let mut terms = Vec::new();
        for term in raw_terms {
            let count = term_count.entry(term.clone()).or_insert(0);

            if *count < MAX_SIMILAR_TERMS {
                terms.push(term);
            }

            *count += 1;
        }

        Ok(Query { terms, schema })
    }

    pub fn simple_terms(&self) -> &[String] {
        &self.terms
    }

    pub fn is_empty(&self) -> bool {
        self.terms.is_empty()
    }

    fn get_tantivy_analyzer(
        entry: &tantivy::schema::FieldEntry,
        tokenizer_manager: &tantivy::tokenizer::TokenizerManager,
    ) -> Option<TextAnalyzer> {
        match entry.field_type() {
            tantivy::schema::FieldType::Str(options) => {
                options.get_indexing_options().map(|indexing_options| {
                    let tokenizer_name = indexing_options.tokenizer();
                    tokenizer_manager
                        .get(tokenizer_name)
                        .expect("Unknown tokenizer")
                })
            }
            _ => None,
        }
    }

    fn process_tantivy_term(
        term: &str,
        analyzer: Option<TextAnalyzer>,
        tantivy_field: tantivy::schema::Field,
    ) -> impl Iterator<Item = tantivy::Term> {
        match analyzer {
            None => vec![tantivy::Term::from_field_text(tantivy_field, term)].into_iter(),
            Some(tokenizer) => {
                let mut terms: Vec<tantivy::Term> = Vec::new();
                let mut token_stream = tokenizer.token_stream(term);
                token_stream.process(&mut |token| {
                    let term = tantivy::Term::from_field_text(tantivy_field, &token.text);
                    terms.push(term);
                });

                terms.into_iter()
            }
        }
    }
}

impl tantivy::query::Query for Query {
    fn weight(
        &self,
        searcher: &tantivy::Searcher,
        scoring_enabled: bool,
    ) -> tantivy::Result<Box<dyn tantivy::query::Weight>> {
        let schema = searcher.schema();
        let tokenizer_manager = searcher.index().tokenizers();

        let queries = self
            .terms
            .iter()
            .map(|term| {
                let one_term = schema
                    .fields()
                    .filter(|(field, _)| ALL_FIELDS[field.field_id() as usize].is_searchable())
                    .into_iter()
                    .map(|(field, entry)| {
                        let analyzer = Query::get_tantivy_analyzer(entry, tokenizer_manager);
                        let processed_terms = Query::process_tantivy_term(term, analyzer, field);

                        let processed_queries = processed_terms
                            .map(|term| {
                                (
                                    Occur::Should,
                                    Box::new(TermQuery::new(
                                        term,
                                        IndexRecordOption::WithFreqsAndPositions,
                                    ))
                                        as Box<dyn tantivy::query::Query>,
                                )
                            })
                            .collect();
                        let boost = ALL_FIELDS[field.field_id() as usize].boost().unwrap_or(1.0);

                        (
                            Occur::Should,
                            Box::new(BoostQuery::new(
                                Box::new(BooleanQuery::new(processed_queries)),
                                boost,
                            )) as Box<dyn tantivy::query::Query>,
                        )
                    })
                    .collect();

                Box::new(BooleanQuery::new(one_term)) as Box<dyn tantivy::query::Query>
            })
            .map(|term_query| (Occur::Must, term_query))
            .collect();

        let query = Box::new(BooleanQuery::new(queries));

        query.weight(searcher, scoring_enabled)
    }

    fn query_terms(&self, terms: &mut BTreeMap<tantivy::Term, bool>) {
        for term_text in &self.terms {
            for (field, entry) in self.schema.fields() {
                if let FieldType::Str(_) = entry.field_type() {
                    terms.insert(tantivy::Term::from_field_text(field, term_text), true);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::create_schema;

    use super::*;

    #[test]
    fn simple_parse() {
        let schema = Arc::new(create_schema());

        let query = Query::parse(
            "This is a simple query the the the the the the the the the the the the the",
            Arc::clone(&schema),
        )
        .expect("Failed to parse query");

        assert_eq!(
            query.terms,
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

        let query = Query::parse("   This is a simple query   ", Arc::clone(&schema))
            .expect("Failed to parse query");

        assert_eq!(
            query.terms,
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

        let terms = Query::parse("123", Arc::clone(&schema))
            .expect("Failed to parse query")
            .terms;
        assert_eq!(terms, vec!["123".to_string()]);

        let terms = Query::parse("123 33", Arc::clone(&schema))
            .expect("Failed to parse query")
            .terms;
        assert_eq!(terms, vec!["123".to_string(), "33".to_string()]);

        let terms = Query::parse("term! term# $", Arc::clone(&schema))
            .expect("Failed to parse query")
            .terms;
        assert_eq!(
            terms,
            vec![
                "term".to_string(),
                "!".to_string(),
                "term".to_string(),
                "#".to_string(),
                "$".to_string()
            ]
        );
    }

    #[test]
    fn empty_query() {
        let schema = Arc::new(create_schema());

        let query = Query::parse("", Arc::clone(&schema)).expect("Failed to parse query");

        assert!(query.is_empty())
    }
}
