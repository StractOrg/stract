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

use crate::{query::weight::Weight, schema::ALL_FIELDS, Error, Result};
use nom::{
    bytes::complete::{tag, take_while},
    character::complete::multispace0,
    error::ParseError,
    multi::separated_list0,
    sequence::preceded,
    IResult,
};
use std::{collections::BTreeMap, sync::Arc};
use tantivy::{
    schema::{FieldType, IndexRecordOption, Schema},
    tokenizer::TextAnalyzer,
};

use self::bm25::Bm25Weight;

mod bm25;
mod field_union;
mod term_intersection;
mod term_scorer;
mod vec_docset;
mod weight;
use itertools::Itertools;

/// A combinator that takes a parser `inner` and produces a parser that also consumes leading
/// whitespace, returning the output of `inner`.
fn ws<'a, F: 'a, O, E: ParseError<&'a str>>(
    inner: F,
) -> impl FnMut(&'a str) -> IResult<&'a str, O, E>
where
    F: Fn(&'a str) -> IResult<&'a str, O, E>,
{
    preceded(multispace0, inner)
}

fn term(term: &str) -> IResult<&str, &str> {
    ws(take_while(|c: char| !c.is_whitespace()))(term)
}

pub struct FieldData {
    tantivy: tantivy::schema::Field,
    scoring: Bm25Weight,
    index_record_option: Option<IndexRecordOption>,
    boost: Option<f32>,
    analyzer: Option<TextAnalyzer>,
}
impl std::fmt::Debug for FieldData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FieldData")
            .field("tanitvy", &self.tantivy)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct Query {
    terms: Vec<String>,
    schema: Arc<Schema>,
}

impl Query {
    pub fn parse(query: &str, schema: Arc<Schema>) -> Result<Query> {
        match separated_list0(tag(" "), term)(query) {
            Ok((remaining, terms)) => {
                debug_assert!(remaining.is_empty());

                let terms = terms
                    .into_iter()
                    .filter(|term| !term.is_empty())
                    .map(|term| term.to_string())
                    .unique()
                    .collect();
                Ok(Query { terms, schema })
            }
            Err(error) => Err(Error::ParsingError(error.to_string())),
        }
    }
}

impl tantivy::query::Query for Query {
    fn weight(
        &self,
        searcher: &tantivy::Searcher,
        scoring_enabled: bool,
    ) -> tantivy::Result<Box<dyn tantivy::query::Weight>> {
        let terms = self.terms.clone();

        let fields: Vec<_> = ALL_FIELDS
            .iter()
            .filter(|field| field.is_searchable())
            .map(|field| {
                let tantivy_field = searcher.schema().get_field(field.as_str()).unwrap();
                let analyzer = searcher.index().tokenizer_for_field(tantivy_field).ok();

                let tantivy_terms: Vec<_> = terms
                    .iter()
                    .flat_map(|term_text| {
                        let terms = analyzer
                            .as_ref()
                            .map(|analyzer| {
                                let mut terms = Vec::new();

                                let mut stream = analyzer.token_stream(term_text);

                                while let Some(token) = stream.next() {
                                    terms.push(token.text.clone());
                                }

                                terms
                            })
                            .unwrap_or_else(|| vec![term_text.clone()]);

                        terms
                            .into_iter()
                            .map(|term| tantivy::Term::from_field_text(tantivy_field, &term))
                    })
                    .collect();

                FieldData {
                    tantivy: tantivy_field,
                    index_record_option: searcher
                        .schema()
                        .get_field_entry(tantivy_field)
                        .field_type()
                        .get_index_record_option(),
                    boost: field.boost(),
                    scoring: Bm25Weight::for_terms(searcher, &tantivy_terms).unwrap(),
                    analyzer,
                }
            })
            .collect();

        Ok(Box::new(Weight::new(terms, fields, scoring_enabled)))
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

        let query = Query::parse("This is a simple query the the query the the", Arc::clone(&schema))
            .expect("Failed to parse query");

        assert_eq!(
            query.terms,
            vec![
                "This".to_string(),
                "is".to_string(),
                "a".to_string(),
                "simple".to_string(),
                "query".to_string(),
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
                "This".to_string(),
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
            vec!["term!".to_string(), "term#".to_string(), "$".to_string()]
        );
    }
}
