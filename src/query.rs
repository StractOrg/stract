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
use crate::{Error, Result};
use nom::{
    bytes::complete::{tag, take_while},
    character::complete::multispace0,
    error::ParseError,
    multi::separated_list0,
    sequence::preceded,
    IResult,
};
use tantivy::{
    query::{BooleanQuery, Occur, TermQuery},
    schema::{Field, FieldEntry, IndexRecordOption, Schema},
    tokenizer::{TextAnalyzer, TokenizerManager},
    Term,
};

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
pub struct Query {
    terms: Vec<String>,
}

impl Query {
    pub fn parse(query: &str) -> Result<Query> {
        match separated_list0(tag(" "), term)(query) {
            Ok((remaining, terms)) => {
                debug_assert!(remaining.is_empty());

                let terms = terms
                    .into_iter()
                    .filter(|term| !term.is_empty())
                    .map(|term| term.to_string())
                    .collect();
                Ok(Query { terms })
            }
            Err(error) => Err(Error::ParsingError(error.to_string())),
        }
    }

    fn get_tantivy_analyzer(
        entry: &FieldEntry,
        tokenizer_manager: &TokenizerManager,
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
        tantivy_field: Field,
    ) -> Term {
        match analyzer {
            None => Term::from_field_text(tantivy_field, term),
            Some(tokenizer) => {
                let mut terms: Vec<Term> = Vec::new();
                let mut token_stream = tokenizer.token_stream(term);
                token_stream.process(&mut |token| {
                    let term = Term::from_field_text(tantivy_field, &token.text);
                    terms.push(term);
                });

                terms.into_iter().next().unwrap()
            }
        }
    }

    pub fn tantivy(
        &self,
        schema: &Schema,
        tokenizer_manager: &TokenizerManager,
    ) -> Box<dyn tantivy::query::Query> {
        let queries = self
            .terms
            .iter()
            .map(|term| {
                let one_term = schema
                    .fields()
                    .into_iter()
                    .map(|(field, entry)| {
                        let analyzer = Query::get_tantivy_analyzer(entry, tokenizer_manager);
                        let term = Query::process_tantivy_term(term, analyzer, field);

                        let term_query = Box::new(TermQuery::new(
                            term,
                            IndexRecordOption::WithFreqsAndPositions,
                        ))
                            as Box<dyn tantivy::query::Query>;
                        (Occur::Should, term_query)
                    })
                    .collect();

                Box::new(BooleanQuery::new(one_term)) as Box<dyn tantivy::query::Query>
            })
            .map(|term_query| (Occur::Must, term_query))
            .collect();

        Box::new(BooleanQuery::new(queries))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_parse() {
        let query = Query::parse("This is a simple query").expect("Failed to parse query");

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
    fn parse_trailing_leading_whitespace() {
        let query = Query::parse("   This is a simple query   ").expect("Failed to parse query");

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
        let terms = Query::parse("123").expect("Failed to parse query").terms;
        assert_eq!(terms, vec!["123".to_string()]);

        let terms = Query::parse("123 33").expect("Failed to parse query").terms;
        assert_eq!(terms, vec!["123".to_string(), "33".to_string()]);

        let terms = Query::parse("term! term# $")
            .expect("Failed to parse query")
            .terms;
        assert_eq!(
            terms,
            vec!["term!".to_string(), "term#".to_string(), "$".to_string()]
        );
    }
}
