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
    schema::{IndexRecordOption, Schema},
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

    pub fn tantivy(&self, schema: &Schema) -> Box<dyn tantivy::query::Query> {
        let queries = self
            .terms
            .iter()
            .map(|term| {
                let one_term = schema
                    .fields()
                    .into_iter()
                    .map(|(field, _)| {
                        (
                            Occur::Should,
                            Box::new(TermQuery::new(
                                Term::from_field_text(field, term),
                                IndexRecordOption::Basic,
                            )) as Box<dyn tantivy::query::Query>,
                        )
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
