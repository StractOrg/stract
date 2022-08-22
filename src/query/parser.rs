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

use tantivy::{
    query::{BooleanQuery, BoostQuery, Occur, TermQuery},
    schema::IndexRecordOption,
    tokenizer::{TextAnalyzer, TokenizerManager},
};

use crate::schema::{Field, ALL_FIELDS};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Term {
    Simple(String),
    Not(Box<Term>),
    Site(String),
    Title(String),
}

impl Term {
    pub fn as_tantivy_query(
        &self,
        fields: &[(tantivy::schema::Field, &tantivy::schema::FieldEntry)],
        tokenizer_manager: &TokenizerManager,
    ) -> (Occur, Box<dyn tantivy::query::Query + 'static>) {
        match self {
            Term::Simple(term) => (
                Occur::Must,
                Box::new(BooleanQuery::new(Term::into_tantivy_simple(
                    term,
                    fields,
                    tokenizer_manager,
                ))),
            ),
            Term::Not(subterm) => (
                Occur::MustNot,
                Box::new(BooleanQuery::new(vec![
                    subterm.as_tantivy_query(fields, tokenizer_manager)
                ])),
            ),
            Term::Site(site) => (
                Occur::Must,
                Box::new(BooleanQuery::new(Term::into_tantivy_site(
                    site,
                    fields,
                    tokenizer_manager,
                ))),
            ),
            Term::Title(title) => {
                let (field, entry) = fields
                    .iter()
                    .find(|(field, _)| {
                        matches!(ALL_FIELDS[field.field_id() as usize], Field::Title)
                    })
                    .unwrap();
                (
                    Occur::Must,
                    Term::tantivy_term_query(field, entry, tokenizer_manager, title),
                )
            }
        }
    }

    fn into_tantivy_simple(
        term: &str,
        fields: &[(tantivy::schema::Field, &tantivy::schema::FieldEntry)],
        tokenizer_manager: &TokenizerManager,
    ) -> Vec<(Occur, Box<dyn tantivy::query::Query + 'static>)> {
        fields
            .iter()
            .filter(|(field, _)| ALL_FIELDS[field.field_id() as usize].is_searchable())
            .into_iter()
            .map(|(field, entry)| {
                (
                    Occur::Should,
                    Term::tantivy_term_query(field, entry, tokenizer_manager, term),
                )
            })
            .collect()
    }

    fn into_tantivy_site(
        term: &str,
        fields: &[(tantivy::schema::Field, &tantivy::schema::FieldEntry)],
        tokenizer_manager: &TokenizerManager,
    ) -> Vec<(Occur, Box<dyn tantivy::query::Query + 'static>)> {
        fields
            .iter()
            .filter(|(field, _)| {
                matches!(
                    ALL_FIELDS[field.field_id() as usize],
                    Field::Domain | Field::Host
                )
            })
            .into_iter()
            .map(|(field, entry)| {
                (
                    Occur::Should,
                    Term::tantivy_term_query(field, entry, tokenizer_manager, term),
                )
            })
            .collect()
    }

    fn tantivy_term_query(
        field: &tantivy::schema::Field,
        entry: &tantivy::schema::FieldEntry,
        tokenizer_manager: &TokenizerManager,
        term: &str,
    ) -> Box<dyn tantivy::query::Query + 'static> {
        let analyzer = Term::get_tantivy_analyzer(entry, tokenizer_manager);
        let processed_terms = Term::process_tantivy_term(term, analyzer, *field);

        let processed_queries = processed_terms
            .map(|term| {
                (
                    Occur::Must,
                    Box::new(TermQuery::new(
                        term,
                        IndexRecordOption::WithFreqsAndPositions,
                    )) as Box<dyn tantivy::query::Query>,
                )
            })
            .collect();
        let boost = ALL_FIELDS[field.field_id() as usize].boost().unwrap_or(1.0);

        Box::new(BoostQuery::new(
            Box::new(BooleanQuery::new(processed_queries)),
            boost,
        ))
    }

    fn get_tantivy_analyzer(
        entry: &tantivy::schema::FieldEntry,
        tokenizer_manager: &tantivy::tokenizer::TokenizerManager,
    ) -> Option<TextAnalyzer> {
        match entry.field_type() {
            tantivy::schema::FieldType::Str(options) => {
                options.get_indexing_options().and_then(|indexing_options| {
                    let tokenizer_name = indexing_options.tokenizer();
                    tokenizer_manager.get(tokenizer_name)
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

fn parse_term(term: &str) -> Box<Term> {
    // TODO: re-write this entire function once if-let chains become stable
    if let Some(not_term) = term.strip_prefix('-') {
        if !not_term.is_empty() && !not_term.starts_with('-') {
            Box::new(Term::Not(parse_term(not_term)))
        } else {
            Box::new(Term::Simple(term.to_string()))
        }
    } else if let Some(site) = term.strip_prefix("site:") {
        if !site.is_empty() {
            Box::new(Term::Site(site.to_string()))
        } else {
            Box::new(Term::Simple(term.to_string()))
        }
    } else if let Some(title) = term.strip_prefix("intitle:") {
        if !title.is_empty() {
            Box::new(Term::Title(title.to_string()))
        } else {
            Box::new(Term::Simple(term.to_string()))
        }
    } else {
        Box::new(Term::Simple(term.to_string()))
    }
}

#[allow(clippy::vec_box)]
pub fn parse(query: &str) -> Vec<Box<Term>> {
    query.split_whitespace().map(parse_term).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_not() {
        assert_eq!(
            parse("this -that"),
            vec![
                Box::new(Term::Simple("this".to_string())),
                Box::new(Term::Not(Box::new(Term::Simple("that".to_string()))))
            ]
        );

        assert_eq!(
            parse("this -"),
            vec![
                Box::new(Term::Simple("this".to_string())),
                Box::new(Term::Simple("-".to_string()))
            ]
        );
    }

    #[test]
    fn double_not() {
        assert_eq!(
            parse("this --that"),
            vec![
                Box::new(Term::Simple("this".to_string())),
                Box::new(Term::Simple("--that".to_string()))
            ]
        );
    }

    #[test]
    fn site() {
        assert_eq!(
            parse("this site:test.com"),
            vec![
                Box::new(Term::Simple("this".to_string())),
                Box::new(Term::Site("test.com".to_string()))
            ]
        );
    }

    #[test]
    fn title() {
        assert_eq!(
            parse("this intitle:test"),
            vec![
                Box::new(Term::Simple("this".to_string())),
                Box::new(Term::Title("test".to_string()))
            ]
        );
    }
}
