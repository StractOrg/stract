// Stract is an open source web search engine.
// Copyright (C) 2023 Stract ApS
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
    query::{BooleanQuery, Occur, PhraseQuery, TermQuery},
    tokenizer::{TextAnalyzer, TokenizerManager},
};

use crate::{
    bangs::BANG_PREFIX,
    schema::{Field, TextField, ALL_FIELDS},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Term {
    Simple(String),
    Phrase(String),
    Not(Box<Term>),
    Site(String),
    Title(String),
    Body(String),
    Url(String),
    PossibleBang(String),
}

impl ToString for Term {
    fn to_string(&self) -> String {
        match self {
            Term::Simple(term) => term.clone(),
            Term::Phrase(phrase) => "\"".to_string() + phrase.as_str() + "\"",
            Term::Not(term) => "-".to_string() + term.to_string().as_str(),
            Term::Site(site) => "site:".to_string() + site.as_str(),
            Term::Title(title) => "intitle:".to_string() + title.as_str(),
            Term::Body(body) => "inbody:".to_string() + body.as_str(),
            Term::Url(url) => "inurl:".to_string() + url.as_str(),
            Term::PossibleBang(bang) => "!".to_string() + bang.as_str(),
        }
    }
}

fn simple_into_tantivy(
    term: &str,
    fields: &[(tantivy::schema::Field, &tantivy::schema::FieldEntry)],
    tokenizer_manager: &TokenizerManager,
) -> Vec<(Occur, Box<dyn tantivy::query::Query + 'static>)> {
    let (backlink_field, backlink_field_entry) = fields
        .iter()
        .find(|(field, _)| {
            matches!(
                ALL_FIELDS[field.field_id() as usize],
                Field::Text(TextField::BacklinkText)
            )
        })
        .unwrap();

    vec![
        (
            Occur::Must,
            Box::new(BooleanQuery::new(Term::into_tantivy_simple(
                term,
                fields,
                tokenizer_manager,
            ))),
        ),
        (
            Occur::Should,
            Box::new(Term::tantivy_text_query(
                backlink_field,
                backlink_field_entry,
                tokenizer_manager,
                term,
            )),
        ),
    ]
}

impl Term {
    pub fn as_simple_text(&self) -> &str {
        match self {
            Term::Simple(term) => term,
            Term::Phrase(terms) => terms,
            Term::Not(term) => term.as_simple_text(),
            Term::Site(term) => term,
            Term::Title(term) => term,
            Term::Body(term) => term,
            Term::Url(term) => term,
            Term::PossibleBang(term) => term,
        }
    }

    pub fn as_tantivy_query(
        &self,
        fields: &[(tantivy::schema::Field, &tantivy::schema::FieldEntry)],
        tokenizer_manager: &TokenizerManager,
    ) -> Vec<(Occur, Box<dyn tantivy::query::Query + 'static>)> {
        match self {
            Term::Simple(term) => simple_into_tantivy(term, fields, tokenizer_manager),
            Term::Phrase(phrase) => {
                let mut phrases = Vec::with_capacity(fields.len());

                for (field, entry) in fields
                    .iter()
                    .filter(|(field, _)| ALL_FIELDS[field.field_id() as usize].is_searchable())
                    .filter(|(field, _)| ALL_FIELDS[field.field_id() as usize].has_pos())
                {
                    phrases.push((
                        Occur::Should,
                        Term::tantivy_text_query(field, entry, tokenizer_manager, phrase),
                    ));
                }

                vec![(Occur::Must, Box::new(BooleanQuery::new(phrases)))]
            }
            Term::Not(subterm) => vec![(
                Occur::MustNot,
                Box::new(BooleanQuery::new(
                    subterm.as_tantivy_query(fields, tokenizer_manager),
                )),
            )],
            Term::Site(site) => vec![(
                Occur::Must,
                Box::new(BooleanQuery::new(Term::into_tantivy_site(
                    site,
                    fields,
                    tokenizer_manager,
                ))),
            )],
            Term::Title(title) => {
                let (field, entry) = fields
                    .iter()
                    .find(|(field, _)| {
                        matches!(
                            ALL_FIELDS[field.field_id() as usize],
                            Field::Text(TextField::Title)
                        )
                    })
                    .unwrap();

                vec![(
                    Occur::Must,
                    Term::tantivy_text_query(field, entry, tokenizer_manager, title),
                )]
            }
            Term::Body(body) => {
                let (field, entry) = fields
                    .iter()
                    .find(|(field, _)| {
                        matches!(
                            ALL_FIELDS[field.field_id() as usize],
                            Field::Text(TextField::AllBody)
                        )
                    })
                    .unwrap();

                vec![(
                    Occur::Must,
                    Term::tantivy_text_query(field, entry, tokenizer_manager, body),
                )]
            }
            Term::Url(url) => {
                let (field, entry) = fields
                    .iter()
                    .find(|(field, _)| {
                        matches!(
                            ALL_FIELDS[field.field_id() as usize],
                            Field::Text(TextField::Url)
                        )
                    })
                    .unwrap();

                vec![(
                    Occur::Must,
                    Term::tantivy_text_query(field, entry, tokenizer_manager, url),
                )]
            }
            Term::PossibleBang(text) => {
                let mut term = String::new();

                term.push(BANG_PREFIX);
                term.push_str(text);

                simple_into_tantivy(&term, fields, tokenizer_manager)
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
            .map(|(field, entry)| {
                (
                    Occur::Should,
                    Term::tantivy_text_query(field, entry, tokenizer_manager, term),
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
                    Field::Text(TextField::DomainNoTokenizer)
                        | Field::Text(TextField::SiteNoTokenizer)
                )
            })
            .map(|(field, entry)| {
                (
                    Occur::Should,
                    Term::tantivy_text_query(field, entry, tokenizer_manager, term),
                )
            })
            .collect()
    }

    fn tantivy_text_query(
        field: &tantivy::schema::Field,
        entry: &tantivy::schema::FieldEntry,
        tokenizer_manager: &TokenizerManager,
        term: &str,
    ) -> Box<dyn tantivy::query::Query + 'static> {
        let analyzer = Term::get_tantivy_analyzer(entry, tokenizer_manager);
        let mut processed_terms = Term::process_tantivy_term(term, analyzer, *field);

        let processed_query =
            if processed_terms.len() > 1 && ALL_FIELDS[field.field_id() as usize].has_pos() {
                Box::new(PhraseQuery::new(processed_terms)) as Box<dyn tantivy::query::Query>
            } else {
                let option = ALL_FIELDS[field.field_id() as usize]
                    .as_text()
                    .unwrap()
                    .index_option();

                let term = processed_terms.remove(0);
                Box::new(TermQuery::new(term, option))
            };

        Box::new(processed_query)
    }

    pub fn get_tantivy_analyzer(
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

    pub fn process_tantivy_term(
        term: &str,
        analyzer: Option<TextAnalyzer>,
        tantivy_field: tantivy::schema::Field,
    ) -> Vec<tantivy::Term> {
        match analyzer {
            None => vec![tantivy::Term::from_field_text(tantivy_field, term)],
            Some(tokenizer) => {
                let mut terms: Vec<tantivy::Term> = Vec::new();
                let mut token_stream = tokenizer.token_stream(term);
                token_stream.process(&mut |token| {
                    let term = tantivy::Term::from_field_text(tantivy_field, &token.text);
                    terms.push(term);
                });

                terms
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
    } else if let Some(body) = term.strip_prefix("inbody:") {
        if !body.is_empty() {
            Box::new(Term::Body(body.to_string()))
        } else {
            Box::new(Term::Simple(term.to_string()))
        }
    } else if let Some(url) = term.strip_prefix("inurl:") {
        if !url.is_empty() {
            Box::new(Term::Url(url.to_string()))
        } else {
            Box::new(Term::Simple(term.to_string()))
        }
    } else if let Some(bang) = term.strip_prefix(BANG_PREFIX) {
        Box::new(Term::PossibleBang(bang.to_string()))
    } else {
        Box::new(Term::Simple(term.to_string()))
    }
}

#[allow(clippy::vec_box)]
pub fn parse(query: &str) -> Vec<Box<Term>> {
    let query = query.to_lowercase();

    let mut res = Vec::new();

    let mut cur_term_begin = 0;

    for (offset, c) in query.char_indices() {
        if cur_term_begin > offset {
            continue;
        }

        if query[cur_term_begin..].starts_with('"') {
            if let Some(offset) = query[cur_term_begin + 1..].find('"') {
                let offset = offset + cur_term_begin + 1;
                res.push(Box::new(Term::Phrase(
                    query[cur_term_begin + 1..offset].to_string(),
                )));

                cur_term_begin = offset + 1;
                continue;
            }
        }
        if c.is_whitespace() {
            if offset - cur_term_begin == 0 {
                cur_term_begin = offset + 1;
                continue;
            }

            res.push(parse_term(&query[cur_term_begin..offset]));
            cur_term_begin = offset + 1;
        }
    }

    if cur_term_begin < query.len() {
        res.push(parse_term(&query[cur_term_begin..query.len()]));
    }

    res
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

    #[test]
    fn body() {
        assert_eq!(
            parse("this inbody:test"),
            vec![
                Box::new(Term::Simple("this".to_string())),
                Box::new(Term::Body("test".to_string()))
            ]
        );
    }

    #[test]
    fn url() {
        assert_eq!(
            parse("this inurl:test"),
            vec![
                Box::new(Term::Simple("this".to_string())),
                Box::new(Term::Url("test".to_string()))
            ]
        );
    }

    #[test]
    fn empty() {
        assert_eq!(parse(""), vec![]);
    }

    #[test]
    fn phrase() {
        assert_eq!(
            parse("\"this is a\" inurl:test"),
            vec![
                // Box::new(Term::Phrase(vec![
                //     "this".to_string(),
                //     "is".to_string(),
                //     "a".to_string()
                // ])),
                Box::new(Term::Phrase("this is a".to_string(),)),
                Box::new(Term::Url("test".to_string()))
            ]
        );
        assert_eq!(
            parse("\"this is a inurl:test"),
            vec![
                Box::new(Term::Simple("\"this".to_string())),
                Box::new(Term::Simple("is".to_string())),
                Box::new(Term::Simple("a".to_string())),
                Box::new(Term::Url("test".to_string()))
            ]
        );
        assert_eq!(
            parse("this is a\" inurl:test"),
            vec![
                Box::new(Term::Simple("this".to_string())),
                Box::new(Term::Simple("is".to_string())),
                Box::new(Term::Simple("a\"".to_string())),
                Box::new(Term::Url("test".to_string()))
            ]
        );

        assert_eq!(
            parse("\"this is a inurl:test\""),
            vec![Box::new(Term::Phrase("this is a inurl:test".to_string(),)),]
        );

        assert_eq!(
            parse("\"\""),
            vec![Box::new(Term::Phrase("".to_string(),)),]
        );
    }
}
