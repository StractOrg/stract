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

use tantivy::{
    query::{BooleanQuery, Occur, PhraseQuery, TermQuery},
    tokenizer::Tokenizer as _,
};

use crate::{
    bangs::BANG_PREFIXES,
    schema::{Field, TextField},
};

use super::{CompoundAwareTerm, SimpleOrPhrase, SimpleTerm, Term, TermCompound};

impl CompoundAwareTerm {
    pub fn as_tantivy_query(
        &self,
        fields: &[tantivy::schema::Field],
    ) -> (Occur, Box<dyn tantivy::query::Query + 'static>) {
        if !self.adjacent_terms.is_empty() {
            if let Term::SimpleOrPhrase(SimpleOrPhrase::Simple(simple_term)) = &self.term {
                return simple_into_tantivy(simple_term, &self.adjacent_terms, fields);
            }
        }

        self.term.as_tantivy_query(fields)
    }
}

fn simple_into_tantivy(
    term: &SimpleTerm,
    adjacent_terms: &[TermCompound],
    fields: &[tantivy::schema::Field],
) -> (Occur, Box<dyn tantivy::query::Query + 'static>) {
    let mut queries = Term::into_tantivy_simple(term, fields);

    let fields = fields
        .iter()
        .filter(|field| {
            matches!(
                Field::get(field.field_id() as usize),
                Some(Field::Text(TextField::AllBody))
                    | Some(Field::Text(TextField::Title))
                    | Some(Field::Text(TextField::Url))
            )
        })
        .copied()
        .collect::<Vec<_>>();

    for adjacent_term in adjacent_terms {
        let combined = adjacent_term
            .terms
            .iter()
            .map(|term| term.as_str())
            .collect::<String>();

        for field in &fields {
            queries.push((Occur::Should, Term::tantivy_text_query(field, &combined)))
        }
    }

    (Occur::Must, Box::new(BooleanQuery::new(queries)))
}

fn phrase_query(
    terms: &[String],
    fields: &[tantivy::schema::Field],
) -> (Occur, Box<dyn tantivy::query::Query + 'static>) {
    let mut phrases = Vec::with_capacity(fields.len());
    let phrase = terms.join(" ");

    for (field, tv_field) in fields
        .iter()
        .filter_map(|tv_field| {
            Field::get(tv_field.field_id() as usize).map(|mapped| (mapped, *tv_field))
        })
        .filter(|(field, _)| field.is_searchable())
        .filter(|(field, _)| field.has_pos())
    {
        let mut processed_terms = Term::process_tantivy_term(&phrase, tv_field);

        if processed_terms.is_empty() {
            continue;
        }

        if processed_terms.len() == 1 {
            let options = field.as_text().unwrap().index_option();

            phrases.push((
                Occur::Should,
                Box::new(TermQuery::new(processed_terms.pop().unwrap(), options))
                    as Box<dyn tantivy::query::Query>,
            ));
        } else {
            phrases.push((
                Occur::Should,
                Box::new(PhraseQuery::new(processed_terms)) as Box<dyn tantivy::query::Query>,
            ));
        }
    }

    (Occur::Must, Box::new(BooleanQuery::new(phrases)))
}

impl Term {
    fn as_tantivy_query(
        &self,
        fields: &[tantivy::schema::Field],
    ) -> (Occur, Box<dyn tantivy::query::Query + 'static>) {
        match self {
            Term::SimpleOrPhrase(SimpleOrPhrase::Simple(term)) => {
                simple_into_tantivy(term, &[], fields)
            }
            Term::SimpleOrPhrase(SimpleOrPhrase::Phrase(phrase)) => phrase_query(phrase, fields),
            Term::Not(subterm) => (
                Occur::MustNot,
                Box::new(BooleanQuery::new(vec![subterm.as_tantivy_query(fields)])),
            ),
            Term::Site(site) => (
                Occur::Must,
                Box::new(BooleanQuery::new(Term::into_tantivy_site(site, fields))),
            ),
            Term::Title(title) => {
                let field = fields
                    .iter()
                    .find(|field| {
                        matches!(
                            Field::get(field.field_id() as usize),
                            Some(Field::Text(TextField::Title))
                        )
                    })
                    .unwrap();

                (
                    Occur::Must,
                    Term::tantivy_text_query(field, &title.as_string()),
                )
            }
            Term::Body(body) => {
                let field = fields
                    .iter()
                    .find(|field| {
                        matches!(
                            Field::get(field.field_id() as usize),
                            Some(Field::Text(TextField::AllBody))
                        )
                    })
                    .unwrap();

                (
                    Occur::Must,
                    Term::tantivy_text_query(field, &body.as_string()),
                )
            }
            Term::Url(url) => {
                let field = fields
                    .iter()
                    .find(|field| {
                        matches!(
                            Field::get(field.field_id() as usize),
                            Some(Field::Text(TextField::Url))
                        )
                    })
                    .unwrap();

                (
                    Occur::Must,
                    Term::tantivy_text_query(field, &url.as_string()),
                )
            }
            Term::PossibleBang(text) => {
                let mut term = String::new();

                term.push(BANG_PREFIXES[0]);
                term.push_str(text);

                simple_into_tantivy(&term.into(), &[], fields)
            }
        }
    }

    fn into_tantivy_simple(
        term: &SimpleTerm,
        fields: &[tantivy::schema::Field],
    ) -> Vec<(Occur, Box<dyn tantivy::query::Query + 'static>)> {
        fields
            .iter()
            .filter_map(|tv_field| {
                Field::get(tv_field.field_id() as usize)
                    .filter(|field| field.is_searchable())
                    .map(|_| tv_field)
            })
            .map(|field| {
                (
                    Occur::Should,
                    Term::tantivy_text_query(field, term.as_str()),
                )
            })
            .collect()
    }

    fn into_tantivy_site(
        term: &str,
        fields: &[tantivy::schema::Field],
    ) -> Vec<(Occur, Box<dyn tantivy::query::Query + 'static>)> {
        fields
            .iter()
            .filter(|field| {
                matches!(
                    Field::get(field.field_id() as usize),
                    Some(Field::Text(TextField::UrlForSiteOperator))
                )
            })
            .map(|field| {
                let processed_terms = Term::process_tantivy_term(term, *field);

                if processed_terms.len() == 1 {
                    let term = processed_terms.first().unwrap().clone();
                    (
                        Occur::Should,
                        Box::new(TermQuery::new(
                            term,
                            tantivy::schema::IndexRecordOption::Basic,
                        )) as Box<dyn tantivy::query::Query>,
                    )
                } else {
                    (
                        Occur::Should,
                        Box::new(PhraseQuery::new(processed_terms))
                            as Box<dyn tantivy::query::Query>,
                    )
                }
            })
            .collect()
    }

    fn tantivy_text_query(
        field: &tantivy::schema::Field,
        term: &str,
    ) -> Box<dyn tantivy::query::Query + 'static> {
        let mut processed_terms = Term::process_tantivy_term(term, *field);

        let option = Field::get(field.field_id() as usize)
            .unwrap()
            .as_text()
            .unwrap()
            .index_option();

        let processed_query = if processed_terms.len() == 1 {
            let term = processed_terms.remove(0);
            Box::new(TermQuery::new(term, option)) as Box<dyn tantivy::query::Query + 'static>
        } else {
            Box::new(BooleanQuery::new(
                processed_terms
                    .into_iter()
                    .map(|term| {
                        (
                            Occur::Must,
                            Box::new(TermQuery::new(term, option))
                                as Box<dyn tantivy::query::Query + 'static>,
                        )
                    })
                    .collect(),
            )) as Box<dyn tantivy::query::Query + 'static>
        };

        Box::new(processed_query)
    }

    fn process_tantivy_term(
        term: &str,
        tantivy_field: tantivy::schema::Field,
    ) -> Vec<tantivy::Term> {
        match Field::get(tantivy_field.field_id() as usize) {
            Some(Field::Fast(_)) => vec![tantivy::Term::from_field_text(tantivy_field, term)],
            Some(Field::Text(text_field)) => {
                let mut terms: Vec<tantivy::Term> = Vec::new();
                let mut tokenizer = text_field.query_tokenizer();
                let mut token_stream = tokenizer.token_stream(term);
                token_stream.process(&mut |token| {
                    let term = tantivy::Term::from_field_text(tantivy_field, &token.text);
                    terms.push(term);
                });

                terms
            }
            None => vec![],
        }
    }
}
