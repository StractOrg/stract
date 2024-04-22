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
// along with this program.  If not, see <https://www.gnu.org/licenses/

use itertools::Itertools;
use tantivy::tokenizer::Tokenizer as _;

use crate::schema::{
    self,
    text_field::{self, TextField},
    TextFieldEnum,
};

use super::parser::{SimpleOrPhrase, SimpleTerm};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Term {
    text: SimpleOrPhrase,
    field: schema::TextFieldEnum,
}

impl Term {
    pub fn new(text: SimpleOrPhrase, field: TextFieldEnum) -> Self {
        Term { text, field }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Node {
    Term(Term),
    And(Box<Node>, Box<Node>),
    Or(Box<Node>, Box<Node>),
    Not(Box<Node>),
}

impl Node {
    pub fn and<T: Into<Node>>(self, other: T) -> Node {
        Node::And(Box::new(self), Box::new(other.into()))
    }

    pub fn or<T: Into<Node>>(self, other: T) -> Node {
        Node::Or(Box::new(self), Box::new(other.into()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Occur {
    Must,
    Should,
    MustNot,
}

impl Occur {
    pub fn compose(left: Occur, right: Occur) -> Occur {
        match (left, right) {
            (Occur::Should, _) => right,
            (Occur::Must, Occur::MustNot) => Occur::MustNot,
            (Occur::Must, _) => Occur::Must,
            (Occur::MustNot, Occur::MustNot) => Occur::Must,
            (Occur::MustNot, _) => Occur::MustNot,
        }
    }
}

impl From<Occur> for tantivy::query::Occur {
    fn from(value: Occur) -> Self {
        match value {
            Occur::Must => tantivy::query::Occur::Must,
            Occur::Should => tantivy::query::Occur::Should,
            Occur::MustNot => tantivy::query::Occur::MustNot,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Query {
    Term(Term),
    Boolean { clauses: Vec<(Occur, Query)> },
}

impl Query {
    fn compact(self) -> Query {
        match self {
            Query::Boolean { clauses } => {
                let mut new_clauses = vec![];
                for (occur, query) in clauses {
                    let query = query.compact();
                    // if the query is a boolean query, and it has the same occur as the current
                    // query, we can merge the clauses into the current query
                    // otherwise, we add the clause as is
                    match query {
                        Query::Boolean {
                            clauses: inner_clauses,
                        } if inner_clauses
                            .iter()
                            .all(|(inner_occur, _)| occur == *inner_occur) =>
                        {
                            new_clauses.extend(inner_clauses);
                        }

                        Query::Boolean {
                            clauses: inner_clauses,
                        } if inner_clauses.len() == 1 => {
                            let (inner_occur, q) = inner_clauses.into_iter().next().unwrap();

                            new_clauses.push((Occur::compose(occur, inner_occur), q));
                        }

                        _ => new_clauses.push((occur, query)),
                    }
                }
                Query::Boolean {
                    clauses: new_clauses,
                }
            }
            Query::Term(term) => Query::Term(term),
        }
    }

    fn deduplicate(self) -> Query {
        match self {
            Query::Boolean { clauses } => Query::Boolean {
                clauses: clauses
                    .into_iter()
                    .map(|(occur, query)| (occur, query.deduplicate()))
                    .unique()
                    .collect(),
            },
            Query::Term(term) => Query::Term(term),
        }
    }

    pub fn as_tantivy(
        &self,
        schema: &tantivy::schema::Schema,
    ) -> Option<Box<dyn tantivy::query::Query>> {
        match self {
            Query::Term(Term { text, field }) => match text {
                SimpleOrPhrase::Simple(s) => {
                    let mut terms = process_tantivy_term(s.as_str(), *field, schema);

                    let option = field.record_option();
                    if terms.len() == 1 {
                        let term = terms.remove(0);
                        Some(Box::new(tantivy::query::TermQuery::new(term, option)))
                    } else {
                        Some(Box::new(tantivy::query::BooleanQuery::new(
                            terms
                                .into_iter()
                                .map(|term| {
                                    (
                                        tantivy::query::Occur::Must,
                                        Box::new(tantivy::query::TermQuery::new(term, option))
                                            as Box<dyn tantivy::query::Query + 'static>,
                                    )
                                })
                                .collect(),
                        )))
                    }
                }
                SimpleOrPhrase::Phrase(p) => {
                    let phrase = p.join(" ");
                    let mut processed_terms = process_tantivy_term(&phrase, *field, schema);

                    if processed_terms.is_empty() {
                        return None;
                    }

                    if processed_terms.len() == 1 {
                        let options = field.record_option();

                        Some(Box::new(tantivy::query::TermQuery::new(
                            processed_terms.pop().unwrap(),
                            options,
                        )) as Box<dyn tantivy::query::Query>)
                    } else {
                        Some(Box::new(tantivy::query::PhraseQuery::new(processed_terms))
                            as Box<dyn tantivy::query::Query>)
                    }
                }
            },
            Query::Boolean { clauses } => {
                let mut t_clauses = Vec::new();
                for (occur, query) in clauses {
                    if let Some(query) = query.as_tantivy(schema) {
                        t_clauses.push(((*occur).into(), query));
                    }
                }

                Some(Box::new(tantivy::query::BooleanQuery::new(t_clauses)))
            }
        }
    }
}

fn process_tantivy_term<T: TextField>(
    term: &str,
    field: T,
    schema: &tantivy::schema::Schema,
) -> Vec<tantivy::Term> {
    let mut terms: Vec<tantivy::Term> = Vec::new();
    let mut tokenizer = field.query_tokenizer();
    let mut token_stream = tokenizer.token_stream(term);
    let tantivy_field = field.tantivy_field(schema);

    token_stream.process(&mut |token| {
        let term = tantivy::Term::from_field_text(tantivy_field, &token.text);
        terms.push(term);
    });

    terms
}

impl Node {
    fn into_non_compacted_query(self) -> Query {
        match self {
            Node::Term(term) => Query::Term(term),
            Node::And(left, right) => Query::Boolean {
                clauses: vec![
                    (Occur::Must, left.into_non_compacted_query()),
                    (Occur::Must, right.into_non_compacted_query()),
                ],
            },
            Node::Or(left, right) => Query::Boolean {
                clauses: vec![
                    (Occur::Should, left.into_non_compacted_query()),
                    (Occur::Should, right.into_non_compacted_query()),
                ],
            },
            Node::Not(inner) => Query::Boolean {
                clauses: vec![(Occur::MustNot, inner.into_non_compacted_query())],
            },
        }
    }

    pub fn into_query(self) -> Query {
        self.into_non_compacted_query().compact().deduplicate()
    }

    fn from_term(term: super::Term) -> Self {
        match term {
            super::Term::SimpleOrPhrase(s) => match s {
                super::SimpleOrPhrase::Simple(term) => TextFieldEnum::all()
                    .filter(|f| f.is_searchable())
                    .map(|field| {
                        Node::Term(Term {
                            text: SimpleOrPhrase::Simple(term.clone()),
                            field,
                        })
                    })
                    .reduce(|left, right| left.or(right))
                    .expect("fields should not be empty"),
                super::SimpleOrPhrase::Phrase(p) => TextFieldEnum::all()
                    .filter(|f| f.is_searchable())
                    .filter(|f| f.is_phrase_searchable())
                    .map(|field| {
                        Node::Term(Term {
                            text: SimpleOrPhrase::Phrase(p.clone()),
                            field,
                        })
                    })
                    .reduce(|left, right| left.or(right))
                    .expect("fields should not be empty"),
            },
            super::Term::Site(s) => Node::Term(Term {
                text: SimpleOrPhrase::Simple(super::parser::SimpleTerm::from(s)),
                field: text_field::UrlForSiteOperator.into(),
            }),
            super::Term::Title(t) => Node::Term(Term {
                text: t,
                field: text_field::Title.into(),
            }),
            super::Term::Body(b) => Node::Term(Term {
                text: b,
                field: text_field::AllBody.into(),
            }),
            super::Term::Url(u) => Node::Term(Term {
                text: u,
                field: text_field::Url.into(),
            }),
            super::Term::PossibleBang { prefix, bang } => {
                let mut s = String::new();
                s.push(prefix);
                s.push_str(bang.as_str());

                let s = SimpleTerm::from(s);

                TextFieldEnum::all()
                    .filter(|f| f.is_searchable())
                    .map(|field| {
                        Node::Term(Term {
                            text: SimpleOrPhrase::Simple(s.clone()),
                            field,
                        })
                    })
                    .reduce(|left, right| left.or(right))
                    .expect("fields should not be empty")
            }
            super::Term::Not(n) => Node::Not(Box::new(Node::from_term(*n))),
        }
    }
}

fn sliding_window(window_size: usize, i: usize) -> impl Iterator<Item = (usize, usize)> {
    (0..=window_size)
        .map(move |offset| {
            let start = (i + offset).saturating_sub(window_size);
            let end = i + offset;

            (start, end)
        })
        .filter(|(start, end)| start < end)
        .filter(|(start, end)| end != start)
}

pub fn initial(terms: Vec<super::Term>) -> Option<Node> {
    let mut nodes = Vec::new();
    let terms_for_adjacent = terms.clone();

    for (i, term) in terms.into_iter().enumerate() {
        let mut adjacent = Vec::new();

        if let super::Term::SimpleOrPhrase(_) = &term {
            for window_size in 2..=3 {
                for (start, end) in sliding_window(window_size, i) {
                    let mut compounds = Vec::new();

                    for k in start..=end {
                        if let Some(super::Term::SimpleOrPhrase(super::SimpleOrPhrase::Simple(s))) =
                            terms_for_adjacent.get(k)
                        {
                            compounds.push(s.clone());
                        }
                    }

                    if !compounds.is_empty() {
                        adjacent.push(super::TermCompound { terms: compounds });
                    }
                }
            }
        }

        let node = Node::from_term(term);

        if !adjacent.is_empty() {
            match adjacent
                .into_iter()
                .flat_map(|compound| {
                    TextFieldEnum::all()
                        .filter(|f| f.is_searchable())
                        .filter(|f| f.is_compound_searchable())
                        .map(move |field| {
                            let compound_text: String = compound
                                .terms
                                .iter()
                                .map(|s| s.as_str().to_string())
                                .collect();

                            Node::Term(Term {
                                text: SimpleOrPhrase::Simple(SimpleTerm::from(compound_text)),
                                field,
                            })
                        })
                })
                .reduce(|left, right| left.or(right))
            {
                Some(adj) => nodes.push(node.or(adj)),
                None => nodes.push(node),
            }
        } else {
            nodes.push(node);
        }
    }

    nodes.into_iter().reduce(|left, right| left.and(right))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(query: &str, fields: &[TextFieldEnum]) -> Node {
        let terms = query
            .split_whitespace()
            .map(|s| SimpleTerm::from(s.to_string()))
            .collect::<Vec<_>>();

        let mut queries = vec![];

        for term in terms {
            let nodes: Vec<_> = fields
                .iter()
                .copied()
                .map(|f| {
                    Node::Term(Term {
                        text: SimpleOrPhrase::Simple(term.clone()),
                        field: f,
                    })
                })
                .collect();

            let term_q = if nodes.len() == 1 {
                nodes[0].clone()
            } else {
                nodes
                    .into_iter()
                    .reduce(|left, right| left.or(right))
                    .unwrap()
            };

            queries.push(term_q);
        }

        if queries.len() == 1 {
            queries[0].clone()
        } else {
            queries
                .into_iter()
                .reduce(|left, right| left.and(right))
                .unwrap()
        }
    }

    #[test]
    fn test_compact() {
        let query = Query::Boolean { clauses: vec![] };

        assert_eq!(query.clone().compact(), query);

        let query = parse(
            "foo bar",
            &[text_field::Title.into(), text_field::AllBody.into()],
        );

        let expected = Query::Boolean {
            clauses: vec![
                (
                    Occur::Must,
                    Query::Boolean {
                        clauses: vec![
                            (
                                Occur::Should,
                                Query::Term(Term {
                                    text: SimpleOrPhrase::Simple(SimpleTerm::from(
                                        "foo".to_string(),
                                    )),
                                    field: text_field::Title.into(),
                                }),
                            ),
                            (
                                Occur::Should,
                                Query::Term(Term {
                                    text: SimpleOrPhrase::Simple(SimpleTerm::from(
                                        "foo".to_string(),
                                    )),
                                    field: text_field::AllBody.into(),
                                }),
                            ),
                        ],
                    },
                ),
                (
                    Occur::Must,
                    Query::Boolean {
                        clauses: vec![
                            (
                                Occur::Should,
                                Query::Term(Term {
                                    text: SimpleOrPhrase::Simple(SimpleTerm::from(
                                        "bar".to_string(),
                                    )),
                                    field: text_field::Title.into(),
                                }),
                            ),
                            (
                                Occur::Should,
                                Query::Term(Term {
                                    text: SimpleOrPhrase::Simple(SimpleTerm::from(
                                        "bar".to_string(),
                                    )),
                                    field: text_field::AllBody.into(),
                                }),
                            ),
                        ],
                    },
                ),
            ],
        };

        assert_eq!(query.into_query().compact(), expected);
    }

    #[test]
    fn test_sliding_window() {
        let window_size = 3;
        let i = 3;

        let expected = vec![(0, 3), (1, 4), (2, 5), (3, 6)];

        assert_eq!(sliding_window(window_size, i).collect::<Vec<_>>(), expected);

        let window_size = 2;
        let i = 3;

        let expected = vec![(1, 3), (2, 4), (3, 5)];

        assert_eq!(sliding_window(window_size, i).collect::<Vec<_>>(), expected);

        let window_size = 2;
        let i = 0;

        let expected = vec![(0, 1), (0, 2)];

        assert_eq!(sliding_window(window_size, i).collect::<Vec<_>>(), expected);
    }
}
