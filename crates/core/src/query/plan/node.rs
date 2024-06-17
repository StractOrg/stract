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

use ahash::HashSetExt;

use crate::query::parser::{SimpleOrPhrase, Term as ParserTerm};
use crate::schema::text_field::{self, TextField as _};
use crate::{query::parser::SimpleTerm, schema::TextFieldEnum};

type HashSet<T> = std::collections::HashSet<T, ahash::RandomState>;

use super::{Occur, Term};

#[derive(Debug, Clone)]
pub enum Node {
    Term(Term),
    And(Box<Node>, Box<Node>),
    Or(Box<Node>, Box<Node>),
    Not(Box<Node>),
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Node::Term(a), Node::Term(b)) => a == b,
            (Node::And(a, b), Node::And(c, d)) => (a == c && b == d) || (a == d && b == c),
            (Node::Or(a, b), Node::Or(c, d)) => (a == c && b == d) || (a == d && b == c),
            (Node::Not(a), Node::Not(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Node {}

impl std::hash::Hash for Node {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Node::Term(term) => term.hash(state),
            Node::And(left, right) => {
                left.hash(state);
                right.hash(state);
            }
            Node::Or(left, right) => {
                left.hash(state);
                right.hash(state);
            }
            Node::Not(inner) => inner.hash(state),
        }
    }
}

impl Node {
    pub fn and<T: Into<Node>>(self, other: T) -> Node {
        Node::And(Box::new(self), Box::new(other.into()))
    }

    pub fn or<T: Into<Node>>(self, other: T) -> Node {
        Node::Or(Box::new(self), Box::new(other.into()))
    }
}

impl Node {
    fn into_non_compacted_query(self) -> super::Query {
        match self {
            Node::Term(term) => super::Query::Term(term),
            Node::And(left, right) => super::Query::Boolean {
                clauses: vec![
                    (Occur::Must, left.into_non_compacted_query()),
                    (Occur::Must, right.into_non_compacted_query()),
                ],
            },
            Node::Or(left, right) => super::Query::Boolean {
                clauses: vec![
                    (Occur::Should, left.into_non_compacted_query()),
                    (Occur::Should, right.into_non_compacted_query()),
                ],
            },
            Node::Not(inner) => super::Query::Boolean {
                clauses: vec![(Occur::MustNot, inner.into_non_compacted_query())],
            },
        }
    }

    pub fn into_query(self) -> super::Query {
        self.optimise()
            .into_non_compacted_query()
            .compact()
            .deduplicate()
    }

    pub fn from_term(term: ParserTerm) -> Self {
        match term {
            ParserTerm::SimpleOrPhrase(s) => match s {
                SimpleOrPhrase::Simple(term) => TextFieldEnum::all()
                    .filter(|f| f.is_searchable())
                    .map(|field| {
                        Node::Term(Term {
                            text: SimpleOrPhrase::Simple(term.clone()),
                            field,
                        })
                    })
                    .reduce(|left, right| left.or(right))
                    .expect("fields should not be empty"),
                SimpleOrPhrase::Phrase(p) => TextFieldEnum::all()
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
            ParserTerm::Site(s) => Node::Term(Term {
                text: SimpleOrPhrase::Simple(SimpleTerm::from(s)),
                field: text_field::UrlForSiteOperator.into(),
            }),
            ParserTerm::LinksTo(s) => Node::Term(Term {
                text: SimpleOrPhrase::Simple(SimpleTerm::from(s)),
                field: text_field::Links.into(),
            }),
            ParserTerm::Title(t) => Node::Term(Term {
                text: t,
                field: text_field::Title.into(),
            }),
            ParserTerm::Body(b) => Node::Term(Term {
                text: b,
                field: text_field::AllBody.into(),
            }),
            ParserTerm::Url(u) => Node::Term(Term {
                text: u,
                field: text_field::Url.into(),
            }),
            ParserTerm::PossibleBang { prefix, bang } => {
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
            ParserTerm::Not(n) => Node::Not(Box::new(Node::from_term(*n))),
        }
    }

    fn or_children(self) -> HashSet<Node> {
        match self {
            Node::Or(left, right) => {
                let mut left = left.or_children();
                let right = right.or_children();

                left.extend(right);
                left
            }
            _ => {
                let mut set = HashSet::new();
                set.insert(self);
                set
            }
        }
    }

    fn prune_direct_or_child(self, child: &Self) -> Self {
        match self {
            Node::Or(a, b) if *a == *child => *b,
            Node::Or(a, b) if *b == *child => *a,
            Node::Or(a, b) => Node::Or(
                Box::new(a.prune_direct_or_child(child)),
                Box::new(b.prune_direct_or_child(child)),
            ),
            t => t,
        }
    }

    pub fn optimise(self) -> Self {
        let mut node = self;
        node = Deduplicate.optimise(node);
        DistributiveLaw.optimise(node)
    }
}

trait Optimisation {
    fn optimise(&self, node: Node) -> Node;
}

/// Re-write queries on the form `(A | B) & (A | C)` to `A | (B & C)`.
/// This avoids having to evaluate the same term multiple times.
struct DistributiveLaw;

impl Optimisation for DistributiveLaw {
    fn optimise(&self, node: Node) -> Node {
        match node {
            Node::Term(term) => Node::Term(term),
            Node::Not(inner) => Node::Not(Box::new(self.optimise(*inner))),
            Node::Or(left, right) => Node::Or(
                Box::new(self.optimise(*left)),
                Box::new(self.optimise(*right)),
            ),
            Node::And(left, right) => {
                let left = self.optimise(*left);
                let right = self.optimise(*right);

                match (left, right) {
                    (Node::Or(left_left, left_right), Node::Or(right_left, right_right)) => {
                        let left_children =
                            Node::Or(left_left.clone(), left_right.clone()).or_children();
                        let right_children =
                            Node::Or(right_left.clone(), right_right.clone()).or_children();

                        let common: Vec<_> = left_children
                            .intersection(&right_children)
                            .cloned()
                            .collect();

                        if common.is_empty() {
                            Node::And(
                                Box::new(Node::Or(left_left, left_right)),
                                Box::new(Node::Or(right_left, right_right)),
                            )
                        } else {
                            let mut left = Node::Or(left_left, left_right);
                            let mut right = Node::Or(right_left, right_right);

                            for c in &common {
                                left = left.prune_direct_or_child(c);
                                right = right.prune_direct_or_child(c);
                            }

                            let res = Node::And(Box::new(left), Box::new(right));

                            let common = common
                                .into_iter()
                                .reduce(|left, right| left.or(right))
                                .unwrap();

                            res.or(common)
                        }
                    }

                    (left, right) => Node::And(Box::new(left), Box::new(right)),
                }
            }
        }
    }
}

struct Deduplicate;

impl Optimisation for Deduplicate {
    fn optimise(&self, node: Node) -> Node {
        match node {
            Node::Term(term) => Node::Term(term),
            Node::Not(inner) => Node::Not(Box::new(self.optimise(*inner))),
            Node::Or(left, right) => {
                let left = self.optimise(*left);
                let right = self.optimise(*right);

                if left == right {
                    left
                } else {
                    Node::Or(Box::new(left), Box::new(right))
                }
            }
            Node::And(left, right) => {
                let left = self.optimise(*left);
                let right = self.optimise(*right);

                if left == right {
                    left
                } else {
                    Node::And(Box::new(left), Box::new(right))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optimisation() {
        let a = Node::Term(Term {
            text: SimpleOrPhrase::Simple(SimpleTerm::from("a".to_string())),
            field: text_field::Title.into(),
        });

        let b = Node::Term(Term {
            text: SimpleOrPhrase::Simple(SimpleTerm::from("b".to_string())),
            field: text_field::Title.into(),
        });

        let c = Node::Term(Term {
            text: SimpleOrPhrase::Simple(SimpleTerm::from("c".to_string())),
            field: text_field::Title.into(),
        });

        let d = Node::Term(Term {
            text: SimpleOrPhrase::Simple(SimpleTerm::from("d".to_string())),
            field: text_field::Title.into(),
        });

        let e = Node::Term(Term {
            text: SimpleOrPhrase::Simple(SimpleTerm::from("e".to_string())),
            field: text_field::Title.into(),
        });

        let f = Node::Term(Term {
            text: SimpleOrPhrase::Simple(SimpleTerm::from("f".to_string())),
            field: text_field::Title.into(),
        });

        let query = a.clone().or(b.clone()).and(a.clone().or(c.clone()));

        let optimised = query.optimise();

        assert_eq!(optimised, a.clone().or(b.clone().and(c.clone())));

        let query = a.clone().or(b.clone()).and(c.clone().or(d.clone()));

        let optimised = query.optimise();

        assert_eq!(
            optimised,
            a.clone().or(b.clone()).and(c.clone().or(d.clone()))
        );

        let query = (a.clone().or(b.clone()).or(c.clone()).or(d.clone()))
            .and(e.clone().or(f.clone()).or(c.clone()).or(d.clone()));
        let optimised = query.optimise();

        assert_eq!(
            optimised,
            ((a.clone().or(b.clone())).and(e.clone().or(f.clone()))).or(c.clone().or(d.clone()))
        );

        let query = (a.clone().or(a.clone()).or(a.clone()).or(a.clone()))
            .and(a.clone().or(a.clone()).or(a.clone()).or(a.clone()));

        let optimised = query.optimise();

        assert_eq!(optimised, a.clone());

        let query = (a.clone().or(b.clone())).and(a.clone().or(c.clone().and(a.clone())));

        let optimised = query.optimise();

        assert_eq!(
            optimised,
            a.clone().or(b.clone().and(c.clone().and(a.clone())))
        );
    }
}
