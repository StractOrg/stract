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

use std::collections::{BTreeMap, HashSet};

use parse_wiki_text::Node;
use serde::{Deserialize, Serialize};

use crate::webpage::Url;

#[derive(Debug)]
pub struct Paragraph {
    pub title: Option<String>,
    pub content: Span,
}

#[derive(Debug)]
pub struct Entity {
    pub title: String,
    pub page_abstract: Span,
    pub info: BTreeMap<String, Span>,
    pub image: Option<Url>,
    pub paragraphs: Vec<Paragraph>,
    pub categories: HashSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Span {
    pub text: String,
    pub links: Vec<Link>,
}

impl Span {
    pub fn merge(&mut self, other_span: Span) {
        let orig_end = self.text.len();
        self.text.push_str(&other_span.text);

        for link in other_span.links {
            self.links.push(Link {
                start: orig_end + link.start,
                end: orig_end + link.end,
                target: link.target,
            });
        }
    }

    pub fn add_link(&mut self, text: &str, link: Link) {
        debug_assert_eq!(self.text.chars().count() + text.chars().count(), link.end);
        debug_assert_eq!(self.text.chars().count(), link.start);
        self.links.push(link);
        self.text.push_str(text);
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct Link {
    pub start: usize,
    pub end: usize,
    pub target: String,
}

impl<'a> From<Vec<Node<'a>>> for Span {
    fn from(nodes: Vec<Node<'a>>) -> Self {
        let mut span = Span {
            text: String::new(),
            links: Vec::new(),
        };

        for node in nodes {
            match node {
                Node::Link {
                    end: _,
                    start: _,
                    target,
                    text,
                } => {
                    let text: String = itertools::intersperse(
                        text.into_iter().filter_map(|node| match node {
                            Node::Text {
                                end: _,
                                start: _,
                                value,
                            } => Some(value),
                            _ => None,
                        }),
                        "",
                    )
                    .collect();

                    let link = Link {
                        target: target.to_string(),
                        start: span.text.chars().count(),
                        end: span.text.chars().count() + text.chars().count(),
                    };
                    span.add_link(&text, link);
                }
                Node::Text {
                    end: _,
                    start: _,
                    value,
                } => {
                    if value == "\n" {
                        if !span.text.is_empty() {
                            span.text.push_str(". ");
                        }
                        continue;
                    }

                    span.text.push_str(value);
                }
                Node::Template {
                    end: _,
                    name: _,
                    parameters,
                    start: _,
                } if span.text.is_empty() => {
                    for other_span in parameters
                        .into_iter()
                        .filter(|parameter| parameter.name.is_none())
                        .map(|parameter| Span::from(parameter.value))
                    {
                        span.merge(other_span);
                        span.text.push(' ');
                    }
                    span.text = span.text.trim_end().to_string();
                }
                _ => {}
            }
        }

        span
    }
}
