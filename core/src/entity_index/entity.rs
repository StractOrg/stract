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

use std::collections::{BTreeMap, HashSet};

use itertools::Itertools;
use parse_wiki_text::{Node, Parameter};
use serde::{Deserialize, Serialize};

pub trait WikiNodeExt<'a> {
    fn as_text(&self) -> Option<&'a str>;
    fn as_category_target(&self) -> Option<&'a str>;
    fn as_template(&self) -> Option<(&Vec<Node<'a>>, &Vec<Parameter<'a>>)>;
}
impl<'a> WikiNodeExt<'a> for Node<'a> {
    fn as_text(&self) -> Option<&'a str> {
        match self {
            Node::Text { value, .. } => Some(value),
            _ => None,
        }
    }
    fn as_category_target(&self) -> Option<&'a str> {
        match self {
            Node::Category { target, .. } => Some(target),
            _ => None,
        }
    }
    fn as_template(&self) -> Option<(&Vec<Node<'a>>, &Vec<Parameter<'a>>)> {
        match self {
            Node::Template {
                name, parameters, ..
            } => Some((name, parameters)),
            _ => None,
        }
    }
}

#[derive(Debug, Default)]
pub struct Paragraph {
    pub title: Option<String>,
    pub content: Span,
}

#[derive(Debug)]
pub struct Entity {
    pub title: String,
    pub page_abstract: Span,
    pub info: BTreeMap<String, Span>,
    pub image: Option<String>,
    pub paragraphs: Vec<Paragraph>,
    pub categories: HashSet<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

    pub fn add_link(&mut self, text: &str, target: String) {
        let link = Link {
            target,
            start: self.text.len(),
            end: self.text.len() + text.len(),
        };
        self.links.push(link);
        self.text.push_str(text);
    }

    pub fn add_node(&mut self, node: &Node) {
        match node {
            Node::Link { target, text, .. } => {
                let text = text.iter().filter_map(|node| node.as_text()).join("");
                self.add_link(&text, target.to_string());
            }
            Node::Text { value, .. } => {
                if *value == "\n" {
                    if !self.text.chars().all(|c| c.is_whitespace()) {
                        self.text.push_str(". ");
                    }
                } else {
                    self.text.push_str(value);
                }
            }
            Node::Template { parameters, .. } if self.text.is_empty() => {
                for other_span in parameters
                    .iter()
                    .filter(|parameter| parameter.name.is_none())
                    .map(|parameter| Span::from(&parameter.value[..]))
                {
                    self.merge(other_span);
                    self.text.push(' ');
                }
                self.text = self.text.trim_end().to_string();
            }
            Node::ParagraphBreak { .. } => {
                self.text.push('\n');
            }
            _ => {}
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct Link {
    pub start: usize,
    pub end: usize,
    pub target: String,
}

impl<'a> From<&[Node<'a>]> for Span {
    fn from(nodes: &[Node<'a>]) -> Self {
        let mut span = Span::default();
        for node in nodes {
            span.add_node(node);
        }
        span
    }
}
