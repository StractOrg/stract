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
// along with this program.  If not, see <https://www.gnu.org/licenses/>

use selectors::Element;

use crate::{iter::NodeIterator, ElementData, NodeDataRef};

#[derive(Debug, PartialEq)]
pub enum Predicate {
    Nth(usize),
    Contains { key: ContainsKey, value: String },
}

#[derive(Debug, PartialEq)]
pub enum ContainsKey {
    Text,
    Attr(String),
}

impl Predicate {
    pub fn parse(s: &str) -> Option<Self> {
        if let Ok(num) = s.parse::<usize>() {
            if num > 0 {
                return Some(Predicate::Nth(num - 1));
            }
        }

        if s.starts_with("contains(text(),") {
            let value = s
                .trim_start_matches("contains(text(),")
                .trim_end_matches(')');
            return Some(Predicate::Contains {
                key: ContainsKey::Text,
                value: value.to_string(),
            });
        }

        if s.starts_with("contains(@") {
            let attr_val = s.trim_start_matches("contains(@").trim_end_matches(')');

            let mut parts = attr_val.split(',');

            if let (Some(attr), Some(value)) = (parts.next(), parts.next()) {
                return Some(Predicate::Contains {
                    key: ContainsKey::Attr(attr.to_string()),
                    value: value
                        .trim()
                        .trim_start_matches(|c| matches!(c, '\'' | '"'))
                        .trim_end_matches(|c| matches!(c, '\'' | '"'))
                        .to_string(),
                });
            }
        }

        None
    }

    pub fn matches(&self, elem: &NodeDataRef<ElementData>) -> bool {
        match self {
            Predicate::Nth(nth) => {
                let parent = elem.parent_element().unwrap();
                parent
                    .as_node()
                    .children()
                    .elements()
                    .nth(*nth)
                    .map(|child| &child == elem)
                    .unwrap_or(false)
            }

            Predicate::Contains { key, value } => match key {
                ContainsKey::Text => elem.text_contents().contains(value),
                ContainsKey::Attr(attr) => elem
                    .attributes
                    .borrow()
                    .get(attr.as_str())
                    .is_some_and(|v| v.split_ascii_whitespace().any(|v| v == value)),
            },
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Expr {
    Root,
    Wildcard,
    Child {
        name: String,
        pred: Option<Predicate>,
    },
}

impl Expr {
    pub fn parse(s: &str) -> Vec<Self> {
        let mut tokens = Vec::new();
        let mut current = String::new();
        let mut in_brackets = false;

        for c in s.chars() {
            match c {
                '/' if !in_brackets => {
                    if !current.is_empty() {
                        tokens.push(current);
                        current = String::new();
                    }
                    tokens.push("/".to_string());
                }
                '[' => {
                    in_brackets = true;
                    tokens.push(current);
                    current = String::new();
                    current.push('[');
                }
                ']' => {
                    in_brackets = false;
                    current.push(']');
                    tokens.push(current);
                    current = String::new();
                }
                _ => current.push(c),
            }
        }

        if !current.is_empty() {
            tokens.push(current);
        }

        let mut result = Vec::new();
        let mut is_root = true;

        let mut i = 0;
        while i < tokens.len() {
            match tokens[i].as_str() {
                "/" => {
                    if is_root {
                        result.push(Expr::Root);
                        is_root = false;
                    }
                }
                _ => {
                    let name = tokens[i].to_string();

                    if name == "." || name == "*" {
                        result.push(Expr::Wildcard);
                        i += 1;
                        is_root = false;
                        continue;
                    }

                    let mut pred = None;

                    if i + 1 < tokens.len()
                        && tokens[i + 1].starts_with('[')
                        && tokens[i + 1].ends_with(']')
                    {
                        if let Some(p) =
                            Predicate::parse(&tokens[i + 1][1..tokens[i + 1].len() - 1])
                        {
                            pred = Some(p);
                            i += 1;
                        }
                    }

                    result.push(Expr::Child {
                        name: name.trim_start_matches('/').to_string(),
                        pred,
                    });
                }
            }
            i += 1;
        }

        result
    }

    pub fn matches(&self, elem: &NodeDataRef<ElementData>) -> bool {
        match self {
            Expr::Root => elem.parent_element().is_none(),
            Expr::Child { name, pred } => {
                if elem.name.local.as_ref() != name {
                    return false;
                }

                if let Some(pred) = pred {
                    pred.matches(elem)
                } else {
                    true
                }
            }
            Expr::Wildcard => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expr_parse() {
        assert_eq!(Expr::parse("/"), vec![Expr::Root]);
        assert_eq!(
            Expr::parse("/html"),
            vec![
                Expr::Root,
                Expr::Child {
                    name: "html".to_string(),
                    pred: None
                }
            ]
        );
        assert_eq!(
            Expr::parse("/html[1]"),
            vec![
                Expr::Root,
                Expr::Child {
                    name: "html".to_string(),
                    pred: Some(Predicate::Nth(0))
                }
            ]
        );
        assert_eq!(
            Expr::parse("/html[1]/body"),
            vec![
                Expr::Root,
                Expr::Child {
                    name: "html".to_string(),
                    pred: Some(Predicate::Nth(0))
                },
                Expr::Child {
                    name: "body".to_string(),
                    pred: None
                }
            ]
        );
        assert_eq!(
            Expr::parse("/html[1]/body[1]"),
            vec![
                Expr::Root,
                Expr::Child {
                    name: "html".to_string(),
                    pred: Some(Predicate::Nth(0))
                },
                Expr::Child {
                    name: "body".to_string(),
                    pred: Some(Predicate::Nth(0))
                }
            ]
        );
        assert_eq!(
            Expr::parse("/html[1]/body[1]"),
            vec![
                Expr::Root,
                Expr::Child {
                    name: "html".to_string(),
                    pred: Some(Predicate::Nth(0))
                },
                Expr::Child {
                    name: "body".to_string(),
                    pred: Some(Predicate::Nth(0))
                },
            ]
        );

        assert_eq!(
            Expr::parse("/html/body/div[4]"),
            vec![
                Expr::Root,
                Expr::Child {
                    name: "html".to_string(),
                    pred: None
                },
                Expr::Child {
                    name: "body".to_string(),
                    pred: None
                },
                Expr::Child {
                    name: "div".to_string(),
                    pred: Some(Predicate::Nth(3))
                }
            ]
        );
    }
}
