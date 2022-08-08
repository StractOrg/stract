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

use logos::{Lexer, Logos};
use std::{borrow::Cow, collections::HashMap};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tag<'a> {
    name: Cow<'a, str>,
    raw: &'a str,
}

impl<'a> Tag<'a> {
    pub fn attributes(&self) -> HashMap<&'a str, &'a str> {
        let mut attributes = HashMap::new();

        for tok in self.raw.split_whitespace().skip(1) {
            if tok.contains('=') {
                if let Some((key, value)) = tok.split_once('=') {
                    attributes.insert(key.trim(), trim_html_stuff(value, false));
                }
            } else {
                if tok == ">" || tok.starts_with(self.name.as_ref()) || tok == "/>" {
                    continue;
                }

                attributes.insert(trim_html_stuff(tok, true), "");
            }
        }

        attributes
    }

    pub fn name(&self) -> &str {
        self.name.as_ref()
    }
}

fn trim_html_stuff(s: &str, break_on_dash: bool) -> &str {
    let mut start_val = 0;
    let mut end_val = s.len();

    for (idx, c) in s.char_indices() {
        start_val = idx;
        if c != ' ' && c != '"' {
            break;
        }
    }

    for (idx, c) in s[start_val..].char_indices() {
        end_val = start_val + idx;
        if break_on_dash && c == '/' {
            break;
        }

        if c == ' ' || c == '"' || c == '>' {
            break;
        }
    }

    &s[start_val..end_val]
}

fn start_tag<'a>(lex: &mut Lexer<'a, Token<'a>>) -> Option<Tag<'a>> {
    let slice = lex.slice();
    let name = name(slice);

    if name.chars().any(|c| c.is_uppercase()) {
        Some(Tag {
            name: Cow::Owned(name.to_ascii_lowercase()),
            raw: slice,
        })
    } else {
        Some(Tag {
            name: Cow::Borrowed(name),
            raw: slice,
        })
    }
}

fn end_tag<'a>(lex: &mut Lexer<'a, Token<'a>>) -> Option<Tag<'a>> {
    let slice = lex.slice();
    let name = name(slice);

    if name.chars().any(|c| c.is_uppercase()) {
        Some(Tag {
            name: Cow::Owned(name.to_ascii_lowercase()),
            raw: slice,
        })
    } else {
        Some(Tag {
            name: Cow::Borrowed(name),
            raw: slice,
        })
    }
}

fn name(s: &str) -> &str {
    let mut start = 0;

    for (idx, c) in s.char_indices() {
        start = idx;
        if !matches!(c, '<' | '/' | '\\' | ' ') {
            break;
        }
    }

    let mut end = start;
    for (idx, c) in s.char_indices().skip(start) {
        end = idx;

        if matches!(c, ' ' | '>' | '/') {
            break;
        }
    }

    s[start..end].trim()
}

#[derive(Clone, Logos, Debug, PartialEq, Eq)]
pub enum Token<'a> {
    #[regex(r"<[^>]+>", start_tag)]
    StartTag(Tag<'a>),

    #[regex(r"<\\?/[a-zA-Z]+>", end_tag)]
    EndTag(Tag<'a>),

    #[regex(r"<[^[/>]]+/>", start_tag)]
    SelfTerminatingTag(Tag<'a>),

    #[token("<!--")]
    BeginComment,

    #[token("-->")]
    EndComment,

    #[error]
    Error,
}

#[cfg(test)]
mod tests {
    use super::*;
    use maplit::hashmap;

    #[test]
    fn simple_link_tag() {
        let mut lex = Token::lexer("<a href=\"test\">");

        let tag = match lex.next().unwrap() {
            Token::StartTag(tag) => tag,
            _ => panic!(),
        };

        assert_eq!(tag.name(), "a");
        assert_eq!(tag.attributes(), hashmap!["href" => "test"]);
        assert_eq!(lex.next(), None);
    }

    #[test]
    fn tag_without_attributes() {
        let mut lex = Token::lexer("<a>");

        let tag = match lex.next().unwrap() {
            Token::StartTag(tag) => tag,
            _ => panic!(),
        };
        assert_eq!(tag.name(), "a");
        assert_eq!(tag.attributes(), hashmap![]);
        assert_eq!(lex.next(), None);

        let mut lex = Token::lexer("<  a>");

        let tag = match lex.next().unwrap() {
            Token::StartTag(tag) => tag,
            _ => panic!(),
        };
        assert_eq!(tag.name(), "a");
        assert_eq!(tag.attributes(), hashmap![]);
        assert_eq!(lex.next(), None);

        let mut lex = Token::lexer("<  a   >");

        let tag = match lex.next().unwrap() {
            Token::StartTag(tag) => tag,
            _ => panic!(),
        };
        assert_eq!(tag.name(), "a");
        assert_eq!(tag.attributes(), hashmap![]);
        assert_eq!(lex.next(), None);

        let mut lex = Token::lexer("<a   >");

        let tag = match lex.next().unwrap() {
            Token::StartTag(tag) => tag,
            _ => panic!(),
        };
        assert_eq!(tag.name(), "a");
        assert_eq!(tag.attributes(), hashmap![]);
        assert_eq!(lex.next(), None);
    }

    #[test]
    fn key_without_value() {
        let mut lex = Token::lexer("<input autofocus>");

        let tag = match lex.next().unwrap() {
            Token::StartTag(tag) => tag,
            _ => panic!(),
        };
        assert_eq!(tag.name(), "input");
        assert_eq!(tag.attributes(), hashmap!["autofocus" => ""]);
        assert_eq!(lex.next(), None);

        let mut lex = Token::lexer("<input autofocus/>");

        let tag = match lex.next().unwrap() {
            Token::SelfTerminatingTag(tag) => tag,
            _ => panic!(),
        };
        assert_eq!(tag.name(), "input");
        assert_eq!(tag.attributes(), hashmap!["autofocus" => ""]);
        assert_eq!(lex.next(), None);
    }

    #[test]
    fn self_terminating_tag() {
        let mut lex = Token::lexer("<a href=\"test\" />");

        let tag = match lex.next().unwrap() {
            Token::SelfTerminatingTag(tag) => tag,
            _ => panic!(),
        };
        assert_eq!(tag.name(), "a");
        assert_eq!(tag.attributes(), hashmap!["href" => "test"]);
        assert_eq!(lex.next(), None);
    }

    #[test]
    fn end_tag() {
        let mut lex = Token::lexer("<a href=\"test\">this is some text</a>");

        let tag = match lex.next().unwrap() {
            Token::StartTag(tag) => tag,
            _ => panic!(),
        };
        assert_eq!(tag.name(), "a");
        assert_eq!(tag.attributes(), hashmap!["href" => "test"]);

        let mut tok = lex.next();

        while let Some(Token::Error) = tok {
            tok = lex.next();
        }

        let tag = match tok.unwrap() {
            Token::EndTag(tag) => tag,
            _ => panic!(),
        };
        assert_eq!(tag.name(), "a");
        assert_eq!(lex.next(), None);
    }
}
