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

fn addr_of(s: &str) -> usize {
    s.as_ptr() as usize
}

fn split_whitespace_indices(s: &str) -> impl Iterator<Item = (usize, &str)> {
    s.split_whitespace()
        .map(move |sub| (addr_of(sub) - addr_of(s), sub))
}

impl<'a> Tag<'a> {
    pub fn attributes(&self) -> HashMap<&'a str, &'a str> {
        let mut attributes = HashMap::new();

        let mut long_attribute: Option<usize> = None;
        let mut is_first_token = true;
        for (start_idx, tok) in split_whitespace_indices(self.raw) {
            let num_quotes = tok.chars().filter(|c| matches!(*c, '"' | '\'')).count();
            if (tok.contains('>') && num_quotes == 0 && is_first_token)
                || tok.contains('<')
                || tok == "/>"
                || tok.starts_with(self.name.as_ref())
            {
                is_first_token = false;
                continue;
            }
            is_first_token = false;

            if num_quotes == 0 {
                if long_attribute.is_none() {
                    // attribute without value (e.g. autofocus)
                    let to_insert = trim_html_stuff(tok, true);
                    if !to_insert.is_empty() {
                        attributes.insert(to_insert, "");
                    }
                }
            } else if num_quotes == 1 {
                if let Some(attribute_start_idx) = long_attribute {
                    // closing attribute
                    let tok = &self.raw[attribute_start_idx..start_idx + tok.len()];
                    if tok.contains('=') {
                        if let Some((key, value)) = tok.split_once('=') {
                            attributes.insert(key.trim(), trim_html_stuff(value, false));
                        }
                    }
                    long_attribute = None;
                } else {
                    // opening attribute
                    long_attribute = Some(start_idx);
                }
            } else if num_quotes == 2 {
                // self-containing attribute
                if tok.contains('=') {
                    if let Some((key, value)) = tok.split_once('=') {
                        attributes.insert(key.trim(), trim_html_stuff(value, false));
                    }
                }
            }
        }

        attributes
    }

    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    pub fn raw(&self) -> &str {
        self.raw
    }
}

fn trim_html_stuff(s: &str, break_on_dash: bool) -> &str {
    let mut start_val = 0;
    let mut end_val = s.len();

    for (idx, c) in s.char_indices() {
        start_val = idx;
        if !matches!(c, ' ' | '"' | '\'') {
            break;
        }
    }

    for (idx, c) in s[start_val..].char_indices() {
        end_val = start_val + idx;
        if break_on_dash && c == '/' {
            break;
        }

        if matches!(c, '>' | '"' | '\'') {
            break;
        }
    }

    &s[start_val..end_val]
}

fn start_tag<'a>(lex: &mut Lexer<'a, Token<'a>>) -> Option<Tag<'a>> {
    let slice = lex.slice();
    let name = name(slice);

    let name = if name.chars().any(|c| c.is_uppercase()) {
        Cow::Owned(name.to_ascii_lowercase())
    } else {
        Cow::Borrowed(name)
    };

    Some(Tag { name, raw: slice })
}

fn end_tag<'a>(lex: &mut Lexer<'a, Token<'a>>) -> Option<Tag<'a>> {
    let slice = lex.slice();
    let name = name(slice);

    let name = if name.chars().any(|c| c.is_uppercase()) {
        Cow::Owned(name.to_ascii_lowercase())
    } else {
        Cow::Borrowed(name)
    };

    Some(Tag { name, raw: slice })
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
    #[regex(r"<[^><]+[^/]?>", start_tag, priority = 0)]
    StartTag(Tag<'a>),

    #[regex(r"<?\\?/[a-zA-Z]+>", end_tag, priority = 1)]
    EndTag(Tag<'a>),

    // #[regex(r"<[^[/>]]+/>", start_tag)]
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
            whut => panic!("{:?}", whut),
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
            whut => panic!("{:?}", whut),
        };
        assert_eq!(tag.name(), "a");
        assert_eq!(tag.attributes(), hashmap![]);
        assert_eq!(lex.next(), None);

        let mut lex = Token::lexer("<  a>");

        let tag = match lex.next().unwrap() {
            Token::StartTag(tag) => tag,
            whut => panic!("{:?}", whut),
        };
        assert_eq!(tag.name(), "a");
        assert_eq!(tag.attributes(), hashmap![]);
        assert_eq!(lex.next(), None);

        let mut lex = Token::lexer("<  a   >");

        let tag = match lex.next().unwrap() {
            Token::StartTag(tag) => tag,
            whut => panic!("{:?}", whut),
        };
        assert_eq!(tag.name(), "a");
        assert_eq!(tag.attributes(), hashmap![]);
        assert_eq!(lex.next(), None);

        let mut lex = Token::lexer("<a   >");

        let tag = match lex.next().unwrap() {
            Token::StartTag(tag) => tag,
            whut => panic!("{:?}", whut),
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
            whut => panic!("{:?}", whut),
        };
        assert_eq!(tag.name(), "input");
        assert_eq!(tag.attributes(), hashmap!["autofocus" => ""]);
        assert_eq!(lex.next(), None);

        let mut lex = Token::lexer("<input autofocus/>");

        let tag = match lex.next().unwrap() {
            Token::SelfTerminatingTag(tag) => tag,
            whut => panic!("{:?}", whut),
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
            whut => panic!("{:?}", whut),
        };
        assert_eq!(tag.name(), "a");
        assert_eq!(tag.attributes(), hashmap!["href" => "test"]);
        assert_eq!(lex.next(), None);
    }

    #[test]
    fn end_tag() {
        let raw = r#"<a href="test">this is some text</a>"#;
        let mut lex = Token::lexer(raw);

        let tag = match lex.next().unwrap() {
            Token::StartTag(tag) => tag,
            whut => {
                panic!("{:?}", whut)
            }
        };
        assert_eq!(tag.name(), "a");
        assert_eq!(tag.attributes(), hashmap!["href" => "test"]);

        let mut tok = lex.next();

        while let Some(Token::Error) = tok {
            tok = lex.next();
        }

        let tag = match tok.unwrap() {
            Token::EndTag(tag) => tag,
            whut => panic!("{:?}", whut),
        };
        assert_eq!(tag.name(), "a");
        assert_eq!(lex.next(), None);
    }

    #[test]
    fn test_split_whitespace_indices() {
        let mut iter = split_whitespace_indices(" Hello world");

        assert_eq!(Some((1, "Hello")), iter.next());
        assert_eq!(Some((7, "world")), iter.next());
    }

    #[test]
    fn attribute_containing_spaces() {
        let mut lex = Token::lexer("<meta description=\"this is a long attribute\">");

        let tag = match lex.next().unwrap() {
            Token::StartTag(tag) => tag,
            _ => panic!(),
        };

        assert_eq!(
            tag.attributes(),
            hashmap!["description" => "this is a long attribute"]
        );
        assert_eq!(lex.next(), None);
    }

    #[test]
    fn script_with_lt() {
        let raw = "<script>if d < a</script>";
        let mut lex = Token::lexer(raw);

        let tag = match lex.next().unwrap() {
            Token::StartTag(tag) => tag,
            whut => panic!("{:?}", whut),
        };

        assert_eq!(tag.name(), "script");

        let mut text = String::new();

        let mut tok = lex.next();
        while let Some(Token::Error) = tok {
            text.push_str(&raw[lex.span()]);
            tok = lex.next();
        }

        assert_eq!(text.as_str(), "if d < a");

        assert!(matches!(tok.unwrap(), Token::EndTag(_)));
        assert_eq!(lex.next(), None);
    }

    #[test]
    fn link_attributes() {
        let raw = "<link href='//securepubads.g.doubleclick.net' rel='preconnect'>";

        let mut lex = Token::lexer(raw);

        let tag = match lex.next().unwrap() {
            Token::StartTag(tag) => tag,
            whut => panic!("{:?}", whut),
        };

        assert_eq!(
            tag.attributes().get("href"),
            Some(&"//securepubads.g.doubleclick.net")
        );
    }
}
