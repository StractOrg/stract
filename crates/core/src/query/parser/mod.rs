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

use crate::{bangs::BANG_PREFIXES, floor_char_boundary};

mod as_tantivy;
mod term;

pub use term::*;

fn parse_term(term: &str) -> Box<Term> {
    // TODO: re-write this entire function once if-let chains become stable
    if let Some(not_term) = term.strip_prefix('-') {
        if !not_term.is_empty() && !not_term.starts_with('-') {
            Box::new(Term::Not(parse_term(not_term)))
        } else {
            Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Simple(
                term.to_string().into(),
            )))
        }
    } else if let Some(site) = term.strip_prefix("site:") {
        if !site.is_empty() {
            Box::new(Term::Site(site.to_string()))
        } else {
            Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Simple(
                term.to_string().into(),
            )))
        }
    } else if let Some(title) = term.strip_prefix("intitle:") {
        if !title.is_empty() {
            Box::new(Term::Title(SimpleOrPhrase::Simple(
                title.to_string().into(),
            )))
        } else {
            Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Simple(
                term.to_string().into(),
            )))
        }
    } else if let Some(body) = term.strip_prefix("inbody:") {
        if !body.is_empty() {
            Box::new(Term::Body(SimpleOrPhrase::Simple(body.to_string().into())))
        } else {
            Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Simple(
                term.to_string().into(),
            )))
        }
    } else if let Some(url) = term.strip_prefix("inurl:") {
        if !url.is_empty() {
            Box::new(Term::Url(SimpleOrPhrase::Simple(url.to_string().into())))
        } else {
            Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Simple(
                term.to_string().into(),
            )))
        }
    } else {
        for bang_prefix in BANG_PREFIXES {
            if let Some(bang) = term.strip_prefix(bang_prefix) {
                return Box::new(Term::PossibleBang(bang.to_string()));
            }
        }

        Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Simple(
            term.to_string().into(),
        )))
    }
}

#[allow(clippy::vec_box)]
pub fn parse(query: &str) -> Vec<Box<Term>> {
    let query = query.to_lowercase().replace(['“', '”'], "\"");

    let mut res = Vec::new();

    let mut cur_term_begin = 0;

    for (offset, c) in query.char_indices() {
        if cur_term_begin > offset {
            continue;
        }

        cur_term_begin = floor_char_boundary(&query, cur_term_begin);

        if query[cur_term_begin..].starts_with('"') {
            if let Some(offset) = query[cur_term_begin + 1..].find('"') {
                let offset = offset + cur_term_begin + 1;
                res.push(Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Phrase(
                    query[cur_term_begin + 1..offset]
                        .split_whitespace()
                        .map(|s| s.to_string())
                        .collect(),
                ))));

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
        res.push(parse_term(
            &query[floor_char_boundary(&query, cur_term_begin)..query.len()],
        ));
    }

    res
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn parse_not() {
        assert_eq!(
            parse("this -that"),
            vec![
                Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Simple(
                    "this".to_string().into()
                ))),
                Box::new(Term::Not(Box::new(Term::SimpleOrPhrase(
                    SimpleOrPhrase::Simple("that".to_string().into())
                ))))
            ]
        );

        assert_eq!(
            parse("this -"),
            vec![
                Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Simple(
                    "this".to_string().into()
                ))),
                Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Simple(
                    "-".to_string().into()
                )))
            ]
        );
    }

    #[test]
    fn double_not() {
        assert_eq!(
            parse("this --that"),
            vec![
                Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Simple(
                    "this".to_string().into()
                ))),
                Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Simple(
                    "--that".to_string().into()
                )))
            ]
        );
    }

    #[test]
    fn site() {
        assert_eq!(
            parse("this site:test.com"),
            vec![
                Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Simple(
                    "this".to_string().into()
                ))),
                Box::new(Term::Site("test.com".to_string()))
            ]
        );
    }

    #[test]
    fn title() {
        assert_eq!(
            parse("this intitle:test"),
            vec![
                Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Simple(
                    "this".to_string().into()
                ))),
                Box::new(Term::Title(SimpleOrPhrase::Simple(
                    "test".to_string().into()
                )))
            ]
        );
    }

    #[test]
    fn body() {
        assert_eq!(
            parse("this inbody:test"),
            vec![
                Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Simple(
                    "this".to_string().into()
                ))),
                Box::new(Term::Body(SimpleOrPhrase::Simple(
                    "test".to_string().into()
                )))
            ]
        );
    }

    #[test]
    fn url() {
        assert_eq!(
            parse("this inurl:test"),
            vec![
                Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Simple(
                    "this".to_string().into()
                ))),
                Box::new(Term::Url(SimpleOrPhrase::Simple("test".to_string().into())))
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
                Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Phrase(vec![
                    "this".to_string(),
                    "is".to_string(),
                    "a".to_string()
                ]))),
                Box::new(Term::Url(SimpleOrPhrase::Simple("test".to_string().into())))
            ]
        );
        assert_eq!(
            parse("\"this is a inurl:test"),
            vec![
                Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Simple(
                    "\"this".to_string().into()
                ))),
                Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Simple(
                    "is".to_string().into()
                ))),
                Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Simple(
                    "a".to_string().into()
                ))),
                Box::new(Term::Url(SimpleOrPhrase::Simple("test".to_string().into())))
            ]
        );
        assert_eq!(
            parse("this is a\" inurl:test"),
            vec![
                Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Simple(
                    "this".to_string().into()
                ))),
                Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Simple(
                    "is".to_string().into()
                ))),
                Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Simple(
                    "a\"".to_string().into()
                ))),
                Box::new(Term::Url(SimpleOrPhrase::Simple("test".to_string().into())))
            ]
        );

        assert_eq!(
            parse("\"this is a inurl:test\""),
            vec![Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Phrase(
                vec![
                    "this".to_string(),
                    "is".to_string(),
                    "a".to_string(),
                    "inurl:test".to_string()
                ]
            )))]
        );

        assert_eq!(
            parse("\"\""),
            vec![Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Phrase(
                vec![]
            )))]
        );
        assert_eq!(
            parse("“this is a“ inurl:test"),
            vec![
                Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Phrase(vec![
                    "this".to_string(),
                    "is".to_string(),
                    "a".to_string()
                ]))),
                Box::new(Term::Url(SimpleOrPhrase::Simple("test".to_string().into())))
            ]
        );
    }

    #[test]
    fn unicode() {
        let query = "\u{a0}";
        assert_eq!(parse(query).len(), 1);
    }

    proptest! {
        #[test]
        fn prop(query: String) {
            parse(&query);
        }
    }
}
