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

pub const MAX_TERMS_PER_QUERY: usize = 32;

use crate::bangs::BANG_PREFIXES;

mod term;

pub use term::*;

fn trim_leading_whitespace(input: &str) -> nom::IResult<&str, &str> {
    let (input, _) = nom::character::complete::multispace0(input)?;
    Ok((input, input))
}

fn until_space_or_end(input: &str) -> nom::IResult<&str, &str> {
    let (input, output) = nom::bytes::complete::take_while(|c: char| c != ' ')(input)?;
    Ok((input, output))
}

fn simple_str(input: &str) -> nom::IResult<&str, &str> {
    // parse until we find a space or the end of the string
    if input.is_empty() {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Eof,
        )));
    }
    let (input, _) = trim_leading_whitespace(input)?;
    let (input, output) = until_space_or_end(input)?;
    Ok((input, output))
}

fn simple(input: &str) -> nom::IResult<&str, SimpleOrPhrase> {
    let (input, output) = simple_str(input)?;
    Ok((input, SimpleOrPhrase::Simple(output.to_string().into())))
}

fn simple_term(input: &str) -> nom::IResult<&str, Term> {
    let (input, output) = simple(input)?;
    Ok((input, Term::SimpleOrPhrase(output)))
}

fn phrase_helper(
    input: &str,
    start_quote: char,
    end_quote: char,
) -> nom::IResult<&str, SimpleOrPhrase> {
    let (input, _) = nom::character::complete::char(start_quote)(input)?;
    let (input, output) = nom::bytes::complete::take_until(end_quote.to_string().as_str())(input)?;
    let (input, _) = nom::character::complete::char(end_quote)(input)?;
    Ok((
        input,
        SimpleOrPhrase::Phrase(output.split_whitespace().map(|s| s.to_string()).collect()),
    ))
}

fn phrase(input: &str) -> nom::IResult<&str, SimpleOrPhrase> {
    let normal = |s| phrase_helper(s, '"', '"');
    let apple1 = |s| phrase_helper(s, '“', '”');
    let apple2 = |s| phrase_helper(s, '“', '“');

    nom::branch::alt((normal, apple1, apple2))(input)
}

fn phrase_term(input: &str) -> nom::IResult<&str, Term> {
    let (input, output) = phrase(input)?;
    Ok((input, Term::SimpleOrPhrase(output)))
}

fn simple_or_phrase(input: &str) -> nom::IResult<&str, SimpleOrPhrase> {
    nom::branch::alt((phrase, simple))(input)
}

fn single_bang(input: &str, pref: char) -> nom::IResult<&str, Term> {
    let (input, _) = nom::character::complete::char(pref)(input)?;
    let (input, output) = until_space_or_end(input)?;
    Ok((
        input,
        Term::PossibleBang {
            prefix: pref,
            bang: output.to_string(),
        },
    ))
}

fn bang(input: &str) -> nom::IResult<&str, Term> {
    for pref in BANG_PREFIXES.iter() {
        if let Ok((input, output)) = single_bang(input, *pref) {
            return Ok((input, output));
        }
    }

    Err(nom::Err::Error(nom::error::Error::new(
        input,
        nom::error::ErrorKind::Fail,
    )))
}

fn site_field(input: &str) -> nom::IResult<&str, Term> {
    // parse 'site:' and then a simple term
    let (input, _) = nom::bytes::complete::tag("site:")(input)?;
    let (input, output) = simple_str(input)?;

    Ok((input, Term::Site(output.to_string())))
}

fn links_to_field(input: &str) -> nom::IResult<&str, Term> {
    // parse 'linksto:' and then a simple term
    let (input, _) = nom::branch::alt((
        nom::bytes::complete::tag("linksto:"),
        nom::bytes::complete::tag("linkto:"),
    ))(input)?;
    let (input, output) = simple_str(input)?;

    Ok((input, Term::LinkTo(output.to_string())))
}

fn links_from_field(input: &str) -> nom::IResult<&str, Term> {
    // parse 'linksfrom:' and then a simple term
    let (input, _) = nom::branch::alt((
        nom::bytes::complete::tag("linksfrom:"),
        nom::bytes::complete::tag("linkfrom:"),
    ))(input)?;
    let (input, output) = simple_str(input)?;

    Ok((input, Term::LinkFrom(output.to_string())))
}

fn title_field(input: &str) -> nom::IResult<&str, Term> {
    // parse 'title:' and then a simple term
    let (input, _) = nom::bytes::complete::tag("intitle:")(input)?;
    let (input, output) = simple_or_phrase(input)?;

    Ok((input, Term::Title(output)))
}

fn body_field(input: &str) -> nom::IResult<&str, Term> {
    // parse 'body:' and then a simple term
    let (input, _) = nom::bytes::complete::tag("inbody:")(input)?;
    let (input, output) = simple_or_phrase(input)?;

    Ok((input, Term::Body(output)))
}

fn url_field(input: &str) -> nom::IResult<&str, Term> {
    // parse 'url:' and then a simple term
    let (input, _) = nom::bytes::complete::tag("inurl:")(input)?;
    let (input, output) = simple_or_phrase(input)?;

    Ok((input, Term::Url(output)))
}

fn field_selector(input: &str) -> nom::IResult<&str, Term> {
    nom::branch::alt((
        site_field,
        links_to_field,
        links_from_field,
        title_field,
        body_field,
        url_field,
    ))(input)
}

fn not(input: &str) -> nom::IResult<&str, Term> {
    // ignore double negation
    if let Ok((_, _)) = nom::bytes::complete::tag::<_, _, nom::error::Error<&str>>("--")(input) {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Fail,
        )));
    }

    let (input, _) = nom::bytes::complete::tag("-")(input)?;
    let (input, output) = term(input)?;
    Ok((input, Term::Not(Box::new(output))))
}

fn ignore_quotes_helper(
    input: &str,
    start_quote: char,
    end_quote: char,
) -> nom::IResult<&str, &str> {
    let (input, _) = nom::character::complete::char(start_quote)(input)?;
    let (input, output) = nom::bytes::complete::take_until(end_quote.to_string().as_str())(input)?;
    let (input, _) = nom::character::complete::char(end_quote)(input)?;
    Ok((input, output))
}

fn ignore_weird_quotes(input: &str) -> nom::IResult<&str, &str> {
    let guillemet = |s| ignore_quotes_helper(s, '«', '»');
    let up_down_quotes = |s| ignore_quotes_helper(s, '„', '“');
    let rev_guillemet = |s| ignore_quotes_helper(s, '»', '«');
    let squares = |s| ignore_quotes_helper(s, '「', '」');

    nom::branch::alt((guillemet, up_down_quotes, rev_guillemet, squares))(input)
}

fn term(input: &str) -> nom::IResult<&str, Term> {
    let (mut input, _) = trim_leading_whitespace(input)?;

    if let Ok((_, new_input)) = ignore_weird_quotes(input) {
        input = new_input;
    }

    nom::branch::alt((phrase_term, bang, field_selector, not, simple_term))(input)
}

pub fn parse(query: &str) -> anyhow::Result<Vec<Term>> {
    if query.is_empty() || query.chars().all(char::is_whitespace) {
        return Ok(vec![]);
    }

    nom::multi::many1(term)(query)
        .map(|(_, res)| res)
        .map_err(|e| anyhow::anyhow!("Failed to parse query: {:?}", e))
}

pub fn truncate(terms: Vec<Term>) -> Vec<Term> {
    terms
        .into_iter()
        .take(MAX_TERMS_PER_QUERY)
        .map(|t| t.truncate())
        .collect()
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{SimpleOrPhrase, Term};

    fn parse(input: &str) -> Vec<Term> {
        super::truncate(super::parse(input).unwrap())
    }

    #[test]
    fn parse_not() {
        assert_eq!(
            parse("this -that"),
            vec![
                Term::SimpleOrPhrase(SimpleOrPhrase::Simple("this".to_string().into())),
                Term::Not(Box::new(Term::SimpleOrPhrase(SimpleOrPhrase::Simple(
                    "that".to_string().into()
                ))))
            ]
        );

        assert_eq!(
            parse("this -"),
            vec![
                Term::SimpleOrPhrase(SimpleOrPhrase::Simple("this".to_string().into())),
                Term::SimpleOrPhrase(SimpleOrPhrase::Simple("-".to_string().into()))
            ]
        );
    }

    #[test]
    fn double_not() {
        assert_eq!(
            parse("this --that"),
            vec![
                Term::SimpleOrPhrase(SimpleOrPhrase::Simple("this".to_string().into())),
                Term::SimpleOrPhrase(SimpleOrPhrase::Simple("--that".to_string().into()))
            ]
        );
    }

    #[test]
    fn site() {
        assert_eq!(
            parse("this site:test.com"),
            vec![
                Term::SimpleOrPhrase(SimpleOrPhrase::Simple("this".to_string().into())),
                Term::Site("test.com".to_string())
            ]
        );
    }

    #[test]
    fn title() {
        assert_eq!(
            parse("this intitle:test"),
            vec![
                Term::SimpleOrPhrase(SimpleOrPhrase::Simple("this".to_string().into())),
                Term::Title(SimpleOrPhrase::Simple("test".to_string().into()))
            ]
        );
    }

    #[test]
    fn body() {
        assert_eq!(
            parse("this inbody:test"),
            vec![
                Term::SimpleOrPhrase(SimpleOrPhrase::Simple("this".to_string().into())),
                Term::Body(SimpleOrPhrase::Simple("test".to_string().into()))
            ]
        );
    }

    #[test]
    fn url() {
        assert_eq!(
            parse("this inurl:test"),
            vec![
                Term::SimpleOrPhrase(SimpleOrPhrase::Simple("this".to_string().into())),
                Term::Url(SimpleOrPhrase::Simple("test".to_string().into()))
            ]
        );
    }

    #[test]
    fn empty() {
        assert_eq!(parse(""), vec![]);
        assert_eq!(parse(" "), vec![]);
    }

    #[test]
    fn phrase() {
        assert_eq!(
            parse("\"this is a\" inurl:test"),
            vec![
                Term::SimpleOrPhrase(SimpleOrPhrase::Phrase(vec![
                    "this".to_string(),
                    "is".to_string(),
                    "a".to_string()
                ])),
                Term::Url(SimpleOrPhrase::Simple("test".to_string().into()))
            ]
        );
        assert_eq!(
            parse("\"this is a inurl:test"),
            vec![
                Term::SimpleOrPhrase(SimpleOrPhrase::Simple("\"this".to_string().into())),
                Term::SimpleOrPhrase(SimpleOrPhrase::Simple("is".to_string().into())),
                Term::SimpleOrPhrase(SimpleOrPhrase::Simple("a".to_string().into())),
                Term::Url(SimpleOrPhrase::Simple("test".to_string().into()))
            ]
        );
        assert_eq!(
            parse("this is a\" inurl:test"),
            vec![
                Term::SimpleOrPhrase(SimpleOrPhrase::Simple("this".to_string().into())),
                Term::SimpleOrPhrase(SimpleOrPhrase::Simple("is".to_string().into())),
                Term::SimpleOrPhrase(SimpleOrPhrase::Simple("a\"".to_string().into())),
                Term::Url(SimpleOrPhrase::Simple("test".to_string().into()))
            ]
        );

        assert_eq!(
            parse("\"this is a inurl:test\""),
            vec![Term::SimpleOrPhrase(SimpleOrPhrase::Phrase(vec![
                "this".to_string(),
                "is".to_string(),
                "a".to_string(),
                "inurl:test".to_string()
            ]))]
        );

        assert_eq!(
            parse("\"\""),
            vec![Term::SimpleOrPhrase(SimpleOrPhrase::Phrase(vec![]))]
        );
        assert_eq!(
            parse("“this is a“ inurl:test"),
            vec![
                Term::SimpleOrPhrase(SimpleOrPhrase::Phrase(vec![
                    "this".to_string(),
                    "is".to_string(),
                    "a".to_string()
                ])),
                Term::Url(SimpleOrPhrase::Simple("test".to_string().into()))
            ]
        );

        assert_eq!(
            parse("that's not"),
            vec![
                Term::SimpleOrPhrase(SimpleOrPhrase::Simple("that's".to_string().into())),
                Term::SimpleOrPhrase(SimpleOrPhrase::Simple("not".to_string().into()))
            ]
        );

        assert_eq!(
            parse("inbody:\"this should work\""),
            vec![Term::Body(SimpleOrPhrase::Phrase(vec![
                "this".to_string(),
                "should".to_string(),
                "work".to_string()
            ]))]
        );
    }

    #[test]
    fn unicode() {
        let query = "🦀";
        assert_eq!(parse(query).len(), 1);
    }

    #[test]
    fn test_truncate() {
        let q = "test ";

        let terms = parse(q);
        assert_eq!(terms.len(), 1);

        let q = q.repeat(1024);
        let terms = parse(&q);
        assert!(terms.len() < 1020);
    }

    proptest! {
        #[test]
        fn prop(query: String) {
            parse(&query);
        }
    }
}
