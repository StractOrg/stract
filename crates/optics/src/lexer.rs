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

use std::fmt::Display;

use logos::{Lexer, Logos};

use super::{Error, Result};

#[derive(Debug, PartialEq, Clone)]
pub enum Token<'a> {
    SemiColon,
    Comma,
    OpenBracket,
    CloseBracket,
    OpenParenthesis,
    CloseParenthesis,

    DiscardNonMatching,
    Rule,
    RankingPipeline,
    Ranking,
    Stage,
    Signal,
    Field,
    Matches,
    Site,
    Url,
    Domain,
    Title,
    Description,
    Content,
    MicroformatTag,
    Schema,
    Action,
    Boost,
    Downrank,
    Discard,
    Like,
    Dislike,

    String(&'a str),
    Number(&'a str),
}

impl<'a> Display for Token<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Token::SemiColon => f.write_str(";"),
            Token::Comma => f.write_str(","),
            Token::OpenBracket => f.write_str("{"),
            Token::CloseBracket => f.write_str("}"),
            Token::OpenParenthesis => f.write_str("("),
            Token::CloseParenthesis => f.write_str(")"),
            Token::DiscardNonMatching => f.write_str("DiscardNonMatching"),
            Token::Rule => f.write_str("Rule"),
            Token::RankingPipeline => f.write_str("RankingPipeline"),
            Token::Ranking => f.write_str("Ranking"),
            Token::Stage => f.write_str("Stage"),
            Token::Signal => f.write_str("Signal"),
            Token::Field => f.write_str("Field"),
            Token::Matches => f.write_str("Matches"),
            Token::Site => f.write_str("Site"),
            Token::Url => f.write_str("Url"),
            Token::Domain => f.write_str("Domain"),
            Token::Title => f.write_str("Title"),
            Token::Description => f.write_str("Description"),
            Token::Content => f.write_str("Content"),
            Token::MicroformatTag => f.write_str("MicroformatTag"),
            Token::Schema => f.write_str("Schema"),
            Token::Action => f.write_str("Action"),
            Token::Boost => f.write_str("Boost"),
            Token::Downrank => f.write_str("Downrank"),
            Token::Discard => f.write_str("Discard"),
            Token::Like => f.write_str("Like"),
            Token::Dislike => f.write_str("Dislike"),
            Token::String(s) => write!(f, "\"{s}\""),
            Token::Number(n) => write!(f, "{n}"),
        }
    }
}

#[derive(Logos, Debug, PartialEq, Clone)]
#[logos(skip r"[ \t\r\n\f]+")]
enum Outer<'a> {
    Error,

    #[token("\"")]
    StartString,
    #[regex(r"/\*")]
    StartBlockComment,
    #[regex(r"//")]
    StartLineComment,

    #[token(";")]
    SemiColon,
    #[token(",")]
    Comma,
    #[token("{")]
    OpenBracket,
    #[token("}")]
    CloseBracket,
    #[token("(")]
    OpenParenthesis,
    #[token(")")]
    CloseParenthesis,

    #[token("DiscardNonMatching")]
    DiscardNonMatching,
    #[token("Rule")]
    Rule,
    #[token("RankingPipeline")]
    RankingPipeline,
    #[token("Ranking")]
    Ranking,
    #[token("Stage")]
    Stage,
    #[token("Signal")]
    Signal,
    #[token("Field")]
    Field,
    #[token("Matches")]
    Matches,
    #[token("Site")]
    Site,
    #[token("Url")]
    Url,
    #[token("Domain")]
    Domain,
    #[token("Title")]
    Title,
    #[token("Description")]
    Description,
    #[token("Content")]
    Content,
    #[token("MicroformatTag")]
    MicroformatTag,
    #[token("Schema")]
    Schema,
    #[token("Action")]
    Action,
    #[token("Boost")]
    Boost,
    #[token("Downrank")]
    Downrank,
    #[token("Discard")]
    Discard,
    #[token("Like")]
    Like,
    #[token("Dislike")]
    Dislike,

    #[regex(r"[+-]?([0-9]*[.])?[0-9]+", |lex| lex.slice())]
    Number(&'a str),
}

#[derive(Logos, Debug, PartialEq, Clone)]
enum BlockComment {
    #[regex(r"[^/*]*")]
    Text,

    #[token("*/")]
    End,
}

#[derive(Logos, Debug, PartialEq, Clone)]
enum LineComment {
    #[regex(r"[^\n]*")]
    Text,

    #[regex(r"\n")]
    End,
}

#[derive(Logos, Debug, PartialEq, Clone)]
enum QuotedString<'a> {
    #[regex(r#"[^\\"]+"#)]
    Text(&'a str),

    #[token(r#"\""#)]
    EscapedQuote,

    #[token("\"")]
    EndString,
}

pub struct LexerBridge<'source> {
    lexer: Lexer<'source, Outer<'source>>,
    source: &'source str,
}

impl<'source> LexerBridge<'source> {
    pub fn new(source: &'source str) -> Self {
        Self {
            lexer: Outer::lexer(source),
            source,
        }
    }
}

impl<'source> LexerBridge<'source> {
    fn lex_next(&mut self) -> Option<Outer<'source>> {
        Some(self.lexer.next()?.unwrap_or(Outer::Error))
    }
}

// Clones as we switch between modes
impl<'source> Iterator for LexerBridge<'source> {
    type Item = Result<(usize, Token<'source>, usize)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut tok = self.lex_next();

        while let Some(innertok) = &tok {
            // ignore comments
            match innertok {
                Outer::StartBlockComment => {
                    let mut inner: Lexer<BlockComment> = self.lexer.clone().morph();
                    for tok in inner.by_ref() {
                        if matches!(tok, Ok(BlockComment::End)) {
                            break;
                        }
                    }

                    self.lexer = inner.morph();
                    tok = self.lex_next()
                }
                Outer::StartLineComment => {
                    let mut inner: Lexer<LineComment> = self.lexer.clone().morph();
                    for tok in inner.by_ref() {
                        if matches!(tok, Ok(LineComment::End)) {
                            break;
                        }
                    }

                    self.lexer = inner.morph();
                    tok = self.lex_next()
                }
                _ => break,
            }
        }

        // read string
        if let Some(Outer::StartString) = &tok {
            let mut inner: Lexer<QuotedString> = self.lexer.clone().morph();
            let start = inner.span().start + 1;

            let mut res = String::new();
            for tok in inner.by_ref() {
                match tok {
                    Err(()) => {
                        return Some(Err(Error::UnexpectedEof {
                            expected: vec!["\"".to_string()],
                        }))
                    }
                    Ok(QuotedString::Text(t)) => res.push_str(t),
                    Ok(QuotedString::EscapedQuote) => res.push('"'),
                    Ok(QuotedString::EndString) => break,
                }
            }
            let end = inner.span().end - 1;

            if start > end {
                return Some(Err(Error::UnexpectedEof {
                    expected: vec!["\"".to_string()],
                }));
            }

            self.lexer = inner.morph();

            return Some(Ok((start, Token::String(&self.source[start..end]), end)));
        }

        if let Some(tok) = tok {
            let s = self.lexer.span();

            match tok {
                Outer::Error => {
                    let tok = self.lexer.slice().to_string();
                    Some(Err(Error::UnrecognizedToken {
                        token: (s.start, tok, s.end),
                    }))
                }
                Outer::StartString => Some(Err(Error::Unknown(s.start, s.end))),
                Outer::StartBlockComment => Some(Err(Error::Unknown(s.start, s.end))),
                Outer::StartLineComment => Some(Err(Error::Unknown(s.start, s.end))),
                Outer::SemiColon => Some(Ok((s.start, Token::SemiColon, s.end))),
                Outer::Comma => Some(Ok((s.start, Token::Comma, s.end))),
                Outer::OpenBracket => Some(Ok((s.start, Token::OpenBracket, s.end))),
                Outer::CloseBracket => Some(Ok((s.start, Token::CloseBracket, s.end))),
                Outer::OpenParenthesis => Some(Ok((s.start, Token::OpenParenthesis, s.end))),
                Outer::CloseParenthesis => Some(Ok((s.start, Token::CloseParenthesis, s.end))),
                Outer::Rule => Some(Ok((s.start, Token::Rule, s.end))),
                Outer::Ranking => Some(Ok((s.start, Token::Ranking, s.end))),
                Outer::Stage => Some(Ok((s.start, Token::Stage, s.end))),
                Outer::RankingPipeline => Some(Ok((s.start, Token::RankingPipeline, s.end))),
                Outer::Signal => Some(Ok((s.start, Token::Signal, s.end))),
                Outer::Field => Some(Ok((s.start, Token::Field, s.end))),
                Outer::Matches => Some(Ok((s.start, Token::Matches, s.end))),
                Outer::Site => Some(Ok((s.start, Token::Site, s.end))),
                Outer::Url => Some(Ok((s.start, Token::Url, s.end))),
                Outer::Domain => Some(Ok((s.start, Token::Domain, s.end))),
                Outer::Title => Some(Ok((s.start, Token::Title, s.end))),
                Outer::Description => Some(Ok((s.start, Token::Description, s.end))),
                Outer::Content => Some(Ok((s.start, Token::Content, s.end))),
                Outer::MicroformatTag => Some(Ok((s.start, Token::MicroformatTag, s.end))),
                Outer::Schema => Some(Ok((s.start, Token::Schema, s.end))),
                Outer::Action => Some(Ok((s.start, Token::Action, s.end))),
                Outer::Boost => Some(Ok((s.start, Token::Boost, s.end))),
                Outer::Downrank => Some(Ok((s.start, Token::Downrank, s.end))),
                Outer::Discard => Some(Ok((s.start, Token::Discard, s.end))),
                Outer::Like => Some(Ok((s.start, Token::Like, s.end))),
                Outer::Dislike => Some(Ok((s.start, Token::Dislike, s.end))),
                Outer::Number(n) => Some(Ok((s.start, Token::Number(n), s.end))),
                Outer::DiscardNonMatching => Some(Ok((s.start, Token::DiscardNonMatching, s.end))),
            }
        } else {
            None
        }
    }
}

pub fn lex(source: &str) -> impl Iterator<Item = Result<(usize, Token<'_>, usize)>> {
    LexerBridge::new(source)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let s = r#"
            // this is a normal comment
            Ranking(Signal("host_centrality"), 3);
            /*
                this is a block commend
             */
            Ranking(Signal("bm25"), 100);
            Rule {
                Matches {
                    Url("/this/is/a/*/pattern")
                }
            }
        "#;

        let lexer = LexerBridge::new(s);

        let result: Vec<Token> = lexer.filter_map(|r| r.ok()).map(|(_, t, _)| t).collect();

        let expected = vec![
            Token::Ranking,
            Token::OpenParenthesis,
            Token::Signal,
            Token::OpenParenthesis,
            Token::String("host_centrality"),
            Token::CloseParenthesis,
            Token::Comma,
            Token::Number("3"),
            Token::CloseParenthesis,
            Token::SemiColon,
            Token::Ranking,
            Token::OpenParenthesis,
            Token::Signal,
            Token::OpenParenthesis,
            Token::String("bm25"),
            Token::CloseParenthesis,
            Token::Comma,
            Token::Number("100"),
            Token::CloseParenthesis,
            Token::SemiColon,
            Token::Rule,
            Token::OpenBracket,
            Token::Matches,
            Token::OpenBracket,
            Token::Url,
            Token::OpenParenthesis,
            Token::String("/this/is/a/*/pattern"),
            Token::CloseParenthesis,
            Token::CloseBracket,
            Token::CloseBracket,
        ];

        assert_eq!(result, expected)
    }

    #[test]
    fn empty_str() {
        let s = r#"
            Ranking(Signal(""), 3)
        "#;

        let lexer = LexerBridge::new(s);

        let result: Vec<Token> = lexer.filter_map(|r| r.ok()).map(|(_, t, _)| t).collect();

        let expected = vec![
            Token::Ranking,
            Token::OpenParenthesis,
            Token::Signal,
            Token::OpenParenthesis,
            Token::String(""),
            Token::CloseParenthesis,
            Token::Comma,
            Token::Number("3"),
            Token::CloseParenthesis,
        ];

        assert_eq!(result, expected)
    }

    #[test]
    fn empty_program() {
        let s = r#""#;

        let lexer = LexerBridge::new(s);

        assert_eq!(lexer.filter_map(|r| r.ok()).count(), 0);
    }
}
