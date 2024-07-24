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
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use tantivy::tokenizer::{BoxTokenStream, TextAnalyzer};

use crate::tokenizer::{self, normalizer, split_with_range::SplitWithRange, Normalize};

#[derive(Clone, Default)]
pub struct NewlineTokenizer {
    analyzer: Option<TextAnalyzer>,
}

impl NewlineTokenizer {
    pub fn as_str() -> &'static str {
        "newline"
    }
}

impl tantivy::tokenizer::Tokenizer for NewlineTokenizer {
    type TokenStream<'a> = BoxTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let builder = TextAnalyzer::builder(Newline);

        self.analyzer = Some(builder.build());

        self.analyzer.as_mut().unwrap().token_stream(text)
    }
}

#[derive(Clone)]
pub struct Newline;

pub struct NewlineTokenStream<'a> {
    stream: Box<dyn Iterator<Item = tokenizer::Token<'a>> + 'a>,
    token: Option<tantivy::tokenizer::Token>,
    next_position: usize,
}

impl tantivy::tokenizer::Tokenizer for Newline {
    type TokenStream<'a> = BoxTokenStream<'a>;

    fn token_stream<'a>(&mut self, text: &'a str) -> Self::TokenStream<'a> {
        let stream = Box::new(
            text.split_with_range(|c| c == '\n' || c == '\r')
                .map(|(s, range)| tokenizer::Token::new(s, range))
                .normalize(&normalizer::Lowercase)
                .normalize(&normalizer::UnicodeNFKD)
                .normalize(&normalizer::UnicodeDiacritics),
        );

        BoxTokenStream::new(NewlineTokenStream::new_boxed(stream))
    }
}

impl<'a> tantivy::tokenizer::TokenStream for NewlineTokenStream<'a> {
    fn advance(&mut self) -> bool {
        self.token = self.stream.next().map(|token| {
            let span = token.span();
            let pos = self.next_position;
            self.next_position += 1;
            tantivy::tokenizer::Token {
                offset_from: span.start,
                offset_to: span.end,
                position: pos,
                text: token.text().to_string(),
                ..Default::default()
            }
        });

        self.token.is_some()
    }

    fn token(&self) -> &tantivy::tokenizer::Token {
        self.token.as_ref().unwrap()
    }

    fn token_mut(&mut self) -> &mut tantivy::tokenizer::Token {
        self.token.as_mut().unwrap()
    }
}

impl<'a> NewlineTokenStream<'a> {
    fn new_boxed(
        stream: Box<dyn Iterator<Item = tokenizer::Token<'a>> + 'a>,
    ) -> BoxTokenStream<'a> {
        BoxTokenStream::new(Self {
            stream,
            token: None,
            next_position: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::tokenizer::Tokenizer as _;

    fn tokenize_newline(s: &str) -> Vec<String> {
        let mut res = Vec::new();
        let mut tokenizer = NewlineTokenizer::default();
        let mut stream = tokenizer.token_stream(s);

        while let Some(token) = stream.next() {
            res.push(token.text.clone());
        }

        res
    }

    #[test]
    fn newline_tokenizer() {
        assert!(tokenize_newline("").is_empty());
        assert_eq!(tokenize_newline("a\nb"), vec!["a", "b"]);
        assert_eq!(tokenize_newline("a\nb\n"), vec!["a", "b"]);
        assert_eq!(tokenize_newline("\na\nb\n"), vec!["a", "b"]);
        assert_eq!(tokenize_newline("\na\nb\nc"), vec!["a", "b", "c"]);
    }

    #[test]
    fn newline_tokenizer_without_newlines() {
        assert!(tokenize_newline("").is_empty());
        assert_eq!(tokenize_newline("test"), vec!["test"]);

        assert_eq!(tokenize_newline("this is"), vec!["this is"]);
        assert_eq!(tokenize_newline("this is a"), vec!["this is a",]);
        assert_eq!(tokenize_newline("this is a test"), vec!["this is a test",]);

        assert_eq!(tokenize_newline("this.is"), vec!["this.is"]);
    }
}
