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

use tantivy::tokenizer::{BoxTokenStream, StopWordFilter, TextAnalyzer};

use crate::tokenizer::{self, normalizer, Normalize, Tokenize};

#[derive(Clone, Default)]
pub struct DefaultTokenizer {
    stopwords: Option<Vec<String>>,
    analyzer: Option<TextAnalyzer>,
}

impl DefaultTokenizer {
    pub fn as_str() -> &'static str {
        "tokenizer"
    }

    pub fn with_stopwords(stopwords: Vec<String>) -> Self {
        Self {
            stopwords: Some(stopwords),
            analyzer: None,
        }
    }
}
impl tantivy::tokenizer::Tokenizer for DefaultTokenizer {
    type TokenStream<'a> = BoxTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let builder = TextAnalyzer::builder(Normal);

        self.analyzer = if let Some(stopwords) = &self.stopwords {
            Some(
                builder
                    .filter(StopWordFilter::remove(stopwords.clone()))
                    .build(),
            )
        } else {
            Some(builder.build())
        };

        self.analyzer.as_mut().unwrap().token_stream(text)
    }
}

#[derive(Clone)]
pub struct Normal;

pub struct NormalTokenStream<'a> {
    stream: Box<dyn Iterator<Item = tokenizer::Token<'a>> + 'a>,
    token: Option<tantivy::tokenizer::Token>,
    next_position: usize,
}

impl tantivy::tokenizer::Tokenizer for Normal {
    type TokenStream<'a> = BoxTokenStream<'a>;

    fn token_stream<'a>(&mut self, text: &'a str) -> Self::TokenStream<'a> {
        let stream = Box::new(
            text.tokenize()
                .normalize(&normalizer::Lowercase)
                .normalize(&normalizer::UnicodeNFKD)
                .normalize(&normalizer::UnicodeDiacritics),
        );

        BoxTokenStream::new(NormalTokenStream::new_boxed(stream))
    }
}

impl<'a> tantivy::tokenizer::TokenStream for NormalTokenStream<'a> {
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

impl<'a> NormalTokenStream<'a> {
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
    use lending_iter::LendingIterator;
    use proptest::prelude::*;
    use tantivy::tokenizer::Tokenizer as _;

    fn tokenize_default(s: &str) -> Vec<String> {
        let mut res = Vec::new();
        let mut tokenizer = DefaultTokenizer::default();
        let mut stream = tokenizer.token_stream(s);
        let mut it = tantivy::tokenizer::TokenStream::iter(&mut stream);

        while let Some(token) = it.next() {
            res.push(token.text.clone());
        }

        res
    }

    #[test]
    fn default_tokenization() {
        assert_eq!(
            tokenize_default("this is a relatively simple123 test    string"),
            vec![
                "this",
                "is",
                "a",
                "relatively",
                "simple123",
                "test",
                "string"
            ]
        );
    }

    #[test]
    fn special_character_tokenization() {
        assert_eq!(
            tokenize_default("example.com"),
            vec!["example", ".", "com",]
        );
        assert_eq!(
            tokenize_default("example. com"),
            vec!["example", ".", "com",]
        );
        assert_eq!(
            tokenize_default("example . com"),
            vec!["example", ".", "com",]
        );

        assert_eq!(
            tokenize_default("a c++ blog post"),
            vec!["a", "c", "+", "+", "blog", "post"]
        );
        assert_eq!(tokenize_default("path/test"), vec!["path", "/", "test",]);
    }

    #[test]
    fn han() {
        assert_eq!(
            tokenize_default("test 漢.com"),
            vec!["test", "漢", ".", "com"]
        );
    }

    #[test]
    fn hiragana() {
        assert_eq!(
            tokenize_default("test あ.com"),
            vec!["test", "あ", ".", "com"]
        );
    }

    #[test]
    fn katakana() {
        assert_eq!(
            tokenize_default("test ダ.com"),
            vec!["test", "タ\u{3099}", ".", "com"]
        );
    }

    #[test]
    fn cyrillic() {
        assert_eq!(
            tokenize_default("test б.com"),
            vec!["test", "б", ".", "com"]
        );
    }

    #[test]
    fn arabic() {
        assert_eq!(
            tokenize_default("test ب.com"),
            vec!["test", "ب", ".", "com"]
        );
    }

    proptest! {
        #[test]
        fn prop_default_tokenization(s: String) {
            let _ = tokenize_default(&s);
        }
    }
}
