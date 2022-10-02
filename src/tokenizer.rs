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
use tantivy::tokenizer::{
    BoxTokenStream, Language, LowerCaser, Stemmer, StopWordFilter, TextAnalyzer,
};

use whatlang::Lang;

struct MyStemmer(Stemmer);

impl From<Lang> for MyStemmer {
    fn from(lang: Lang) -> Self {
        match lang {
            Lang::Dan => MyStemmer(Stemmer::new(Language::Danish)),
            Lang::Ara => MyStemmer(Stemmer::new(Language::Arabic)),
            Lang::Nld => MyStemmer(Stemmer::new(Language::Dutch)),
            Lang::Fin => MyStemmer(Stemmer::new(Language::Finnish)),
            Lang::Fra => MyStemmer(Stemmer::new(Language::French)),
            Lang::Deu => MyStemmer(Stemmer::new(Language::German)),
            Lang::Hun => MyStemmer(Stemmer::new(Language::Hungarian)),
            Lang::Ita => MyStemmer(Stemmer::new(Language::Italian)),
            Lang::Por => MyStemmer(Stemmer::new(Language::Portuguese)),
            Lang::Ron => MyStemmer(Stemmer::new(Language::Romanian)),
            Lang::Rus => MyStemmer(Stemmer::new(Language::Russian)),
            Lang::Spa => MyStemmer(Stemmer::new(Language::Spanish)),
            Lang::Swe => MyStemmer(Stemmer::new(Language::Swedish)),
            Lang::Tam => MyStemmer(Stemmer::new(Language::Tamil)),
            Lang::Tur => MyStemmer(Stemmer::new(Language::Turkish)),
            _ => MyStemmer(Stemmer::new(Language::English)),
        }
    }
}

#[derive(Clone)]
pub enum Tokenizer {
    Normal(Normal),
    Identity(Identity),
    Stemmed(Stemmed),
}

impl Tokenizer {
    pub fn new_stemmed() -> Self {
        Self::Stemmed(Stemmed::default())
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            Tokenizer::Normal(_) => Normal::as_str(),
            Tokenizer::Stemmed(_) => Stemmed::as_str(),
            Tokenizer::Identity(_) => Identity::as_str(),
        }
    }
}

impl Default for Tokenizer {
    fn default() -> Self {
        Self::Normal(Normal::default())
    }
}

#[derive(Clone, Default)]
pub struct Normal {
    stopwords: Option<Vec<String>>,
}

impl Normal {
    pub fn as_str() -> &'static str {
        "tokenizer"
    }

    pub fn with_stopwords(stopwords: Vec<String>) -> Self {
        Self {
            stopwords: Some(stopwords),
        }
    }
}

#[derive(Clone, Default)]
pub struct Stemmed {
    force_language: Option<Lang>,
}

impl Stemmed {
    pub fn as_str() -> &'static str {
        "stemmed_tokenizer"
    }
    pub fn with_forced_language(lang: Lang) -> Self {
        Self {
            force_language: Some(lang),
        }
    }
}

#[derive(Clone, Default)]
pub struct Identity {}

impl Identity {
    pub fn as_str() -> &'static str {
        "identity_tokenizer"
    }
}

impl tantivy::tokenizer::Tokenizer for Tokenizer {
    fn token_stream<'a>(&self, text: &'a str) -> tantivy::tokenizer::BoxTokenStream<'a> {
        match self {
            Tokenizer::Normal(tokenizer) => tokenizer.token_stream(text),
            Tokenizer::Stemmed(tokenizer) => tokenizer.token_stream(text),
            Tokenizer::Identity(tokenizer) => tokenizer.token_stream(text),
        }
    }
}

impl tantivy::tokenizer::Tokenizer for Normal {
    fn token_stream<'a>(&self, text: &'a str) -> tantivy::tokenizer::BoxTokenStream<'a> {
        let mut analyzer = TextAnalyzer::from(Simple).filter(LowerCaser);

        if let Some(stopwords) = &self.stopwords {
            analyzer = analyzer.filter(StopWordFilter::remove(stopwords.clone()));
        }

        analyzer.token_stream(text)
    }
}

impl tantivy::tokenizer::Tokenizer for Stemmed {
    fn token_stream<'a>(&self, text: &'a str) -> tantivy::tokenizer::BoxTokenStream<'a> {
        let analyzer = TextAnalyzer::from(Simple)
            .filter(LowerCaser)
            .filter(StopWordFilter::remove(vec![]));

        let lang = match self.force_language {
            Some(lang) => Some(lang),
            None => whatlang::detect_lang(text),
        };

        match lang {
            Some(lang) => analyzer.filter(MyStemmer::from(lang).0).token_stream(text),
            None => analyzer.token_stream(text),
        }
    }
}

impl tantivy::tokenizer::Tokenizer for Identity {
    fn token_stream<'a>(&self, text: &'a str) -> tantivy::tokenizer::BoxTokenStream<'a> {
        BoxTokenStream::from(IdentityTokenStream::from(text.to_string()))
    }
}

pub struct IdentityTokenStream {
    num_advances: usize,
    token: Option<tantivy::tokenizer::Token>,
}

impl From<String> for IdentityTokenStream {
    fn from(text: String) -> Self {
        Self {
            num_advances: 0,
            token: Some(tantivy::tokenizer::Token {
                offset_from: 0,
                offset_to: text.len(),
                position: 0,
                text,
                ..Default::default()
            }),
        }
    }
}

impl tantivy::tokenizer::TokenStream for IdentityTokenStream {
    fn advance(&mut self) -> bool {
        self.num_advances += 1;

        if self.num_advances == 1 {
            true
        } else {
            self.token = None;
            false
        }
    }

    fn token(&self) -> &tantivy::tokenizer::Token {
        self.token.as_ref().unwrap()
    }

    fn token_mut(&mut self) -> &mut tantivy::tokenizer::Token {
        self.token.as_mut().unwrap()
    }
}

#[derive(Logos, Debug, PartialEq)]
enum Token {
    #[regex("[\\w|\\p{Han}|\\p{Hiragana}|\\p{Katakana}|\\p{Cyrillic}|\\p{Arabic}]+")]
    Text,

    #[error]
    #[regex(r"[ \t\n\f]+", logos::skip)]
    Error,
}

#[derive(Clone)]
pub struct Simple;

pub struct SimpleTokenStream<'a> {
    lexer: Lexer<'a, Token>,
    token: Option<tantivy::tokenizer::Token>,
    next_position: usize,
}

impl tantivy::tokenizer::Tokenizer for Simple {
    fn token_stream<'a>(&self, text: &'a str) -> BoxTokenStream<'a> {
        let lexer = Token::lexer(text);
        BoxTokenStream::from(SimpleTokenStream {
            lexer,
            token: None,
            next_position: 0,
        })
    }
}

impl<'a> tantivy::tokenizer::TokenStream for SimpleTokenStream<'a> {
    fn advance(&mut self) -> bool {
        self.token = self.lexer.next().map(|_| {
            let span = self.lexer.span();
            let pos = self.next_position;
            self.next_position += 1;
            tantivy::tokenizer::Token {
                offset_from: span.start,
                offset_to: span.end,
                position: pos,
                text: self.lexer.slice().to_string(),
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

#[cfg(test)]
mod tests {
    use tantivy::tokenizer::Tokenizer;

    use super::*;

    fn tokenize(s: &str) -> Vec<String> {
        let mut res = Vec::new();
        let mut stream = Normal::default().token_stream(s);

        while let Some(token) = stream.next() {
            res.push(token.text.clone());
        }

        res
    }

    #[test]
    fn simple_tokenization() {
        assert_eq!(
            tokenize("this is a relatively simple123 test    string"),
            vec![
                "this".to_string(),
                "is".to_string(),
                "a".to_string(),
                "relatively".to_string(),
                "simple123".to_string(),
                "test".to_string(),
                "string".to_string()
            ]
        );
    }

    #[test]
    fn special_character_tokenization() {
        assert_eq!(
            tokenize("example.com"),
            vec!["example".to_string(), ".".to_string(), "com".to_string(),]
        );
        assert_eq!(
            tokenize("example. com"),
            vec!["example".to_string(), ".".to_string(), "com".to_string(),]
        );
        assert_eq!(
            tokenize("example . com"),
            vec!["example".to_string(), ".".to_string(), "com".to_string(),]
        );

        assert_eq!(
            tokenize("a c++ blog post"),
            vec![
                "a".to_string(),
                "c".to_string(),
                "+".to_string(),
                "+".to_string(),
                "blog".to_string(),
                "post".to_string()
            ]
        );
        assert_eq!(
            tokenize("path/test"),
            vec!["path".to_string(), "/".to_string(), "test".to_string(),]
        );
    }

    #[test]
    fn han() {
        assert_eq!(
            tokenize("test 漢.com"),
            vec![
                "test".to_string(),
                "漢".to_string(),
                ".".to_string(),
                "com".to_string()
            ]
        );
    }

    #[test]
    fn hiragana() {
        assert_eq!(
            tokenize("test あ.com"),
            vec![
                "test".to_string(),
                "あ".to_string(),
                ".".to_string(),
                "com".to_string()
            ]
        );
    }

    #[test]
    fn katakana() {
        assert_eq!(
            tokenize("test ダ.com"),
            vec![
                "test".to_string(),
                "ダ".to_string(),
                ".".to_string(),
                "com".to_string()
            ]
        );
    }

    #[test]
    fn cyrillic() {
        assert_eq!(
            tokenize("test б.com"),
            vec![
                "test".to_string(),
                "б".to_string(),
                ".".to_string(),
                "com".to_string()
            ]
        );
    }

    #[test]
    fn arabic() {
        assert_eq!(
            tokenize("test ب.com"),
            vec![
                "test".to_string(),
                "ب".to_string(),
                ".".to_string(),
                "com".to_string()
            ]
        );
    }
}
