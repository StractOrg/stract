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

use std::str::CharIndices;

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

pub struct FlattenedJson {
    flattened_json: String,
}

fn rec_flatten(val: serde_json::Value) -> Vec<String> {
    match val {
        serde_json::Value::Null => vec![],
        serde_json::Value::Bool(b) => vec![format!("=\"{b}\"")],
        serde_json::Value::Number(n) => vec![format!("=\"{n}\"")],
        serde_json::Value::String(s) => vec![format!("=\"{}\"", s.replace('"', "\\\""))],
        serde_json::Value::Array(arr) => arr
            .into_iter()
            .flat_map(|val| rec_flatten(val).into_iter())
            .collect(),
        serde_json::Value::Object(map) => {
            let mut res = Vec::new();
            for (key, value) in map {
                let mut k = ".".to_string();
                k.push_str(&key);

                for val in rec_flatten(value) {
                    let mut k = k.clone();
                    k.push_str(&val);
                    res.push(k)
                }
            }

            res
        }
    }
}

impl FlattenedJson {
    pub fn new<T>(value: &T) -> crate::Result<Self>
    where
        T: serde::Serialize,
    {
        let json = serde_json::to_string(value)?;
        let val: serde_json::Value = serde_json::from_str(&json)?;

        let flattened_json = itertools::intersperse(
            rec_flatten(val)
                .into_iter()
                .map(|l| l.strip_prefix('.').unwrap().to_string()),
            "\n".to_string(),
        )
        .collect();

        Ok(Self { flattened_json })
    }

    pub fn token_stream(&self) -> BoxTokenStream {
        tantivy::tokenizer::Tokenizer::token_stream(&JsonField, &self.flattened_json)
    }

    pub fn as_str() -> &'static str {
        "flattened_json_tokenizer"
    }
}

#[derive(Clone)]
pub struct JsonField;

impl tantivy::tokenizer::Tokenizer for JsonField {
    fn token_stream<'a>(&self, text: &'a str) -> BoxTokenStream<'a> {
        BoxTokenStream::from(JsonFieldTokenStream {
            text,
            chars: text.char_indices(),
            token: tantivy::tokenizer::Token::default(),
        })
    }
}

pub struct JsonFieldTokenStream<'a> {
    text: &'a str,
    chars: CharIndices<'a>,
    token: tantivy::tokenizer::Token,
}

impl<'a> JsonFieldTokenStream<'a> {
    // search for the end of the current token.
    fn search_token_end(&mut self, is_quote: bool) -> usize {
        let mut escaped = false;
        for (offset, c) in self.chars.by_ref() {
            if is_quote {
                if c == '\\' {
                    escaped = true;
                } else {
                    if c == '"' && !escaped {
                        return offset;
                    }

                    escaped = false;
                }
            } else if !c.is_alphanumeric() {
                return offset;
            }
        }

        self.text.len()
    }
}

impl<'a> tantivy::tokenizer::TokenStream for JsonFieldTokenStream<'a> {
    fn advance(&mut self) -> bool {
        self.token.text.clear();
        self.token.position = self.token.position.wrapping_add(1);
        let mut prev_was_quote = false;

        while let Some((offset_from, c)) = self.chars.next() {
            if c.is_alphanumeric() {
                let offset_to = self.search_token_end(prev_was_quote);
                self.token.offset_from = offset_from;
                self.token.offset_to = offset_to;

                if prev_was_quote {
                    self.token.offset_from -= 1;
                    self.token.offset_to += 1;
                }

                self.token
                    .text
                    .push_str(&self.text[self.token.offset_from..self.token.offset_to]);
                return true;
            }

            prev_was_quote = c == '"';
        }
        false
    }

    fn token(&self) -> &tantivy::tokenizer::Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut tantivy::tokenizer::Token {
        &mut self.token
    }
}

#[cfg(test)]
mod tests {
    use tantivy::tokenizer::Tokenizer;

    use super::*;

    fn tokenize_simple(s: &str) -> Vec<String> {
        let mut res = Vec::new();
        let mut stream = Normal::default().token_stream(s);

        while let Some(token) = stream.next() {
            res.push(token.text.clone());
        }

        res
    }

    fn tokenize_json(s: &str) -> Vec<String> {
        let mut res = Vec::new();
        let mut stream = JsonField.token_stream(s);

        while let Some(token) = stream.next() {
            res.push(token.text.clone());
        }

        res
    }

    #[test]
    fn simple_tokenization() {
        assert_eq!(
            tokenize_simple("this is a relatively simple123 test    string"),
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
            tokenize_simple("example.com"),
            vec!["example".to_string(), ".".to_string(), "com".to_string(),]
        );
        assert_eq!(
            tokenize_simple("example. com"),
            vec!["example".to_string(), ".".to_string(), "com".to_string(),]
        );
        assert_eq!(
            tokenize_simple("example . com"),
            vec!["example".to_string(), ".".to_string(), "com".to_string(),]
        );

        assert_eq!(
            tokenize_simple("a c++ blog post"),
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
            tokenize_simple("path/test"),
            vec!["path".to_string(), "/".to_string(), "test".to_string(),]
        );
    }

    #[test]
    fn tokenize_json_field() {
        assert_eq!(
            tokenize_json(
                r#"
                Test.field="value"
            "#
            ),
            vec![
                "Test".to_string(),
                "field".to_string(),
                "\"value\"".to_string(),
            ]
        );
        assert_eq!(
            tokenize_json(
                r#"
                Test.field="this is the value"
            "#
            ),
            vec![
                "Test".to_string(),
                "field".to_string(),
                "\"this is the value\"".to_string(),
            ]
        );
        assert_eq!(
            tokenize_json(
                r#"
                Test.field="this is\" the value"
            "#
            ),
            vec![
                "Test".to_string(),
                "field".to_string(),
                "\"this is\\\" the value\"".to_string(),
            ]
        );
        assert_eq!(
            tokenize_json(
                r#"
                Test.field="this*@# is\" the\" 
value"
            "#
            ),
            vec![
                "Test".to_string(),
                "field".to_string(),
                "\"this*@# is\\\" the\\\" \nvalue\"".to_string(),
            ]
        );
    }

    fn flattened_json_helper(json: &str, expected: &str) {
        let parsed: serde_json::Value = serde_json::from_str(json).unwrap();
        let flat = &FlattenedJson::new(&parsed).unwrap().flattened_json;

        assert_eq!(flat, expected);
    }

    #[test]
    fn flatten_json_object() {
        let json = r#"
        {
            "key1": "val1",
            "key2": "val2"
        }
        "#;
        let expected = r#"key1="val1"
key2="val2""#;

        flattened_json_helper(json, expected);

        let json = r#"
        {
            "key1": 1,
            "key2": 2
        }
        "#;
        let expected = r#"key1="1"
key2="2""#;

        flattened_json_helper(json, expected);

        let json = r#"
        {
            "key1": {
                "key2": "value1",
                "key3": "value2"
            }
        }
        "#;
        let expected = r#"key1.key2="value1"
key1.key3="value2""#;

        flattened_json_helper(json, expected);

        let json = r#"
        {
            "key1": [
                "value1",
                "value2"
            ]
        }
        "#;
        let expected = r#"key1="value1"
key1="value2""#;

        flattened_json_helper(json, expected);

        let json = r#"
        {
            "key1": [
                "value1",
                {
                    "key2": "value2",
                    "key3": 123
                }
            ]
        }
        "#;
        let expected = r#"key1="value1"
key1.key2="value2"
key1.key3="123""#;

        flattened_json_helper(json, expected);

        let json = r#"
        {
            "key1": [
                "value1",
                {
                    "key2": "this\" is @ a # test"
                }
            ]
        }
        "#;
        let expected = r#"key1="value1"
key1.key2="this\" is @ a # test""#;

        flattened_json_helper(json, expected);
    }

    #[test]
    fn han() {
        assert_eq!(
            tokenize_simple("test 漢.com"),
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
            tokenize_simple("test あ.com"),
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
            tokenize_simple("test ダ.com"),
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
            tokenize_simple("test б.com"),
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
            tokenize_simple("test ب.com"),
            vec![
                "test".to_string(),
                "ب".to_string(),
                ".".to_string(),
                "com".to_string()
            ]
        );
    }
}
