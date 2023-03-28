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

use std::{array, ops::Range, str::CharIndices};

use logos::{Lexer, Logos};
use tantivy::tokenizer::{
    BoxTokenStream, Language, LowerCaser, Stemmer, StopWordFilter, TextAnalyzer,
};

use whatlang::Lang;

use crate::{ceil_char_boundary, floor_char_boundary};

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
    Bigram(BigramTokenizer),
    Trigram(TrigramTokenizer),
    Json(JsonField),
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
            Tokenizer::Bigram(_) => BigramTokenizer::as_str(),
            Tokenizer::Trigram(_) => TrigramTokenizer::as_str(),
            Tokenizer::Json(_) => JsonField::as_str(),
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
pub struct BigramTokenizer {}

impl BigramTokenizer {
    pub fn as_str() -> &'static str {
        "bigram_tokenizer"
    }
}

#[derive(Clone, Default)]
pub struct TrigramTokenizer {}

impl TrigramTokenizer {
    pub fn as_str() -> &'static str {
        "trigram_tokenizer"
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
            Tokenizer::Json(tokenizer) => tokenizer.token_stream(text),
            Tokenizer::Bigram(tokenizer) => tokenizer.token_stream(text),
            Tokenizer::Trigram(tokenizer) => tokenizer.token_stream(text),
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

impl tantivy::tokenizer::Tokenizer for BigramTokenizer {
    fn token_stream<'a>(&self, text: &'a str) -> tantivy::tokenizer::BoxTokenStream<'a> {
        let inner = Normal::default().token_stream(text);
        let stream: NGramTokenStream<2> = NGramTokenStream::new(text, inner);
        BoxTokenStream::from(stream)
    }
}

impl tantivy::tokenizer::Tokenizer for TrigramTokenizer {
    fn token_stream<'a>(&self, text: &'a str) -> tantivy::tokenizer::BoxTokenStream<'a> {
        let inner = Normal::default().token_stream(text);
        let stream: NGramTokenStream<3> = NGramTokenStream::new(text, inner);
        BoxTokenStream::from(stream)
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

#[derive(Default)]
struct TokenRef {
    range: Range<usize>,
    position: usize,
}

impl From<&tantivy::tokenizer::Token> for TokenRef {
    fn from(token: &tantivy::tokenizer::Token) -> Self {
        Self {
            range: token.offset_from..token.offset_to,
            position: token.position,
        }
    }
}

struct NGramTokenStream<'a, const N: usize> {
    inner: BoxTokenStream<'a>,
    text: &'a str,
    token: tantivy::tokenizer::Token,
    token_refs: [TokenRef; N],
    is_first: bool,
    next_pos: usize,
}

impl<'a, const N: usize> NGramTokenStream<'a, N> {
    fn new(text: &'a str, inner: BoxTokenStream<'a>) -> Self {
        Self {
            inner,
            text,
            token: tantivy::tokenizer::Token::default(),
            token_refs: array::from_fn(|_| TokenRef::default()),
            is_first: true,
            next_pos: 0,
        }
    }
}

impl<'a> tantivy::tokenizer::TokenStream for NGramTokenStream<'a, 1> {
    fn advance(&mut self) -> bool {
        self.is_first = false;
        let res = self.inner.advance();

        if res {
            self.token_refs[0].range = self.inner.token().offset_from..self.inner.token().offset_to;
            self.token_refs[0].position = self.next_pos;
            self.next_pos += 1;
        }

        self.token = tantivy::tokenizer::Token {
            offset_from: self.token_refs[0].range.start,
            offset_to: self.token_refs[0].range.end,
            position: self.token_refs[0].position,
            text: self.text[self.token_refs[0].range.clone()].to_string(),
            position_length: 1,
        };

        res
    }

    fn token(&self) -> &tantivy::tokenizer::Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut tantivy::tokenizer::Token {
        &mut self.token
    }
}

impl<'a> tantivy::tokenizer::TokenStream for NGramTokenStream<'a, 2> {
    fn advance(&mut self) -> bool {
        if self.is_first {
            if !self.inner.advance() {
                return false;
            }

            self.token_refs[0] = self.inner.token().into();

            if !self.inner.advance() {
                return false;
            }
            self.token_refs[1] = self.inner.token().into();
        } else {
            if !self.inner.advance() {
                return false;
            }

            self.token_refs.rotate_left(1);
            self.token_refs[1] = self.inner.token().into();
        }
        self.is_first = false;

        self.next_pos += 1;

        self.token.position = self.next_pos;
        self.token.text =
            self.text[self.token_refs[0].range.start..self.token_refs[1].range.end].to_string();
        self.token.position_length = 2;
        true
    }

    fn token(&self) -> &tantivy::tokenizer::Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut tantivy::tokenizer::Token {
        &mut self.token
    }
}

impl<'a> tantivy::tokenizer::TokenStream for NGramTokenStream<'a, 3> {
    fn advance(&mut self) -> bool {
        if self.is_first {
            if !self.inner.advance() {
                return false;
            }

            self.token_refs[0] = self.inner.token().into();

            if !self.inner.advance() {
                return false;
            }
            self.token_refs[1] = self.inner.token().into();

            if !self.inner.advance() {
                return false;
            }
            self.token_refs[2] = self.inner.token().into();
        } else {
            if !self.inner.advance() {
                return false;
            }

            self.token_refs.rotate_left(1);
            self.token_refs[2] = self.inner.token().into();
        }
        self.is_first = false;

        self.next_pos += 1;

        self.token.position = self.next_pos;
        self.token.text =
            self.text[self.token_refs[0].range.start..self.token_refs[2].range.end].to_string();
        self.token.position_length = 3;
        true
    }

    fn token(&self) -> &tantivy::tokenizer::Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut tantivy::tokenizer::Token {
        &mut self.token
    }
}

pub struct FlattenedJson {
    flattened_json: String,
}

struct IntermediateFlatValue {
    parent_keys: Vec<String>,
    val: serde_json::Value,
}

fn flatten(val: serde_json::Value) -> Vec<String> {
    let mut res = Vec::new();

    let mut stack = Vec::new();
    stack.push(IntermediateFlatValue {
        parent_keys: Vec::new(),
        val,
    });

    while let Some(elem) = stack.pop() {
        match elem.val {
            serde_json::Value::Null => res.push(
                itertools::intersperse(elem.parent_keys.into_iter(), ".".to_string()).collect(),
            ),
            serde_json::Value::Bool(b) => {
                let key: String =
                    itertools::intersperse(elem.parent_keys.into_iter(), ".".to_string()).collect();
                res.push(format!("{key}=\"{b}\""))
            }
            serde_json::Value::Number(n) => {
                let key: String =
                    itertools::intersperse(elem.parent_keys.into_iter(), ".".to_string()).collect();
                res.push(format!("{key}=\"{n}\""))
            }
            serde_json::Value::String(s) => {
                let key: String =
                    itertools::intersperse(elem.parent_keys.into_iter(), ".".to_string()).collect();
                res.push(format!("{key}=\"{}\"", s.replace('"', "\\\"")))
            }
            serde_json::Value::Array(arr) => {
                for item in arr {
                    stack.push(IntermediateFlatValue {
                        parent_keys: elem.parent_keys.clone(),
                        val: item,
                    });
                }
            }
            serde_json::Value::Object(map) => {
                for (key, val) in map {
                    let mut parent_keys = elem.parent_keys.clone();
                    parent_keys.push(key);

                    stack.push(IntermediateFlatValue { parent_keys, val });
                }
            }
        }
    }

    res.reverse();

    res
}

impl FlattenedJson {
    pub fn new<T>(value: &T) -> crate::Result<Self>
    where
        T: serde::Serialize,
    {
        let json = serde_json::to_string(value)?;
        let val: serde_json::Value = serde_json::from_str(&json)?;

        let flattened_json =
            itertools::intersperse(flatten(val).into_iter(), "\n".to_string()).collect();

        Ok(Self { flattened_json })
    }

    pub fn token_stream(&self) -> BoxTokenStream {
        tantivy::tokenizer::Tokenizer::token_stream(&JsonField, &self.flattened_json)
    }

    pub fn text(&self) -> &str {
        &self.flattened_json
    }
}

#[derive(Clone)]
pub struct JsonField;

impl JsonField {
    pub fn as_str() -> &'static str {
        "json_tokenizer"
    }
}

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

                    self.token.offset_from = floor_char_boundary(self.text, self.token.offset_from);
                    self.token.offset_to =
                        ceil_char_boundary(self.text, self.token.offset_to).min(self.text.len());
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
    use tantivy::tokenizer::Tokenizer as _;

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

    fn tokenize_bigram(s: &str) -> Vec<String> {
        let mut res = Vec::new();
        let mut stream = Tokenizer::Bigram(BigramTokenizer::default()).token_stream(s);

        while let Some(token) = stream.next() {
            res.push(token.text.clone());
        }

        res
    }

    fn tokenize_trigram(s: &str) -> Vec<String> {
        let mut res = Vec::new();
        let mut stream = Tokenizer::Trigram(TrigramTokenizer::default()).token_stream(s);

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
    fn out_of_bounds_crash() {
        tokenize_json(
            r#"
Breadcrumb.title="Home"
Breadcrumb.url="https://www.eurotecnicaservice.it/?lang=en"
Breadcrumb.title="Fuser Pur"
Breadcrumb.url="https://www.eurotecnicaservice.it/testing\"
"#,
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
    fn bigram_tokenizer() {
        assert!(tokenize_bigram("").is_empty());
        assert!(tokenize_bigram("test").is_empty());

        assert_eq!(tokenize_bigram("this is"), vec!["this is".to_string()]);
        assert_eq!(
            tokenize_bigram("this is a"),
            vec!["this is".to_string(), "is a".to_string()]
        );
        assert_eq!(
            tokenize_bigram("this is a test"),
            vec![
                "this is".to_string(),
                "is a".to_string(),
                "a test".to_string()
            ]
        );
    }

    #[test]
    fn trigram_tokenizer() {
        assert!(tokenize_trigram("").is_empty());
        assert!(tokenize_trigram("test").is_empty());
        assert!(tokenize_trigram("this is").is_empty());

        assert_eq!(tokenize_trigram("this is a"), vec!["this is a".to_string()]);
        assert_eq!(
            tokenize_trigram("this is a test"),
            vec!["this is a".to_string(), "is a test".to_string(),]
        );
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
