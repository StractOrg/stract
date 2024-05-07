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

use std::{array, collections::VecDeque, str::CharIndices};

use logos::{Lexer, Logos};
use tantivy::tokenizer::{
    BoxTokenStream, Language, LowerCaser, Stemmer, StopWordFilter, TextAnalyzer,
};

use whatlang::Lang;

use crate::{ceil_char_boundary, floor_char_boundary};

use self::{add_space_last::AddSpaceLast, split_preserve::StrSplitPreserve};

mod add_space_last;
mod split_preserve;

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
    SiteOperator(SiteOperatorUrlTokenizer),
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
            Tokenizer::SiteOperator(_) => SiteOperatorUrlTokenizer::as_str(),
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
    analyzer: Option<TextAnalyzer>,
}

impl Normal {
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

#[derive(Clone)]
pub struct BigramTokenizer {
    inner_tokenizer: Normal,
}

impl Default for BigramTokenizer {
    fn default() -> Self {
        Self {
            inner_tokenizer: Normal::with_stopwords(vec![".".to_string()]),
        }
    }
}

impl BigramTokenizer {
    pub fn as_str() -> &'static str {
        "bigram_tokenizer"
    }
}

#[derive(Clone)]
pub struct TrigramTokenizer {
    inner_tokenizer: Normal,
}

impl Default for TrigramTokenizer {
    fn default() -> Self {
        Self {
            inner_tokenizer: Normal::with_stopwords(vec![".".to_string()]),
        }
    }
}

impl TrigramTokenizer {
    pub fn as_str() -> &'static str {
        "trigram_tokenizer"
    }
}

#[derive(Clone, Default)]
pub struct Stemmed {
    force_language: Option<Lang>,
    analyzer: Option<TextAnalyzer>,
}

impl Stemmed {
    pub fn as_str() -> &'static str {
        "stemmed_tokenizer"
    }
    pub fn with_forced_language(lang: Lang) -> Self {
        Self {
            force_language: Some(lang),
            analyzer: None,
        }
    }
}

#[derive(Clone, Default, Debug)]
pub struct Identity {}

impl Identity {
    pub fn as_str() -> &'static str {
        "identity_tokenizer"
    }
}

impl tantivy::tokenizer::Tokenizer for Tokenizer {
    type TokenStream<'a> = BoxTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        match self {
            Tokenizer::Normal(tokenizer) => tokenizer.token_stream(text),
            Tokenizer::Stemmed(tokenizer) => tokenizer.token_stream(text),
            Tokenizer::Identity(tokenizer) => tokenizer.token_stream(text),
            Tokenizer::Json(tokenizer) => tokenizer.token_stream(text),
            Tokenizer::Bigram(tokenizer) => tokenizer.token_stream(text),
            Tokenizer::Trigram(tokenizer) => tokenizer.token_stream(text),
            Tokenizer::SiteOperator(tokenizer) => tokenizer.token_stream(text),
        }
    }
}

impl tantivy::tokenizer::Tokenizer for Normal {
    type TokenStream<'a> = BoxTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let builder = TextAnalyzer::builder(Simple).filter(LowerCaser);

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

impl tantivy::tokenizer::Tokenizer for Stemmed {
    type TokenStream<'a> = BoxTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let builder = TextAnalyzer::builder(Simple).filter(LowerCaser);

        let lang = match self.force_language {
            Some(lang) => Some(lang),
            None => whatlang::detect_lang(text),
        };

        self.analyzer = match lang {
            Some(lang) => Some(builder.filter(MyStemmer::from(lang).0).build()),
            None => Some(builder.build()),
        };

        self.analyzer.as_mut().unwrap().token_stream(text)
    }
}

impl tantivy::tokenizer::Tokenizer for Identity {
    type TokenStream<'a> = BoxTokenStream<'a>;

    fn token_stream<'a>(&mut self, text: &'a str) -> Self::TokenStream<'a> {
        BoxTokenStream::new(IdentityTokenStream::from(text.to_string()))
    }
}

impl tantivy::tokenizer::Tokenizer for BigramTokenizer {
    type TokenStream<'a> = BoxTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let inner_stream = self.inner_tokenizer.token_stream(text);
        let stream: NGramTokenStream<2> = NGramTokenStream::new(inner_stream);
        BoxTokenStream::new(stream)
    }
}

impl tantivy::tokenizer::Tokenizer for TrigramTokenizer {
    type TokenStream<'a> = BoxTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let inner = self.inner_tokenizer.token_stream(text);
        let stream: NGramTokenStream<3> = NGramTokenStream::new(inner);
        BoxTokenStream::new(stream)
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
#[logos(skip r"[ \t\n\f]+")]
enum Token {
    #[regex("[\\w|\\p{Han}|\\p{Hiragana}|\\p{Katakana}|\\p{Cyrillic}|\\p{Arabic}]+")]
    Text,
}

#[derive(Clone)]
pub struct Simple;

pub struct SimpleTokenStream<'a> {
    lexer: Lexer<'a, Token>,
    token: Option<tantivy::tokenizer::Token>,
    next_position: usize,
}

impl tantivy::tokenizer::Tokenizer for Simple {
    type TokenStream<'a> = BoxTokenStream<'a>;

    fn token_stream<'a>(&mut self, text: &'a str) -> Self::TokenStream<'a> {
        let lexer = Token::lexer(text);
        BoxTokenStream::new(SimpleTokenStream {
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

pub struct NGramTokenStream<'a, const N: usize> {
    inner: BoxTokenStream<'a>,
    token: tantivy::tokenizer::Token,
    token_window: [tantivy::tokenizer::Token; N],
    next_pos: usize,
}

impl<'a, const N: usize> NGramTokenStream<'a, N> {
    pub fn new(inner: BoxTokenStream<'a>) -> Self {
        Self {
            inner,
            token: tantivy::tokenizer::Token::default(),
            token_window: array::from_fn(|_| tantivy::tokenizer::Token::default()),
            next_pos: 0,
        }
    }
}

fn reuse_token_alloc(token: &mut tantivy::tokenizer::Token, new_token: &tantivy::tokenizer::Token) {
    token.text.clear();
    token.text += new_token.text.as_str();
    token.offset_from = new_token.offset_from;
    token.offset_to = new_token.offset_to;
    token.position = new_token.position;
    token.position_length = new_token.position_length;
}

impl<'a, const N: usize> tantivy::tokenizer::TokenStream for NGramTokenStream<'a, N> {
    fn advance(&mut self) -> bool {
        if !self.inner.advance() {
            return false;
        }

        self.token_window.rotate_left(1);
        reuse_token_alloc(&mut self.token_window[N - 1], self.inner.token());

        while self.token_window[0].text.is_empty() {
            if !self.inner.advance() {
                return false;
            }

            self.token_window.rotate_left(1);
            reuse_token_alloc(&mut self.token_window[N - 1], self.inner.token());
        }

        self.next_pos += 1;

        let begin = self
            .token_window
            .iter()
            .position(|token| !token.text.is_empty())
            .unwrap_or(N - 1);

        self.token.position = self.next_pos;
        self.token.offset_from = self.token_window[begin].offset_from;
        self.token.offset_to = self.token_window[N - 1].offset_to;
        self.token.position_length = N - begin;

        self.token.text.clear();
        for token in &self.token_window {
            self.token.text += token.text.as_str();
        }

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
    inner_tokenizer: JsonField,
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
            serde_json::Value::Null => {
                res.push(itertools::intersperse(elem.parent_keys, ".".to_string()).collect())
            }
            serde_json::Value::Bool(b) => {
                let key: String =
                    itertools::intersperse(elem.parent_keys, ".".to_string()).collect();
                res.push(format!("{key}=\"{b}\""))
            }
            serde_json::Value::Number(n) => {
                let key: String =
                    itertools::intersperse(elem.parent_keys, ".".to_string()).collect();
                res.push(format!("{key}=\"{n}\""))
            }
            serde_json::Value::String(s) => {
                let key: String =
                    itertools::intersperse(elem.parent_keys, ".".to_string()).collect();
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

        let flattened_json = itertools::intersperse(flatten(val), "\n".to_string()).collect();

        Ok(Self {
            flattened_json,
            inner_tokenizer: JsonField,
        })
    }

    pub fn token_stream(&mut self) -> BoxTokenStream {
        tantivy::tokenizer::Tokenizer::token_stream(&mut self.inner_tokenizer, &self.flattened_json)
    }

    pub fn text(&self) -> &str {
        &self.flattened_json
    }
}

#[derive(Clone, Debug)]
pub struct JsonField;

impl JsonField {
    pub fn as_str() -> &'static str {
        "json_tokenizer"
    }
}

impl tantivy::tokenizer::Tokenizer for JsonField {
    type TokenStream<'a> = BoxTokenStream<'a>;

    fn token_stream<'a>(&mut self, text: &'a str) -> Self::TokenStream<'a> {
        BoxTokenStream::new(JsonFieldTokenStream {
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
            if !matches!(c, '.' | '\n' | '"') {
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

#[derive(Clone, Default)]
struct ParsedUrl {
    protocol: Option<VecDeque<String>>,
    domain: Option<VecDeque<String>>,
    path: VecDeque<String>,
}

#[derive(Debug, Clone)]
pub struct SiteOperatorUrlTokenizer;

impl SiteOperatorUrlTokenizer {
    pub fn as_str() -> &'static str {
        "site_operator_url_tokenizer"
    }
}

impl tantivy::tokenizer::Tokenizer for SiteOperatorUrlTokenizer {
    type TokenStream<'a> = BoxTokenStream<'a>;

    fn token_stream<'a>(&mut self, text: &'a str) -> Self::TokenStream<'a> {
        let parsed_url = url::Url::parse(text)
            .or_else(|_| url::Url::parse(&format!("http://{}", text)))
            .map(|url| {
                let domain = Some(
                    url.host_str()
                        .unwrap_or("")
                        .split_preserve(|c| matches!(c, '.'))
                        .filter(|s| !(*s).is_empty())
                        .map(|s| s.to_string())
                        .add_space_last()
                        .collect(),
                );
                let path: VecDeque<_> = url
                    .path()
                    .split_preserve(|c| matches!(c, '/' | '-' | '_'))
                    .filter(|s| !(*s).is_empty())
                    .map(|s| s.to_string())
                    .collect();

                if matches!(url.scheme(), "http" | "https") {
                    ParsedUrl {
                        protocol: None,
                        domain,
                        path,
                    }
                } else {
                    let mut v = VecDeque::new();
                    v.push_back(url.scheme().to_string());

                    ParsedUrl {
                        protocol: Some(v),
                        domain,
                        path,
                    }
                }
            })
            .unwrap_or_default();

        BoxTokenStream::new(SiteOperatorUrlTokenStream {
            url: parsed_url,
            token: tantivy::tokenizer::Token::default(),
        })
    }
}

pub struct SiteOperatorUrlTokenStream {
    url: ParsedUrl,
    token: tantivy::tokenizer::Token,
}

impl tantivy::tokenizer::TokenStream for SiteOperatorUrlTokenStream {
    fn advance(&mut self) -> bool {
        if let Some(protocol) = self.url.protocol.as_mut() {
            self.token.position = self.token.position.wrapping_add(1);
            self.token.text.clear();

            if let Some(s) = protocol.pop_front() {
                self.token.text.push_str(&s);
                self.token.offset_from = 0;
                self.token.offset_to = s.len();
            } else {
                self.token.offset_from = self.token.offset_to;
                self.token.text.push_str("://");
                self.token.offset_to += self.token.text.len();

                self.url.protocol = None;
            }

            return true;
        }

        if let Some(domain) = self.url.domain.as_mut() {
            if let Some(s) = domain.pop_front() {
                self.token.text.clear();
                self.token.position = self.token.position.wrapping_add(1);

                self.token.text.push_str(&s);

                self.token.offset_from = self.token.offset_to;
                self.token.offset_to += self.token.text.len();
                return true;
            }
        }

        if let Some(s) = self.url.path.pop_front() {
            self.token.text.clear();
            self.token.position = self.token.position.wrapping_add(1);

            self.token.text.push_str(&s);
            self.token.offset_from = self.token.offset_to;
            self.token.offset_to += self.token.text.len();

            return true;
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
        let mut tokenizer = Normal::default();
        let mut stream = tokenizer.token_stream(s);

        while let Some(token) = stream.next() {
            res.push(token.text.clone());
        }

        res
    }

    fn tokenize_json(s: &str) -> Vec<String> {
        let mut res = Vec::new();
        let mut tokenizer = JsonField;
        let mut stream = tokenizer.token_stream(s);

        while let Some(token) = stream.next() {
            res.push(token.text.clone());
        }

        res
    }

    fn tokenize_bigram(s: &str) -> Vec<String> {
        let mut res = Vec::new();
        let mut tokenizer = Tokenizer::Bigram(BigramTokenizer::default());
        let mut stream = tokenizer.token_stream(s);

        while let Some(token) = stream.next() {
            res.push(token.text.clone());
        }

        res
    }

    fn tokenize_trigram(s: &str) -> Vec<String> {
        let mut res = Vec::new();

        let mut tokenizer = Tokenizer::Trigram(TrigramTokenizer::default());
        let mut stream = tokenizer.token_stream(s);

        while let Some(token) = stream.next() {
            res.push(token.text.clone());
        }

        res
    }

    fn tokenize_url(s: &str) -> Vec<String> {
        let mut res = Vec::new();
        let mut tokenizer = SiteOperatorUrlTokenizer;
        let mut stream = tokenizer.token_stream(s);

        while let Some(token) = stream.next() {
            res.push(token.text.clone());
        }

        res
    }

    fn tokenize_identity(s: &str) -> Vec<String> {
        let mut res = Vec::new();
        let mut tokenizer = Identity {};
        let mut stream = tokenizer.token_stream(s);

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
        assert_eq!(tokenize_simple("example.com"), vec!["example", ".", "com",]);
        assert_eq!(
            tokenize_simple("example. com"),
            vec!["example", ".", "com",]
        );
        assert_eq!(
            tokenize_simple("example . com"),
            vec!["example", ".", "com",]
        );

        assert_eq!(
            tokenize_simple("a c++ blog post"),
            vec!["a", "c", "+", "+", "blog", "post"]
        );
        assert_eq!(tokenize_simple("path/test"), vec!["path", "/", "test",]);
    }

    #[test]
    fn tokenize_json_field() {
        assert_eq!(
            tokenize_json(r#"Test.field="value""#),
            vec!["Test", "field", "\"value\"",]
        );
        assert_eq!(
            tokenize_json(r#"Test.field="this is the value""#),
            vec!["Test", "field", "\"this is the value\"",]
        );
        assert_eq!(
            tokenize_json(r#"Test.field="this is\" the value""#),
            vec!["Test", "field", "\"this is\\\" the value\"",]
        );
        assert_eq!(
            tokenize_json("Test.field=\"this*@# is\\\" the\\\" \nvalue\""),
            vec!["Test", "field", "\"this*@# is\\\" the\\\" \nvalue\"",]
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
            "$key1": {
                "$key2": "value1",
                "key3": "value2"
            }
        }
        "#;
        let expected = r#"$key1.$key2="value1"
$key1.key3="value2""#;

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

        assert_eq!(tokenize_bigram("this is"), vec!["thisis"]);
        assert_eq!(tokenize_bigram("this is a"), vec!["thisis", "isa",]);
        assert_eq!(
            tokenize_bigram("this is a test"),
            vec!["thisis", "isa", "atest",]
        );

        // '.' is a stopword
        assert_eq!(tokenize_bigram("this.is"), vec!["thisis"]);
    }

    #[test]
    fn trigram_tokenizer() {
        assert!(tokenize_trigram("").is_empty());
        assert!(tokenize_trigram("test").is_empty());
        assert!(tokenize_trigram("this is").is_empty());

        assert_eq!(tokenize_trigram("this is a"), vec!["thisisa",]);
        assert_eq!(
            tokenize_trigram("this is a test"),
            vec!["thisisa", "isatest",]
        );
    }

    #[test]
    fn han() {
        assert_eq!(
            tokenize_simple("test 漢.com"),
            vec!["test", "漢", ".", "com"]
        );
    }

    #[test]
    fn hiragana() {
        assert_eq!(
            tokenize_simple("test あ.com"),
            vec!["test", "あ", ".", "com"]
        );
    }

    #[test]
    fn katakana() {
        assert_eq!(
            tokenize_simple("test ダ.com"),
            vec!["test", "ダ", ".", "com"]
        );
    }

    #[test]
    fn cyrillic() {
        assert_eq!(tokenize_simple("test б.com"), vec!["test", "б", ".", "com"]);
    }

    #[test]
    fn arabic() {
        assert_eq!(tokenize_simple("test ب.com"), vec!["test", "ب", ".", "com"]);
    }

    #[test]
    fn url() {
        assert_eq!(
            tokenize_url("https://www.example.com"),
            vec!["www", ".", "example", ".", "com ",]
        );

        assert_eq!(
            tokenize_url("https://www.example.com/test"),
            vec!["www", ".", "example", ".", "com ", "/", "test",]
        );

        assert_eq!(tokenize_url("example.com"), vec!["example", ".", "com ",]);

        assert_eq!(
            tokenize_url("example.com/another/path"),
            vec!["example", ".", "com ", "/", "another", "/", "path",]
        );

        assert_eq!(tokenize_url(".com"), vec![".", "com ",])
    }

    #[test]
    fn identity() {
        assert_eq!(tokenize_identity("this is a test"), vec!["this is a test"]);
        assert_eq!(tokenize_identity("a-b"), vec!["a-b"]);
    }
}
