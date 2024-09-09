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

use tantivy::tokenizer::BoxTokenStream;

pub use self::{
    bigram::BigramTokenizer, default::DefaultTokenizer, identity::Identity, json::FlattenedJson,
    json::JsonField, split_newlines::NewlineTokenizer, stemmed::Stemmed, trigram::TrigramTokenizer,
    url::UrlTokenizer, words::WordTokenizer,
};

mod default;
mod identity;
mod json;
mod pred;
mod split_newlines;
mod stemmed;
mod url;
mod words;

mod bigram;
mod ngram;
mod trigram;

#[derive(Clone)]
pub enum FieldTokenizer {
    Default(DefaultTokenizer),
    Identity(Identity),
    Stemmed(Stemmed),
    Bigram(BigramTokenizer),
    Trigram(TrigramTokenizer),
    Json(JsonField),
    Url(UrlTokenizer),
    Newline(NewlineTokenizer),
    Words(WordTokenizer),
}

impl FieldTokenizer {
    pub fn as_str(&self) -> &'static str {
        match self {
            FieldTokenizer::Default(_) => DefaultTokenizer::as_str(),
            FieldTokenizer::Stemmed(_) => Stemmed::as_str(),
            FieldTokenizer::Identity(_) => Identity::as_str(),
            FieldTokenizer::Bigram(_) => BigramTokenizer::as_str(),
            FieldTokenizer::Trigram(_) => TrigramTokenizer::as_str(),
            FieldTokenizer::Json(_) => JsonField::as_str(),
            FieldTokenizer::Url(_) => UrlTokenizer::as_str(),
            FieldTokenizer::Newline(_) => NewlineTokenizer::as_str(),
            FieldTokenizer::Words(_) => WordTokenizer::as_str(),
        }
    }
}

impl From<Stemmed> for FieldTokenizer {
    fn from(stemmed: Stemmed) -> Self {
        Self::Stemmed(stemmed)
    }
}

impl Default for FieldTokenizer {
    fn default() -> Self {
        Self::Default(DefaultTokenizer::default())
    }
}
impl tantivy::tokenizer::Tokenizer for FieldTokenizer {
    type TokenStream<'a> = BoxTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        match self {
            FieldTokenizer::Default(tokenizer) => tokenizer.token_stream(text),
            FieldTokenizer::Stemmed(tokenizer) => tokenizer.token_stream(text),
            FieldTokenizer::Identity(tokenizer) => tokenizer.token_stream(text),
            FieldTokenizer::Json(tokenizer) => tokenizer.token_stream(text),
            FieldTokenizer::Bigram(tokenizer) => tokenizer.token_stream(text),
            FieldTokenizer::Trigram(tokenizer) => tokenizer.token_stream(text),
            FieldTokenizer::Url(tokenizer) => tokenizer.token_stream(text),
            FieldTokenizer::Newline(tokenizer) => tokenizer.token_stream(text),
            FieldTokenizer::Words(tokenizer) => tokenizer.token_stream(text),
        }
    }
}
