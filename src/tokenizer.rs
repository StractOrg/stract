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

use tantivy::tokenizer::{Language, LowerCaser, SimpleTokenizer, Stemmer, TextAnalyzer};
use whatlang::Lang;

struct MyStemmer(Stemmer);

impl From<Lang> for MyStemmer {
    fn from(lang: Lang) -> Self {
        match lang {
            Lang::Dan => MyStemmer(Stemmer::new(Language::Danish)),
            Lang::Ara => MyStemmer(Stemmer::new(Language::Arabic)),
            Lang::Nld => MyStemmer(Stemmer::new(Language::Dutch)),
            Lang::Eng => MyStemmer(Stemmer::new(Language::English)),
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
    NormalTokenizer(NormalTokenizer),
    StemmedTokenizer(StemmedTokenizer),
}

impl Tokenizer {
    pub fn new_stemmed() -> Self {
        Self::StemmedTokenizer(Default::default())
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            Tokenizer::NormalTokenizer(_) => NormalTokenizer::as_str(),
            Tokenizer::StemmedTokenizer(_) => StemmedTokenizer::as_str(),
        }
    }
}

impl Default for Tokenizer {
    fn default() -> Self {
        Self::NormalTokenizer(Default::default())
    }
}

#[derive(Clone, Default)]
pub struct NormalTokenizer {}

impl NormalTokenizer {
    pub fn as_str() -> &'static str {
        "tokenizer"
    }
}

#[derive(Clone, Default)]
pub struct StemmedTokenizer {}

impl StemmedTokenizer {
    pub fn as_str() -> &'static str {
        "stemmed_tokenizer"
    }
}

impl tantivy::tokenizer::Tokenizer for Tokenizer {
    fn token_stream<'a>(&self, text: &'a str) -> tantivy::tokenizer::BoxTokenStream<'a> {
        match self {
            Tokenizer::NormalTokenizer(tokenizer) => tokenizer.token_stream(text),
            Tokenizer::StemmedTokenizer(tokenizer) => tokenizer.token_stream(text),
        }
    }
}

impl tantivy::tokenizer::Tokenizer for NormalTokenizer {
    fn token_stream<'a>(&self, text: &'a str) -> tantivy::tokenizer::BoxTokenStream<'a> {
        TextAnalyzer::from(SimpleTokenizer)
            .filter(LowerCaser)
            .token_stream(text)
    }
}

impl tantivy::tokenizer::Tokenizer for StemmedTokenizer {
    fn token_stream<'a>(&self, text: &'a str) -> tantivy::tokenizer::BoxTokenStream<'a> {
        let analyzer = TextAnalyzer::from(SimpleTokenizer).filter(LowerCaser);
        if let Some(lang) = whatlang::detect_lang(text) {
            analyzer.filter(MyStemmer::from(lang).0).token_stream(text)
        } else {
            analyzer.token_stream(text)
        }
    }
}
