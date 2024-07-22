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

use tantivy::tokenizer::{BoxTokenStream, LowerCaser, TextAnalyzer};
use whatlang::Lang;

use crate::tokenizer::stemmer::Stemmer;

use super::default::Normal;

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
impl tantivy::tokenizer::Tokenizer for Stemmed {
    type TokenStream<'a> = BoxTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let builder = TextAnalyzer::builder(Normal).filter(LowerCaser);

        let lang = match self.force_language {
            Some(lang) => Some(lang),
            None => whatlang::detect_lang(text),
        };

        self.analyzer = match lang {
            Some(lang) => Some(builder.filter(Stemmer::from(lang).into_tantivy()).build()),
            None => Some(builder.build()),
        };

        self.analyzer.as_mut().unwrap().token_stream(text)
    }
}
