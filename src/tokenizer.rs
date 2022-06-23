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

use tantivy::tokenizer::{
    Language, LowerCaser, PreTokenizedStream, PreTokenizedString, SimpleTokenizer, Stemmer,
    TextAnalyzer, TokenStream,
};
use whatlang::Lang;

struct TokenStreamMerger<'a> {
    streams: Vec<tantivy::tokenizer::BoxTokenStream<'a>>,
    current_stream: usize,
}

impl<'a> TokenStreamMerger<'a> {
    fn merge(streams: Vec<tantivy::tokenizer::BoxTokenStream<'a>>) -> Self {
        Self {
            streams,
            current_stream: 0,
        }
    }
}

impl<'a> tantivy::tokenizer::TokenStream for TokenStreamMerger<'a> {
    fn advance(&mut self) -> bool {
        if let Some(stream) = self.streams.get_mut(self.current_stream) {
            let mut has_more = stream.advance();

            if !has_more {
                self.current_stream += 1;
                has_more = self.current_stream < self.streams.len()
                    && self.streams[self.current_stream].advance();
            }

            has_more
        } else {
            false
        }
    }

    fn token(&self) -> &tantivy::tokenizer::Token {
        self.streams[self.current_stream].token()
    }

    fn token_mut(&mut self) -> &mut tantivy::tokenizer::Token {
        self.streams[self.current_stream].token_mut()
    }
}

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

#[derive(Clone, Default)]
pub struct Tokenizer {}

impl tantivy::tokenizer::Tokenizer for Tokenizer {
    fn token_stream<'a>(&self, text: &'a str) -> tantivy::tokenizer::BoxTokenStream<'a> {
        let analyzer = TextAnalyzer::from(SimpleTokenizer).filter(LowerCaser);

        let mut streams = vec![analyzer.token_stream(text)];

        if let Some(lang) = whatlang::detect_lang(text) {
            let stemmed_analyser = analyzer.clone().filter(MyStemmer::from(lang).0);
            streams.push(stemmed_analyser.token_stream(text));
        }

        let mut merger = TokenStreamMerger::merge(streams);

        let mut tokens = Vec::new();
        while let Some(token) = merger.next() {
            tokens.push(token.clone());
        }

        tokens.sort_by(|a, b| {
            a.offset_from
                .partial_cmp(&b.offset_from)
                .unwrap_or_else(|| a.offset_to.cmp(&b.offset_to))
        });

        tantivy::tokenizer::BoxTokenStream::from(PreTokenizedStream::from(PreTokenizedString {
            text: text.to_string(),
            tokens,
        }))
    }
}
