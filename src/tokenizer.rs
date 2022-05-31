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
#[derive(Clone, Default)]
pub struct Tokenizer {}

impl tantivy::tokenizer::Tokenizer for Tokenizer {
    fn token_stream<'a>(&self, text: &'a str) -> tantivy::tokenizer::BoxTokenStream<'a> {
        let mut analyzer = TextAnalyzer::from(SimpleTokenizer).filter(LowerCaser);

        // TODO:
        // Use whatlang to determine which stemmer to use.
        // We will somehow need to search for the non-stemmed version
        // of the string, as whatlang might be wrong which will result in
        // wrong stemming. I tried merging multiple token streams but it
        // caused tantivy to crash. A solution might be to create a separate index
        // for stem and non-stemmed and do merging in the searcher. This
        // might use too much space.
        analyzer = analyzer.filter(Stemmer::new(Language::English));

        analyzer.token_stream(text.clone())
    }
}
