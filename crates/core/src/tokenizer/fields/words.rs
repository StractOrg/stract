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

use tantivy::tokenizer::{BoxTokenStream, TextAnalyzer};

use super::pred::PredTokenizer;

#[derive(Clone, Default)]
pub struct WordTokenizer {
    analyzer: Option<TextAnalyzer>,
}

impl WordTokenizer {
    pub fn as_str() -> &'static str {
        "word"
    }
}

impl tantivy::tokenizer::Tokenizer for WordTokenizer {
    type TokenStream<'a> = BoxTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        let builder = TextAnalyzer::builder(PredTokenizer(|c| c.is_whitespace()));

        self.analyzer = Some(builder.build());

        self.analyzer.as_mut().unwrap().token_stream(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lending_iter::LendingIterator;
    use tantivy::tokenizer::Tokenizer as _;

    fn tokenize(s: &str) -> Vec<String> {
        let mut res = Vec::new();
        let mut tokenizer = WordTokenizer::default();
        let mut stream = tokenizer.token_stream(s);
        let mut it = tantivy::tokenizer::TokenStream::iter(&mut stream);

        while let Some(token) = it.next() {
            res.push(token.text.clone());
        }

        res
    }

    #[test]
    fn test_words_tokenizer() {
        assert!(tokenize("").is_empty());
        assert_eq!(tokenize("a b"), vec!["a", "b"]);
        assert_eq!(tokenize("a b "), vec!["a", "b"]);
        assert_eq!(tokenize(" a b "), vec!["a", "b"]);
        assert_eq!(tokenize("a b c"), vec!["a", "b", "c"]);
    }
}
