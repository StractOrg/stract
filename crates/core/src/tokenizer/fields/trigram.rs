use tantivy::tokenizer::BoxTokenStream;

use super::{default::DefaultTokenizer, ngram::NGramTokenStream};

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
#[derive(Clone)]
pub struct TrigramTokenizer {
    inner_tokenizer: DefaultTokenizer,
}

impl Default for TrigramTokenizer {
    fn default() -> Self {
        Self {
            inner_tokenizer: DefaultTokenizer::with_stopwords(vec![]),
        }
    }
}

impl TrigramTokenizer {
    pub fn as_str() -> &'static str {
        "trigram_tokenizer"
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

#[cfg(test)]
mod tests {
    use tantivy::tokenizer::Tokenizer;

    use super::*;

    fn tokenize_trigram(s: &str) -> Vec<String> {
        let mut res = Vec::new();

        let mut tokenizer = TrigramTokenizer::default();
        let mut stream = tokenizer.token_stream(s);

        while let Some(token) = stream.next() {
            res.push(token.text.clone());
        }

        res
    }

    #[test]
    fn trigram_tokenizer() {
        assert!(tokenize_trigram("").is_empty());
        assert!(tokenize_trigram("test").is_empty());
        assert!(tokenize_trigram("this is").is_empty());

        assert_eq!(tokenize_trigram("this is a"), vec!["thisisa",]);
        assert_eq!(
            tokenize_trigram("this is a test"),
            vec!["thisisa", "isatest"]
        );
    }
}
