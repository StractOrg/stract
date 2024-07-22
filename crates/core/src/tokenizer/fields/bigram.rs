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

use super::{default::DefaultTokenizer, ngram::NGramTokenStream};

#[derive(Clone)]
pub struct BigramTokenizer {
    inner_tokenizer: DefaultTokenizer,
}

impl Default for BigramTokenizer {
    fn default() -> Self {
        Self {
            inner_tokenizer: DefaultTokenizer::with_stopwords(vec![]),
        }
    }
}

impl BigramTokenizer {
    pub fn as_str() -> &'static str {
        "bigram_tokenizer"
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

#[cfg(test)]
mod tests {
    use tantivy::tokenizer::Tokenizer;

    use super::*;
    fn tokenize_bigram(s: &str) -> Vec<String> {
        let mut res = Vec::new();
        let mut tokenizer = BigramTokenizer::default();
        let mut stream = tokenizer.token_stream(s);

        while let Some(token) = stream.next() {
            res.push(token.text.clone());
        }

        res
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

        assert_eq!(tokenize_bigram("this.is"), vec!["this.", ".is"]);
    }
}
