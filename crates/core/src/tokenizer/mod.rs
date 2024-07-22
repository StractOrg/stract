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

mod add_space_last;
pub mod fields;
mod script;
mod script_tokenizer;
mod segmenter;
mod split_preserve;
mod split_whitespace_with_range;
mod stemmer;

pub use fields::FieldTokenizer;

use self::segmenter::Segmenter;

#[derive(Debug)]
pub struct Token {
    text: String,
    span: std::ops::Range<usize>,
}

impl Token {
    pub fn new(text: String, span: std::ops::Range<usize>) -> Self {
        Token { text, span }
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn span(&self) -> std::ops::Range<usize> {
        self.span.clone()
    }

    pub fn offset(&mut self, offset: usize) {
        self.span = self.span.start + offset..self.span.end + offset;
    }
}

pub trait Tokenize {
    fn tokenize(&self) -> impl Iterator<Item = Token> + '_;
}

impl Tokenize for str {
    fn tokenize(&self) -> impl Iterator<Item = Token> + '_ {
        self.segments().flat_map(|segment| segment.tokenize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn test_tokenizer() {
        let input = "Hello, world! This is a test.";

        let tokens: Vec<_> = input.tokenize().collect();
        assert_eq!(tokens.len(), 9);

        assert_eq!(tokens[0].text(), "Hello");
        assert_eq!(tokens[1].text(), ",");
        assert_eq!(tokens[2].text(), "world");
        assert_eq!(tokens[3].text(), "!");
        assert_eq!(tokens[4].text(), "This");
        assert_eq!(tokens[5].text(), "is");
        assert_eq!(tokens[6].text(), "a");
        assert_eq!(tokens[7].text(), "test");
        assert_eq!(tokens[8].text(), ".");
    }

    proptest! {
        #[test]
        fn prop_tokenizer_correct_span(txt: String) {
            let tokens: Vec<_> = txt.tokenize().collect();
            for token in tokens {
                assert_eq!(&txt[token.span()], token.text());
            }
        }
    }
}
