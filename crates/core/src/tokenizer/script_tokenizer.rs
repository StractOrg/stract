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

use super::{
    split_preserve::StrSplitPreserveWithRange,
    split_whitespace_with_range::SplitWhitespaceWithRange, Token,
};

pub trait ScriptTokenizer {
    fn tokenize<'a>(&self, text: &'a str) -> Box<dyn Iterator<Item = Token> + 'a>;
}

pub struct Latin;

impl ScriptTokenizer for Latin {
    fn tokenize<'a>(&self, text: &'a str) -> Box<dyn Iterator<Item = Token> + 'a> {
        Box::new(
            text.split_whitespace_with_range()
                .flat_map(|(txt, span)| {
                    let offset = span.start;
                    txt.split_preserve_with_range(|c| !c.is_alphabetic() && !c.is_numeric())
                        .map(move |(txt, span)| {
                            let span = offset + span.start..offset + span.end;
                            (txt, span)
                        })
                })
                .map(|(txt, span)| Token::new(txt.to_string(), span)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn test_latin() {
        let tokenizer = Latin;
        let txt = "Hello, world! 123";
        let tokens: Vec<_> = tokenizer.tokenize(txt).collect();
        assert_eq!(tokens.len(), 5);
        assert_eq!(tokens[0].text(), "Hello");
        assert_eq!(tokens[1].text(), ",");
        assert_eq!(tokens[2].text(), "world");
        assert_eq!(tokens[3].text(), "!");
        assert_eq!(tokens[4].text(), "123");
    }

    proptest! {
        #[test]
        fn prop_latin_correct_span(txt: String) {
            let tokenizer = Latin;
            let tokens: Vec<_> = tokenizer.tokenize(&txt).collect();
            for token in tokens {
                assert_eq!(&txt[token.span()], token.text());
            }
        }
    }
}
