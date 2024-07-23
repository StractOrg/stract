// Stract is an open source web search engine.
// Copyright (C) 2023 Stract ApS
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

use super::Normalizer;
use crate::tokenizer::Token;

pub struct Lowercase;

impl<'a> Normalizer<'a> for Lowercase {
    fn normalize(&self, mut token: Token<'a>) -> Token<'a> {
        if token.text().is_ascii() {
            token.mut_text().make_ascii_lowercase();
            token
        } else {
            Token::new(token.text().to_lowercase(), token.span())
        }
    }

    fn should_normalize(&self, token: &Token<'a>) -> bool {
        token.text().chars().any(|c| c.is_uppercase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lowercase() {
        let normalizer = Lowercase;
        let token = Token::new("Hello", 0..5);
        let normalized = normalizer.normalize(token.clone());
        assert_eq!(normalized.text(), "hello");
        assert_eq!(normalized.span(), 0..5);
        assert!(normalizer.should_normalize(&token));

        let token = Token::new("hello", 0..5);
        let normalized = normalizer.normalize(token.clone());
        assert_eq!(normalized.text(), "hello");

        let token = Token::new("HÈLLÖ", 0..5);
        let normalized = normalizer.normalize(token.clone());
        assert_eq!(normalized.text(), "hèllö");
    }
}
