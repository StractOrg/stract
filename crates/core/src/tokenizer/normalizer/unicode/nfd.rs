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

use std::iter;

use crate::tokenizer::normalizer::CharNormalizer;

pub struct UnicodeNFD;

impl CharNormalizer for UnicodeNFD {
    fn normalize(&self, c: char) -> impl Iterator<Item = char> {
        unicode_normalization::UnicodeNormalization::nfd(c)
    }

    fn should_normalize(&self, c: char) -> bool {
        match unicode_normalization::is_nfd_quick(iter::once(c)) {
            unicode_normalization::IsNormalized::No
            | unicode_normalization::IsNormalized::Maybe => true,
            unicode_normalization::IsNormalized::Yes => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::UnicodeNFD;
    use crate::tokenizer::normalizer::Normalizer;
    use crate::tokenizer::Token;

    #[test]
    fn test_nfd() {
        let normalizer = UnicodeNFD;
        let token = Token::new("ﬃ", 0..3);
        let normalized = normalizer.normalize(token.clone());
        assert_eq!(normalized.text(), "ﬃ");
        assert_eq!(normalized.span(), 0..3);

        let token = Token::new("ffi", 0..3);
        let normalized = normalizer.normalize(token.clone());
        assert_eq!(normalized.text(), "ffi");

        let token = Token::new("HÈLLÖ", 0..5);
        let normalized = normalizer.normalize(token.clone());
        assert_eq!(normalized.text(), "HE\u{300}LLO\u{308}");

        let token = Token::new("hello", 0..5);
        let normalized = normalizer.normalize(token.clone());
        assert_eq!(normalized.text(), "hello");

        let token = Token::new("café", 0..4);
        let normalized = normalizer.normalize(token.clone());
        assert_eq!(normalized.text(), "cafe\u{301}");
    }
}
