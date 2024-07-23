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

mod lowercase;
mod unicode;

pub use lowercase::Lowercase;
pub use unicode::diacritics::UnicodeDiacritics;
pub use unicode::nfd::UnicodeNFD;
pub use unicode::nfkc::UnicodeNFKC;
pub use unicode::nfkd::UnicodeNFKD;

use super::Token;

pub trait Normalizer<'a> {
    fn normalize(&self, token: Token<'a>) -> Token<'a>;
    fn should_normalize(&self, token: &Token<'a>) -> bool;
}

pub trait CharNormalizer {
    fn normalize(&self, c: char) -> impl Iterator<Item = char>;
    fn should_normalize(&self, c: char) -> bool;
}

impl<'a, T> Normalizer<'a> for T
where
    T: CharNormalizer,
{
    fn normalize(&self, token: Token<'a>) -> Token<'a> {
        let text = token
            .text()
            .chars()
            .flat_map(|c| self.normalize(c))
            .collect::<String>();

        Token::new(text, token.span())
    }

    fn should_normalize(&self, token: &Token<'a>) -> bool {
        token.text().chars().any(|c| self.should_normalize(c))
    }
}
