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

use crate::tokenizer::normalizer::CharNormalizer;

#[inline]
fn is_diacritic(c: char) -> bool {
    matches!(c,
      '\u{0300}'..='\u{036F}'
      | '\u{1AB0}'..='\u{1AFF}'
      | '\u{1DC0}'..='\u{1DFF}'
      | '\u{20D0}'..='\u{20FF}'
      | '\u{FE20}'..='\u{FE2F}')
}

pub struct UnicodeDiacritics;

impl CharNormalizer for UnicodeDiacritics {
    fn normalize(&self, c: char) -> impl Iterator<Item = char> {
        if is_diacritic(c) {
            None.into_iter()
        } else {
            Some(c).into_iter()
        }
    }

    fn should_normalize(&self, c: char) -> bool {
        is_diacritic(c)
    }
}
