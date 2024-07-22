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

pub trait SplitWhitespaceWithRange {
    fn split_whitespace_with_range(&self) -> SplitWhitespaceWithRangeIter;
}

pub struct SplitWhitespaceWithRangeIter<'a> {
    s: &'a str,
    start: usize,
}

impl<'a> SplitWhitespaceWithRangeIter<'a> {
    fn new(s: &'a str) -> Self {
        Self { s, start: 0 }
    }
}

impl<'a> Iterator for SplitWhitespaceWithRangeIter<'a> {
    type Item = (&'a str, std::ops::Range<usize>);

    fn next(&mut self) -> Option<Self::Item> {
        for c in self.s[self.start..].chars() {
            if !c.is_whitespace() {
                break;
            }
            self.start += c.len_utf8();
        }

        if self.start >= self.s.len() {
            return None;
        }

        let start = self.s[self.start..].find(|c: char| !c.is_whitespace())?;
        let start = self.start + start;
        let end = self.s[start..]
            .find(char::is_whitespace)
            .map(|end| start + end)
            .unwrap_or(self.s.len());
        let range = start..end;
        self.start = end;
        Some((&self.s[range.clone()], range))
    }
}

impl SplitWhitespaceWithRange for str {
    fn split_whitespace_with_range(&self) -> SplitWhitespaceWithRangeIter {
        SplitWhitespaceWithRangeIter::new(self)
    }
}

impl SplitWhitespaceWithRange for String {
    fn split_whitespace_with_range(&self) -> SplitWhitespaceWithRangeIter {
        SplitWhitespaceWithRangeIter::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn test_split_whitespace_with_range() {
        let txt = "Hello, world! 123";
        let tokens: Vec<_> = txt.split_whitespace_with_range().collect();
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0], ("Hello,", 0..6));
        assert_eq!(tokens[1], ("world!", 7..13));
        assert_eq!(tokens[2], ("123", 14..17));
    }

    #[test]
    fn test_split_whitespace_with_range_empty() {
        let txt = "";
        let tokens: Vec<_> = txt.split_whitespace_with_range().collect();
        assert_eq!(tokens.len(), 0);
    }

    #[test]
    fn test_multi_whitespace() {
        let txt = "Hello,   world! 123";
        let tokens: Vec<_> = txt.split_whitespace_with_range().collect();
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0], ("Hello,", 0..6));
        assert_eq!(tokens[1], ("world!", 9..15));
        assert_eq!(tokens[2], ("123", 16..19));
    }

    proptest! {
        #[test]
        fn prop_split_whitespace_with_range(s: String) {
            let tokens: Vec<_> = s.split_whitespace_with_range().collect();
            for (txt, range) in tokens {
                assert_eq!(&s[range.clone()], txt);
            }
        }
    }
}
