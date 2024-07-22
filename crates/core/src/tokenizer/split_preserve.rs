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

pub struct SplitPreserveWithRange<'a, P>
where
    P: Fn(char) -> bool,
{
    s: &'a str,
    pred: P,
    start: usize,
    last_pred: Option<char>,
}

impl<'a, P> SplitPreserveWithRange<'a, P>
where
    P: Fn(char) -> bool,
{
    fn new(s: &'a str, pred: P) -> Self {
        Self {
            s,
            pred,
            start: 0,
            last_pred: None,
        }
    }
}

impl<'a, P> Iterator for SplitPreserveWithRange<'a, P>
where
    P: Fn(char) -> bool,
{
    type Item = (&'a str, std::ops::Range<usize>);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(c) = self.last_pred.take() {
            let range = self.start - c.len_utf8()..self.start;
            return Some((&self.s[range.clone()], range));
        }

        if self.start >= self.s.len() {
            return None;
        }

        for (i, c) in self.s[self.start..].char_indices() {
            if (self.pred)(c) {
                let range = self.start..self.start + i;
                let res = &self.s[range.clone()];
                self.start += i + c.len_utf8();

                if i == 0 {
                    let range = self.start - c.len_utf8()..self.start;
                    return Some((&self.s[range.clone()], range));
                }

                self.last_pred = Some(c);

                return Some((res, range));
            }
        }

        if self.start < self.s.len() {
            let range = self.start..self.s.len();
            let res = &self.s[range.clone()];

            self.start = self.s.len();

            Some((res, range))
        } else {
            None
        }
    }
}

pub struct SplitPreserve<'a, P>
where
    P: Fn(char) -> bool,
{
    inner: SplitPreserveWithRange<'a, P>,
}

impl<'a, P> SplitPreserve<'a, P>
where
    P: Fn(char) -> bool,
{
    fn new(s: &'a str, pred: P) -> Self {
        Self {
            inner: SplitPreserveWithRange::new(s, pred),
        }
    }
}

impl<'a, P> Iterator for SplitPreserve<'a, P>
where
    P: Fn(char) -> bool,
{
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(s, _)| s)
    }
}

pub trait StrSplitPreserve {
    fn split_preserve<F>(&self, pred: F) -> SplitPreserve<F>
    where
        F: Fn(char) -> bool;
}

impl StrSplitPreserve for str {
    fn split_preserve<F>(&self, pred: F) -> SplitPreserve<F>
    where
        F: Fn(char) -> bool,
    {
        SplitPreserve::new(self, pred)
    }
}

impl StrSplitPreserve for String {
    fn split_preserve<F>(&self, pred: F) -> SplitPreserve<F>
    where
        F: Fn(char) -> bool,
    {
        SplitPreserve::new(self, pred)
    }
}

pub trait StrSplitPreserveWithRange {
    fn split_preserve_with_range<F>(&self, pred: F) -> SplitPreserveWithRange<F>
    where
        F: Fn(char) -> bool;
}

impl StrSplitPreserveWithRange for str {
    fn split_preserve_with_range<F>(&self, pred: F) -> SplitPreserveWithRange<F>
    where
        F: Fn(char) -> bool,
    {
        SplitPreserveWithRange::new(self, pred)
    }
}

impl StrSplitPreserveWithRange for String {
    fn split_preserve_with_range<F>(&self, pred: F) -> SplitPreserveWithRange<F>
    where
        F: Fn(char) -> bool,
    {
        SplitPreserveWithRange::new(self, pred)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let res = "hello.brave.new.world"
            .split_preserve(|c| c == '.')
            .collect::<Vec<_>>();
        assert_eq!(res, vec!["hello", ".", "brave", ".", "new", ".", "world"]);

        let res = "hello".split_preserve(|c| c == '.').collect::<Vec<_>>();
        assert_eq!(res, vec!["hello"]);
    }

    #[test]
    fn test_starts_with() {
        let res = ".hello.brave.new.world"
            .split_preserve(|c| c == '.')
            .collect::<Vec<_>>();
        assert_eq!(
            res,
            vec![".", "hello", ".", "brave", ".", "new", ".", "world"]
        );
    }

    #[test]
    fn test_ends_with() {
        let res = "hello.brave.new.world."
            .split_preserve(|c| c == '.')
            .collect::<Vec<_>>();
        assert_eq!(
            res,
            vec!["hello", ".", "brave", ".", "new", ".", "world", "."]
        );
    }

    #[test]
    fn test_empty() {
        let res = "".split_preserve(|c| c == '.').collect::<Vec<_>>();
        assert_eq!(res, vec![] as Vec<&str>);
    }

    #[test]
    fn test_no_split() {
        let res = "hello".split_preserve(|c| c == '.').collect::<Vec<_>>();
        assert_eq!(res, vec!["hello"]);
    }

    #[test]
    fn test_single_char() {
        let res = ".".split_preserve(|c| c == '.').collect::<Vec<_>>();
        assert_eq!(res, vec!["."]);
    }

    #[test]
    fn test_multi_char() {
        let res = "....".split_preserve(|c| c == '.').collect::<Vec<_>>();
        assert_eq!(res, vec![".", ".", ".", "."]);
    }
}
