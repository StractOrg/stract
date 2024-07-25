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

mod adapters;

pub trait LendingIterator {
    type Item<'a>
    where
        Self: 'a;

    fn next(&mut self) -> Option<Self::Item<'_>>;

    fn enumerate(self) -> adapters::Enumerate<Self>
    where
        Self: Sized,
    {
        adapters::Enumerate::new(self)
    }

    fn cloned<'a, T>(self) -> adapters::Cloned<Self>
    where
        Self: Sized,
        for<'b> Self::Item<'b>: std::ops::Deref<Target = T>,
        T: Clone,
    {
        adapters::Cloned::new(self)
    }

    fn fold<B, F>(self, init: B, f: F) -> B
    where
        Self: Sized,
        F: FnMut(B, Self::Item<'_>) -> B,
    {
        let mut f = f;
        let mut acc = init;
        let mut iter = self;

        while let Some(item) = iter.next() {
            acc = f(acc, item);
        }

        acc
    }

    fn count(self) -> usize
    where
        Self: Sized,
    {
        self.fold(0, |acc, _| acc + 1)
    }

    fn map<B, F>(self, f: F) -> adapters::Map<Self, F>
    where
        Self: Sized,
        F: FnMut(Self::Item<'_>) -> B,
    {
        adapters::Map::new(self, f)
    }

    fn filter<F>(self, f: F) -> adapters::Filter<Self, F>
    where
        Self: Sized,
        F: FnMut(&Self::Item<'_>) -> bool,
    {
        adapters::Filter::new(self, f)
    }

    fn flatten<'a>(self) -> adapters::Flatten<'a, Self>
    where
        Self: Sized,
        Self::Item<'a>: LendingIterator,
    {
        adapters::Flatten::new(self)
    }
}

impl<'a, I> LendingIterator for &'a mut I
where
    I: LendingIterator,
{
    type Item<'b> = I::Item<'b>
    where
        I: 'b,
        'a: 'b;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        (*self).next()
    }
}

pub trait IntoLendingIterator: Sized {
    fn lending(self) -> IntoLending<Self>;
}

pub struct IntoLending<I> {
    iter: I,
}

impl<I> LendingIterator for IntoLending<I>
where
    I: Iterator,
{
    type Item<'a> = I::Item
    where
        Self: 'a;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iter.next()
    }
}

impl<I> IntoLendingIterator for I
where
    I: Iterator,
{
    fn lending(self) -> IntoLending<Self> {
        IntoLending { iter: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Iter {
        items: Vec<i32>,
        index: usize,
    }

    impl Iter {
        fn new(items: Vec<i32>) -> Self {
            Self { items, index: 0 }
        }
    }

    impl LendingIterator for Iter {
        type Item<'a> = &'a i32;

        fn next(&mut self) -> Option<Self::Item<'_>> {
            if self.index < self.items.len() {
                let item = &self.items[self.index];
                self.index += 1;
                Some(item)
            } else {
                None
            }
        }
    }

    impl From<Vec<i32>> for Iter {
        fn from(items: Vec<i32>) -> Self {
            Self::new(items)
        }
    }

    #[test]
    fn test_lending_iterator() {
        let items = vec![1, 2, 3];
        let mut iter = Iter::new(items);
        assert_eq!(iter.next(), Some(&1));
        assert_eq!(iter.next(), Some(&2));
        assert_eq!(iter.next(), Some(&3));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_enumerate() {
        let items = vec![1, 2, 3];
        let mut iter = Iter::new(items).enumerate();
        assert_eq!(iter.next(), Some((0, &1)));
        assert_eq!(iter.next(), Some((1, &2)));
        assert_eq!(iter.next(), Some((2, &3)));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_cloned() {
        let items = vec![1, 2, 3];
        let mut iter = Iter::new(items).cloned();
        assert_eq!(iter.next(), Some(1));
        assert_eq!(iter.next(), Some(2));
        assert_eq!(iter.next(), Some(3));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_fold() {
        let items = vec![1, 2, 3];
        let iter = Iter::new(items);
        let sum = iter.fold(0, |acc, item| acc + item);
        assert_eq!(sum, 6);
    }

    #[test]
    fn test_count() {
        let items = vec![1, 2, 3];
        let iter = Iter::new(items);
        let count = iter.count();
        assert_eq!(count, 3);
    }

    #[test]
    fn test_map() {
        let items = vec![1, 2, 3];
        let mut iter = Iter::new(items).map(|item| item * 2);
        assert_eq!(iter.next(), Some(2));
        assert_eq!(iter.next(), Some(4));
        assert_eq!(iter.next(), Some(6));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_filter() {
        let items = vec![1, 2, 3];
        let mut iter = Iter::new(items).filter(|item| **item % 2 == 0);
        assert_eq!(iter.next(), Some(&2));
        assert_eq!(iter.next(), None);
    }

    struct NestedIter {
        items: Vec<Iter>,
        index: usize,
    }

    impl NestedIter {
        fn new(items: Vec<Iter>) -> Self {
            Self { items, index: 0 }
        }
    }

    impl LendingIterator for NestedIter {
        type Item<'a> = &'a mut Iter;

        fn next(&mut self) -> Option<Self::Item<'_>> {
            if self.index < self.items.len() {
                let item = &mut self.items[self.index];
                self.index += 1;
                Some(item)
            } else {
                None
            }
        }
    }

    #[test]
    fn test_flatten() {
        let items = vec![
            Iter::new(vec![1, 2]),
            Iter::new(vec![3, 4]),
            Iter::new(vec![5, 6]),
        ];
        let mut iter = NestedIter::new(items).flatten();
        assert_eq!(iter.next(), Some(&1));
        assert_eq!(iter.next(), Some(&2));
        assert_eq!(iter.next(), Some(&3));
        assert_eq!(iter.next(), Some(&4));
        assert_eq!(iter.next(), Some(&5));
        assert_eq!(iter.next(), Some(&6));
        assert_eq!(iter.next(), None);
    }
}
