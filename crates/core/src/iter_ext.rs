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

use std::cmp::Ordering;

pub trait IteratorExt: Iterator {
    fn peekable_non_lazy(self) -> Peekable<Self>
    where
        Self: Sized,
    {
        Peekable::new(self)
    }
}

pub trait VectorExt {
    type Iter: Iterator;
    fn flat_sorted_by<F>(self, f: F) -> FlatSortedBy<Self::Iter, F>
    where
        Self: Sized,
        F: FnMut(&<Self::Iter as Iterator>::Item, &<Self::Iter as Iterator>::Item) -> Ordering;
}

impl<I> VectorExt for Vec<I>
where
    I: Iterator,
{
    type Iter = I;

    fn flat_sorted_by<F>(self, f: F) -> FlatSortedBy<Self::Iter, F>
    where
        Self: Sized,
        F: FnMut(&<Self::Iter as Iterator>::Item, &<Self::Iter as Iterator>::Item) -> Ordering,
    {
        FlatSortedBy::new(self, f)
    }
}

impl<I: Iterator> IteratorExt for I {}

pub struct Peekable<I>
where
    I: Iterator,
{
    iter: I,
    peeked: Option<I::Item>,
}

impl<I: Iterator> Peekable<I> {
    pub fn new(iter: I) -> Self {
        let mut iter = iter;
        let peeked = iter.next();
        Self { iter, peeked }
    }

    pub fn peek(&self) -> Option<&I::Item> {
        self.peeked.as_ref()
    }
}

impl<I: Iterator> Iterator for Peekable<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let peeked = self.peeked.take();
        if peeked.is_some() {
            self.peeked = self.iter.next();
        }

        peeked
    }
}

/// Iterator that flattens an iterator of iterators and sorts the items.
/// It assumes that each inner iterator is sorted.
pub struct FlatSortedBy<I, F>
where
    I: Iterator,
{
    iters: Vec<Peekable<I>>,
    f: F,
}

impl<I, F> FlatSortedBy<I, F>
where
    I: Iterator,
{
    pub fn new(iters: Vec<I>, f: F) -> Self
    where
        I: Iterator,
    {
        let iters = iters.into_iter().map(Peekable::new).collect();
        Self { iters, f }
    }
}

impl<I, F> Iterator for FlatSortedBy<I, F>
where
    I: Iterator,
    F: FnMut(&<I as Iterator>::Item, &<I as Iterator>::Item) -> Ordering,
{
    type Item = <I as Iterator>::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let mut min = self
            .iters
            .iter_mut()
            .min_by(|a, b| match (a.peek(), b.peek()) {
                (Some(a), Some(b)) => (self.f)(a, b),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            });

        min.as_mut()?.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flat_sorted_by() {
        let iters = vec![
            vec![1, 2, 3].into_iter(),
            vec![4, 5, 6].into_iter(),
            vec![7, 8, 9].into_iter(),
        ];
        let sorted: Vec<_> = iters.flat_sorted_by(|a, b| a.cmp(b)).collect();
        assert_eq!(sorted, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }
}
