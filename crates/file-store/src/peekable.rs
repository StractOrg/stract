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
// along with this program.  If not, see <https://www.gnu.org/licenses/

/// An iterator that allows peeking at the next element.
/// Unlike the standard library's `Peekable`, this implementation
/// *is not* lazy and will always consume the next element when peeking.
/// This is useful when you want to peek at the next element without
/// having a mutable reference to the iterator.
pub struct Peekable<I>
where
    I: Iterator,
{
    iter: I,
    peeked: Option<I::Item>,
}

impl<I> Peekable<I>
where
    I: Iterator,
{
    pub fn new(iter: I) -> Self {
        let mut iter = iter;
        let peeked = iter.next();
        Self { iter, peeked }
    }

    pub fn peek(&self) -> Option<&I::Item> {
        self.peeked.as_ref()
    }
}

impl<I> Iterator for Peekable<I>
where
    I: Iterator,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let peeked = self.peeked.take();
        if peeked.is_some() {
            self.peeked = self.iter.next();
        }

        peeked
    }
}
