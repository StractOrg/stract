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

use crate::LendingIterator;

pub struct Enumerate<I> {
    iter: I,
    index: usize,
}

impl<I> Enumerate<I> {
    pub fn new(iter: I) -> Self {
        Self { iter, index: 0 }
    }
}

impl<I> From<I> for Enumerate<I> {
    fn from(iter: I) -> Self {
        Self::new(iter)
    }
}

impl<I> LendingIterator for Enumerate<I>
where
    I: LendingIterator,
{
    type Item<'a> = (usize, I::Item<'a>)
    where
        I: 'a;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iter.next().map(|item| {
            let index = self.index;
            self.index += 1;
            (index, item)
        })
    }
}
