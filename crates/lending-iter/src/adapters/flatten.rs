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

pub struct Flatten<'a, I>
where
    I: LendingIterator,
    I::Item<'a>: LendingIterator,
    Self: 'a,
{
    iter: I,
    current: Option<I::Item<'a>>,
}

impl<'a, I> Flatten<'a, I>
where
    I: LendingIterator,
    I::Item<'a>: LendingIterator,
{
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            current: None,
        }
    }
}

impl<'a, I> LendingIterator for Flatten<'a, I>
where
    I: LendingIterator,
    I::Item<'a>: LendingIterator,
    Self: 'a,
{
    type Item<'b> = <I::Item<'a> as LendingIterator>::Item<'b>
    where
        Self: 'b;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        // SAFETY (polonius):
        // - `self` is a mutable reference, so it is the only reference to this instance.
        // - we have a mutable reference to `self.current`, so it is the only reference to this field.
        // - `self.iter` is not accessed while `self.current` is borrowed.

        loop {
            let self_ = unsafe { &mut *(self as *mut Self) };

            if let Some(current) = unsafe { &mut *(&mut self_.current as *mut Option<I::Item<'a>>) }
            {
                if let Some(item) = current.next() {
                    return Some(item);
                }
            }

            self_.current = self_.iter.next();
            self_.current.as_ref()?;
        }
    }
}
