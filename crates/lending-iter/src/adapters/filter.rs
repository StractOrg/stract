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

pub struct Filter<I, P> {
    iter: I,
    predicate: P,
}

impl<I, P> Filter<I, P> {
    pub fn new(iter: I, predicate: P) -> Self {
        Self { iter, predicate }
    }
}

impl<I, P> LendingIterator for Filter<I, P>
where
    I: LendingIterator,
    for<'a> P: FnMut(&I::Item<'a>) -> bool,
{
    type Item<'a> = I::Item<'a>
    where
        Self: 'a;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        // SAFETY (polonius):
        // - `self` is a mutable reference, so it is the only reference to this instance.
        // - we have a mutable reference to `self.iter`, so it is the only reference to this field.
        // - the reference to `item` is only returned in previous loop iff. the predicate is true.
        //      Otherwise, the reference is dropped before the next iteration.

        loop {
            let self_ = unsafe { &mut *(self as *mut Self) };
            if let Some(item) = self_.iter.next() {
                if (self_.predicate)(&item) {
                    return Some(item);
                }
            } else {
                return None;
            }
        }
    }
}
