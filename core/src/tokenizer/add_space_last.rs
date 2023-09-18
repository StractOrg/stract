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

use std::iter::Peekable;

pub struct AddSpaceToLast<I: Iterator<Item = String>> {
    inner: Peekable<I>,
    last_item: Option<String>,
}

impl<I: Iterator<Item = String>> AddSpaceToLast<I> {
    fn new(inner: I) -> Self {
        Self {
            inner: inner.peekable(),
            last_item: None,
        }
    }
}

impl<I: Iterator<Item = String>> Iterator for AddSpaceToLast<I> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(last_item) = self.last_item.take() {
            return Some(last_item);
        }

        match self.inner.next() {
            Some(mut item) => {
                if self.inner.peek().is_none() {
                    item.push(' ');
                }

                self.last_item = Some(item);
                self.next()
            }
            None => None,
        }
    }
}

pub trait AddSpaceLast: Iterator<Item = String> + Sized {
    fn add_space_last(self) -> AddSpaceToLast<Self> {
        AddSpaceToLast::new(self)
    }
}

impl<I: Iterator<Item = String>> AddSpaceLast for I {}
