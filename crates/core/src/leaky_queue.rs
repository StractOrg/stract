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

use std::collections::VecDeque;

#[derive(Clone)]
pub struct LeakyQueue<T: Clone> {
    inner: VecDeque<T>,
    max_size: usize,
}

impl<T: Clone> LeakyQueue<T> {
    pub fn new(max_size: usize) -> Self {
        Self {
            inner: VecDeque::with_capacity(max_size),
            max_size,
        }
    }

    pub fn push(&mut self, item: T) {
        while self.inner.len() >= self.max_size {
            self.inner.pop_front();
        }

        self.inner.push_back(item);
    }

    pub fn pop(&mut self) -> Option<T> {
        self.inner.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let mut q = LeakyQueue::new(3);

        q.push(1);

        assert_eq!(q.pop(), Some(1));
        assert_eq!(q.pop(), None);

        q.push(1);
        q.push(2);
        q.push(3);
        q.push(4);

        assert_eq!(q.pop(), Some(2));
        assert_eq!(q.pop(), Some(3));
        assert_eq!(q.pop(), Some(4));
        assert_eq!(q.pop(), None);
    }
}
