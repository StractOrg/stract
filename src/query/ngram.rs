// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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

use std::array;

pub struct NGram<const N: usize, T: Clone> {
    res: [Option<T>; N],
    inner: Box<dyn Iterator<Item = T>>,
}

impl<const N: usize, T: Clone> NGram<N, T> {
    pub fn from_iter(it: impl Iterator<Item = T> + 'static) -> Self {
        Self {
            res: array::from_fn(|_| None),
            inner: Box::new(it),
        }
    }
}

impl<const N: usize, T: Clone> Iterator for NGram<N, T> {
    type Item = [Option<T>; N];

    fn next(&mut self) -> Option<Self::Item> {
        self.res.rotate_left(1);
        self.res[N - 1] = self.inner.next();

        if self.res.iter().all(Option::is_none) {
            None
        } else {
            Some(self.res.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let mut it: NGram<3, _> = NGram::from_iter(vec![1, 2, 3, 4].into_iter());

        assert_eq!(it.next(), Some([None, None, Some(1)]));
        assert_eq!(it.next(), Some([None, Some(1), Some(2)]));
        assert_eq!(it.next(), Some([Some(1), Some(2), Some(3)]));
        assert_eq!(it.next(), Some([Some(2), Some(3), Some(4)]));
        assert_eq!(it.next(), Some([Some(3), Some(4), None]));
        assert_eq!(it.next(), Some([Some(4), None, None]));
        assert_eq!(it.next(), None);
    }

    #[test]
    fn small_iter() {
        let mut it: NGram<3, _> = NGram::from_iter(vec![1].into_iter());

        assert_eq!(it.next(), Some([None, None, Some(1)]));
        assert_eq!(it.next(), Some([None, Some(1), None]));
        assert_eq!(it.next(), Some([Some(1), None, None]));
        assert_eq!(it.next(), None);
    }
}
