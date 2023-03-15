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

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Block {
    data: u64,
    offset: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BitVec {
    ranks: Vec<usize>,
    sqrt_len: f64,
}

impl BitVec {
    pub fn new(ranks: Vec<usize>) -> Self {
        let len = ranks.len();
        Self {
            ranks,
            sqrt_len: (len as f64).sqrt(),
        }
    }

    pub fn sim(&self, other: &Self) -> f64 {
        let mut i = 0;
        let mut j = 0;

        let mut dot: u64 = 0;

        while i < self.ranks.len() && j < other.ranks.len() {
            match self.ranks[i].cmp(&other.ranks[j]) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
                std::cmp::Ordering::Equal => {
                    dot += 1;
                    i += 1;
                    j += 1;
                }
            }
        }

        if dot == 0 {
            0.0
        } else {
            dot as f64 / (self.sqrt_len * other.sqrt_len)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;
    use std::iter::repeat;

    fn into_ranks(a: &[bool]) -> Vec<usize> {
        a.iter()
            .enumerate()
            .filter(|(_, b)| **b)
            .map(|(i, _)| i)
            .collect()
    }

    fn naive_sim(a: &[bool], b: &[bool]) -> f64 {
        let dot = a
            .iter()
            .zip_eq(b.iter())
            .filter(|(a, b)| **a && **b)
            .count();

        let len_a = a.iter().filter(|a| **a).count();
        let len_b = b.iter().filter(|b| **b).count();

        dot as f64 / ((len_a as f64).sqrt() * (len_b as f64).sqrt())
    }

    #[test]
    fn simple() {
        let a: Vec<_> = repeat(false)
            .take(1000)
            .chain(repeat(true).take(10))
            .collect();

        let b: Vec<_> = repeat(false)
            .take(1000)
            .chain(repeat(true).take(8))
            .chain(repeat(false).take(2))
            .collect();

        let expected = naive_sim(&a, &b);

        assert!(expected > 0.894);

        let a = BitVec::new(into_ranks(&a));
        let b = BitVec::new(into_ranks(&b));

        assert!((expected - a.sim(&b)).abs() < 0.00001);
    }

    #[test]
    fn zero_sim() {
        let a: Vec<_> = repeat(false).take(1000).collect();

        let b: Vec<_> = repeat(true).take(1000).collect();

        let a = BitVec::new(into_ranks(&a));
        let b = BitVec::new(into_ranks(&b));

        assert_eq!(a.sim(&b), 0.0);
    }

    #[test]
    fn empty_sim() {
        let a: Vec<_> = Vec::new();
        let b: Vec<_> = Vec::new();

        let a = BitVec::new(into_ranks(&a));
        let b = BitVec::new(into_ranks(&b));

        assert_eq!(a.sim(&b), 0.0);
    }

    #[test]
    fn low_sim() {
        let a: Vec<_> = repeat(false)
            .take(100000)
            .chain(repeat(true).take(10))
            .collect();

        let b: Vec<_> = repeat(true)
            .take(100000)
            .chain(repeat(true).take(8))
            .chain(repeat(false).take(2))
            .collect();

        let expected = naive_sim(&a, &b);

        assert!(expected < 0.01);

        let a = BitVec::new(into_ranks(&a));
        let b = BitVec::new(into_ranks(&b));

        assert!((expected - a.sim(&b)).abs() < 0.00001);
    }
}
