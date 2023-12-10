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

use itertools::Itertools;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct VeryJankyBloomFilter {
    data: Vec<u64>,
}

impl VeryJankyBloomFilter {
    fn new(num_blooms: usize) -> Self {
        Self {
            data: vec![0; num_blooms],
        }
    }

    fn hash(&self, item: &u64) -> (usize, u64) {
        (
            (item.wrapping_mul(11400714819323198549) % self.data.len() as u64) as usize,
            item.wrapping_mul(11400714819323198549) % 64,
        )
    }

    fn insert(&mut self, item: u64) {
        let (a, b) = self.hash(&item);
        self.data[a] |= 1 << b;
    }

    #[inline]
    fn has_intersection(&self, other: &Self) -> bool {
        self.data
            .iter()
            .zip_eq(other.data.iter())
            .any(|(a, b)| a & b != 0)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Posting {
    ranks: Vec<u64>,
    skip_pointers: Vec<u64>,
    skip_size: usize,
}

impl Posting {
    fn new(ranks: Vec<u64>) -> Self {
        let skip_size = (ranks.len() as f64).sqrt() as usize;

        let skip_pointers = ranks
            .iter()
            .enumerate()
            .filter(|(i, _)| i % skip_size == 0)
            .map(|(_, rank)| *rank)
            .collect();

        Self {
            ranks,
            skip_pointers,
            skip_size,
        }
    }

    fn intersection_size(&self, other: &Self) -> usize {
        let mut i = 0;
        let mut j = 0;

        let mut count = 0;

        while i < self.ranks.len() && j < other.ranks.len() {
            let a = self.ranks[i];
            let b = other.ranks[j];

            match a.cmp(&b) {
                std::cmp::Ordering::Equal => {
                    count += 1;
                    i += 1;
                    j += 1;
                }
                std::cmp::Ordering::Less => {
                    i = self.next_skip_index(i, b);
                }
                std::cmp::Ordering::Greater => {
                    j = other.next_skip_index(j, a);
                }
            }
        }

        count
    }

    fn next_skip_index(&self, current_index: usize, target: u64) -> usize {
        let mut index = current_index;

        while (index / self.skip_size) + 1 < self.skip_pointers.len() {
            let skip_index = (index / self.skip_size) + 1;
            let skip_value = self.skip_pointers[skip_index];

            if skip_value >= target {
                break;
            }

            index = skip_index * self.skip_size;

            if index >= self.ranks.len() {
                return self.ranks.len();
            }
        }

        if index == current_index {
            index += 1;
        }

        std::cmp::min(index, self.ranks.len())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BitVec {
    bloom: VeryJankyBloomFilter,
    posting: Posting,
    sqrt_len: f64,
}

impl BitVec {
    pub fn new(mut ranks: Vec<u64>) -> Self {
        ranks.sort();
        ranks.dedup();
        ranks.shrink_to_fit();

        let len = ranks.len();
        let mut bloom = VeryJankyBloomFilter::new(16);

        for rank in &ranks {
            bloom.insert(*rank);
        }

        let posting = Posting::new(ranks);

        Self {
            bloom,
            posting,
            sqrt_len: (len as f64).sqrt(),
        }
    }

    pub fn sim(&self, other: &Self) -> f64 {
        if self.sqrt_len == 0.0 || other.sqrt_len == 0.0 {
            return 0.0;
        }

        if !self.bloom.has_intersection(&other.bloom) {
            return 0.0;
        }

        let intersect = self.posting.intersection_size(&other.posting) as f64;

        intersect / (self.sqrt_len * other.sqrt_len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;
    use std::iter::repeat;

    fn into_ranks(a: &[bool]) -> Vec<u64> {
        a.iter()
            .enumerate()
            .filter(|(_, b)| **b)
            .map(|(i, _)| i as u64)
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

        let a = BitVec::new(into_ranks(&a));
        let b = BitVec::new(into_ranks(&b));

        let sim = a.sim(&b);

        assert!((expected - sim).abs() < 0.1);
    }

    #[test]
    fn zero_sim() {
        let a: Vec<_> = repeat(false).take(1000).collect();

        let b: Vec<_> = repeat(true).take(1000).collect();

        let a = BitVec::new(into_ranks(&a));
        let b = BitVec::new(into_ranks(&b));

        let sim = a.sim(&b);

        assert_eq!(sim, 0.0);
    }

    #[test]
    fn empty_sim() {
        let a: Vec<_> = Vec::new();
        let b: Vec<_> = Vec::new();

        let a = BitVec::new(into_ranks(&a));
        let b = BitVec::new(into_ranks(&b));

        let sim = a.sim(&b);

        assert_eq!(sim, 0.0);
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

        let sim = a.sim(&b);

        assert!((expected - sim).abs() < 0.1);
    }
}
