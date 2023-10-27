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

use hyperloglog::HyperLogLog;

const THRESHOLD_SIM_ESTIMATE: f64 = 0.1;

const HYPERLOGLOG_REGISTERS: usize = 128;

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

#[derive(Clone)]
struct ScratchSpace {
    hyperloglog: HyperLogLog<HYPERLOGLOG_REGISTERS>,
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
                    match self.skip_pointers.get(i / self.skip_size + 1).copied() {
                        Some(skip_a) => {
                            if skip_a < b {
                                i += self.skip_size - i % self.skip_size;
                            } else {
                                i += 1;
                            }
                        }
                        None => {
                            i += 1;
                        }
                    }
                }
                std::cmp::Ordering::Greater => {
                    match other.skip_pointers.get(j / other.skip_size + 1).copied() {
                        Some(skip_b) => {
                            if skip_b < a {
                                j += other.skip_size - j % other.skip_size;
                            } else {
                                j += 1;
                            }
                        }
                        None => {
                            j += 1;
                        }
                    }
                }
            }
        }

        count
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BitVec {
    bloom: VeryJankyBloomFilter,
    hyperloglog: HyperLogLog<HYPERLOGLOG_REGISTERS>,
    posting: Posting,
    sqrt_len: f64,
}

impl BitVec {
    pub fn new(mut ranks: Vec<u64>) -> Self {
        ranks.sort();
        ranks.dedup();
        ranks.shrink_to_fit();

        let len = ranks.len();
        let mut bloom = VeryJankyBloomFilter::new(2);
        let mut hyperloglog = HyperLogLog::default();

        for rank in &ranks {
            bloom.insert(*rank);
            hyperloglog.add(*rank);
        }

        let posting = Posting::new(ranks);

        Self {
            bloom,
            hyperloglog,
            posting,
            sqrt_len: (len as f64).sqrt(),
        }
    }

    fn sim(&self, other: &Self, scratchspace: &mut ScratchSpace) -> f64 {
        if self.sqrt_len == 0.0 || other.sqrt_len == 0.0 {
            return 0.0;
        }

        if !self.bloom.has_intersection(&other.bloom) {
            return 0.0;
        }

        self.hyperloglog
            .merge_into(&other.hyperloglog, &mut scratchspace.hyperloglog);

        let union_est = scratchspace.hyperloglog.size();

        let intersect_est = (self.posting.ranks.len() + other.posting.ranks.len())
            .checked_sub(union_est)
            .unwrap_or_default();

        let sim_est = (intersect_est as f64) / (self.sqrt_len * other.sqrt_len);

        if sim_est < THRESHOLD_SIM_ESTIMATE {
            return 0.0;
        }

        let intersect = self.posting.intersection_size(&other.posting) as f64;
        intersect / (self.sqrt_len * other.sqrt_len)
    }
}

#[derive(Clone)]
pub struct BitVecSimilarity {
    scratchspace: ScratchSpace,
}

impl Default for BitVecSimilarity {
    fn default() -> Self {
        Self::new()
    }
}

impl BitVecSimilarity {
    pub fn new() -> Self {
        Self {
            scratchspace: ScratchSpace {
                hyperloglog: HyperLogLog::default(),
            },
        }
    }

    pub fn sim(&mut self, a: &BitVec, b: &BitVec) -> f64 {
        a.sim(b, &mut self.scratchspace)
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

        let sim = BitVecSimilarity::default().sim(&a, &b);

        assert!((expected - sim).abs() < 0.1);
    }

    #[test]
    fn zero_sim() {
        let a: Vec<_> = repeat(false).take(1000).collect();

        let b: Vec<_> = repeat(true).take(1000).collect();

        let a = BitVec::new(into_ranks(&a));
        let b = BitVec::new(into_ranks(&b));

        let sim = BitVecSimilarity::default().sim(&a, &b);

        assert_eq!(sim, 0.0);
    }

    #[test]
    fn empty_sim() {
        let a: Vec<_> = Vec::new();
        let b: Vec<_> = Vec::new();

        let a = BitVec::new(into_ranks(&a));
        let b = BitVec::new(into_ranks(&b));

        let sim = BitVecSimilarity::default().sim(&a, &b);

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

        let sim = BitVecSimilarity::default().sim(&a, &b);

        assert!((expected - sim).abs() < 0.1);
    }
}
