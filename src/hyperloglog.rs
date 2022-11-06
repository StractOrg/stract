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

#[derive(Clone)]
pub struct HyperLogLog<const N: usize> {
    registers: [u8; N],
}

impl<const N: usize> Default for HyperLogLog<N> {
    fn default() -> Self {
        Self {
            registers: array::from_fn(|_| 0),
        }
    }
}

impl<const N: usize> HyperLogLog<N> {
    fn hash(item: u64) -> u64 {
        item.wrapping_mul(11400714819323198549)
    }

    fn am(&self) -> f64 {
        let m = self.registers.len();

        if m >= 128 {
            0.7213 / (1. + 1.079 / (m as f64))
        } else if m >= 64 {
            0.709
        } else if m >= 32 {
            0.697
        } else {
            0.673
        }
    }

    pub fn add(&mut self, item: u64) {
        let b = (N as f64).log2() as usize;
        let hash = Self::hash(item) as usize;

        let index_mask = (1usize << b) - 1;
        let i = hash & index_mask;
        let p = ((hash & !index_mask).leading_zeros() + 1) as u8;

        self.registers[i] = self.registers[i].max(p);
    }

    pub fn size(&self) -> usize {
        let m = self.registers.len();

        let z = 1f64
            / self
                .registers
                .iter()
                .map(|&val| 2_f64.powi(-(i32::from(val))))
                .sum::<f64>();

        (self.am() * (m as f64).powi(2) * z) as usize
    }

    #[cfg(test)]
    pub fn relative_error(&self) -> f64 {
        (3f64 * 2f64.ln() - 1f64).sqrt() / (self.registers.len() as f64).sqrt()
    }

    pub fn merge(&mut self, other: &Self) {
        for i in 0..N {
            self.registers[i] = self.registers[i].max(other.registers[i]);
        }
    }

    pub fn registers(&self) -> &[u8] {
        &self.registers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_estimate_within_bounds() {
        let mut set: HyperLogLog<16> = HyperLogLog::default();

        for item in 0..10_000_000 {
            set.add(item);
        }

        let delta = (set.relative_error() * (set.size() as f64)) as usize;
        let lower_bound = set.size() - delta;
        let upper_bound = set.size() + delta;

        assert!(set.size() > lower_bound && set.size() < upper_bound);
    }

    #[test]
    fn merge() {
        let mut without_merge: HyperLogLog<16> = HyperLogLog::default();

        let mut a: HyperLogLog<16> = HyperLogLog::default();
        let mut b: HyperLogLog<16> = HyperLogLog::default();

        for item in 0..10_000 {
            without_merge.add(item);
            a.add(item);
        }

        for item in 10_001..20_000 {
            without_merge.add(item);
            b.add(item);
        }

        a.merge(&b);

        assert_eq!(a.registers, without_merge.registers);
    }
}
