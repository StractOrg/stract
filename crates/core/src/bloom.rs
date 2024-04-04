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

use bitvec::vec::BitVec;

#[derive(Clone)]
pub struct BloomFilter {
    bit_vec: BitVec,
    num_bits: u64,
}

impl BloomFilter {
    pub fn new(estimated_items: u64, fp: f64) -> Self {
        let num_bits = Self::num_bits(estimated_items, fp);
        Self {
            bit_vec: BitVec::repeat(false, num_bits as usize),
            num_bits,
        }
    }

    pub fn empty_from(other: &Self) -> Self {
        Self {
            bit_vec: BitVec::repeat(false, other.num_bits as usize),
            num_bits: other.num_bits,
        }
    }

    fn num_bits(estimated_items: u64, fp: f64) -> u64 {
        ((estimated_items as f64) * fp.ln() / (-8.0 * 2.0_f64.ln().powi(2))).ceil() as u64
    }

    fn hash(item: u64) -> usize {
        item.wrapping_mul(11400714819323198549) as usize
    }

    pub fn insert(&mut self, item: u64) {
        let h = Self::hash(item);
        self.bit_vec.set(h % self.num_bits as usize, true);
    }

    pub fn contains(&self, item: u64) -> bool {
        let h = Self::hash(item);
        self.bit_vec[h % self.num_bits as usize]
    }

    pub fn estimate_card(&self) -> u64 {
        let num_ones = self.bit_vec.count_ones() as u64;

        if num_ones == 0 || self.num_bits == 0 {
            return 0;
        }

        if num_ones == self.num_bits {
            return u64::MAX;
        }

        (-(self.num_bits as i64) * (1.0 - (num_ones as f64) / (self.num_bits as f64)).ln() as i64)
            .try_into()
            .unwrap_or_default()
    }

    pub fn merge(&mut self, other: Self) {
        self.bit_vec |= other.bit_vec;
    }
}
