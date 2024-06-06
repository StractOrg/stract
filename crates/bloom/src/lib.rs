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

pub fn combine_u64s(nums: [u64; 2]) -> u128 {
    ((nums[0] as u128) << 64) | (nums[1] as u128)
}

pub fn split_u128(num: u128) -> [u64; 2] {
    [(num >> 64) as u64, num as u64]
}

const XXH3_SECRET: &[u8] = &xxhash_rust::const_xxh3::const_custom_default_secret(42);
pub fn fast_stable_hash_64(t: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64_with_secret(t, XXH3_SECRET)
}

pub fn fast_stable_hash_128(t: &[u8]) -> u128 {
    xxhash_rust::xxh3::xxh3_128_with_secret(t, XXH3_SECRET)
}

const LARGE_PRIME: u64 = 11400714819323198549;

/// Calculate the number of bits needed for a Bloom filter.
#[inline]
fn num_bits(estimated_items: u64, fp: f64) -> u64 {
    ((estimated_items as f64) * fp.ln() / (-8.0 * 2.0_f64.ln().powi(2))).ceil() as u64
}

/// Calculate the number of hash functions needed for a Bloom filter.
#[inline]
fn num_hashes(num_bits: u64, estimated_items: u64) -> u64 {
    (((num_bits as f64) / estimated_items as f64 * 2.0_f64.ln()).ceil() as u64).max(1)
}

#[derive(
    Clone,
    bincode::Encode,
    bincode::Decode,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    PartialEq,
    Eq,
)]
pub struct U64BloomFilter {
    #[bincode(with_serde)]
    bit_vec: BitVec,
}

impl U64BloomFilter {
    pub fn new(estimated_items: u64, fp: f64) -> Self {
        let num_bits = num_bits(estimated_items, fp);
        Self {
            bit_vec: BitVec::repeat(false, num_bits as usize),
        }
    }

    pub fn empty_from(other: &Self) -> Self {
        Self {
            bit_vec: BitVec::repeat(false, other.bit_vec.len()),
        }
    }

    pub fn fill(&mut self) {
        for i in 0..self.bit_vec.len() {
            self.bit_vec.set(i, true);
        }
    }

    fn hash(item: u64) -> usize {
        item.wrapping_mul(LARGE_PRIME) as usize
    }

    pub fn insert(&mut self, item: u64) {
        let h = Self::hash(item);
        let num_bits = self.bit_vec.len();
        self.bit_vec.set(h % num_bits, true);
    }

    pub fn contains(&self, item: u64) -> bool {
        let h = Self::hash(item);
        self.bit_vec[h % self.bit_vec.len()]
    }

    pub fn estimate_card(&self) -> u64 {
        let num_ones = self.bit_vec.count_ones() as u64;

        if num_ones == 0 || self.bit_vec.is_empty() {
            return 0;
        }

        if num_ones == self.bit_vec.len() as u64 {
            return u64::MAX;
        }

        (-(self.bit_vec.len() as i64)
            * (1.0 - (num_ones as f64) / (self.bit_vec.len() as f64)).ln() as i64)
            .try_into()
            .unwrap_or_default()
    }

    pub fn union(&mut self, other: Self) {
        debug_assert_eq!(self.bit_vec.len(), other.bit_vec.len());

        self.bit_vec |= other.bit_vec;
    }
}

#[derive(bincode::Encode, bincode::Decode)]
pub struct BytesBloomFilter<T> {
    #[bincode(with_serde)]
    bit_vec: BitVec,
    num_hashes: u64,
    _marker: std::marker::PhantomData<T>,
}

impl<T> BytesBloomFilter<T> {
    pub fn new(estimated_items: u64, fp: f64) -> Self {
        let num_bits = num_bits(estimated_items, fp);
        let num_hashes = num_hashes(num_bits, estimated_items);
        Self {
            bit_vec: BitVec::repeat(false, num_bits as usize),
            num_hashes,
            _marker: std::marker::PhantomData,
        }
    }

    fn hash_raw(item: &[u8]) -> [u64; 2] {
        split_u128(fast_stable_hash_128(item))
    }

    pub fn contains_raw(&self, item: &[u8]) -> bool {
        let [a, b] = Self::hash_raw(item);

        for i in 0..self.num_hashes {
            let h = ((a.wrapping_mul(i).wrapping_add(b)) % LARGE_PRIME) % self.bit_vec.len() as u64;
            if !self.bit_vec[h as usize] {
                return false;
            }
        }

        true
    }

    pub fn insert_raw(&mut self, item: &[u8]) {
        // see https://en.wikipedia.org/wiki/Universal_hashing#Hashing_integers
        // for why this universal hash construction works
        let [a, b] = Self::hash_raw(item);

        for i in 0..self.num_hashes {
            let h = ((a.wrapping_mul(i).wrapping_add(b)) % LARGE_PRIME) % self.bit_vec.len() as u64;
            self.bit_vec.set(h as usize, true);
        }
    }
}

impl<T> BytesBloomFilter<T>
where
    T: AsRef<[u8]>,
{
    pub fn insert(&mut self, item: &T) {
        self.insert_raw(item.as_ref())
    }

    pub fn contains(&self, item: &T) -> bool {
        self.contains_raw(item.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter() {
        let mut bf = U64BloomFilter::new(100, 0.01);
        bf.insert(1);
        bf.insert(2);
        bf.insert(3);
        bf.insert(4);
        bf.insert(5);

        assert!(bf.contains(1));
        assert!(bf.contains(2));
        assert!(bf.contains(3));
        assert!(bf.contains(4));
        assert!(bf.contains(5));
        assert!(!bf.contains(6));
        assert!(!bf.contains(7));
        assert!(!bf.contains(8));
        assert!(!bf.contains(9));
        assert!(!bf.contains(10));
    }

    #[test]
    fn test_bloom_filter_bytes() {
        let mut bf = BytesBloomFilter::new(100, 0.01);
        bf.insert(&1u64.to_be_bytes());
        bf.insert(&2u64.to_be_bytes());
        bf.insert(&3u64.to_be_bytes());
        bf.insert(&4u64.to_be_bytes());
        bf.insert(&5u64.to_be_bytes());

        assert!(bf.contains(&1u64.to_be_bytes()));
        assert!(bf.contains(&2u64.to_be_bytes()));
        assert!(bf.contains(&3u64.to_be_bytes()));
        assert!(bf.contains(&4u64.to_be_bytes()));
        assert!(bf.contains(&5u64.to_be_bytes()));
        assert!(!bf.contains(&6u64.to_be_bytes()));
        assert!(!bf.contains(&7u64.to_be_bytes()));
        assert!(!bf.contains(&8u64.to_be_bytes()));
        assert!(!bf.contains(&9u64.to_be_bytes()));
        assert!(!bf.contains(&10u64.to_be_bytes()));
    }

    #[test]
    fn split_combine_u128() {
        for num in 0..10000_u128 {
            assert_eq!(combine_u64s(split_u128(num)), num);
        }
    }
}
