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

use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
};
use tokenizer::Tokenizer;

pub type HashType = u64;

fn hash_token(token: &tantivy::tokenizer::Token) -> HashType {
    let mut hasher = DefaultHasher::default();
    token.text.as_str().hash(&mut hasher);
    hasher.finish()
}

pub fn hash(text: &str) -> HashType {
    let mut tokenizer = Tokenizer::default();

    let mut stream = tantivy::tokenizer::Tokenizer::token_stream(&mut tokenizer, text);

    let mut v = [0i64; HashType::BITS as usize];

    while let Some(token) = stream.next() {
        let h = hash_token(token);

        for (i, item) in v.iter_mut().enumerate() {
            let bit = (h >> i) & 1;
            if bit == 1 {
                *item = item.saturating_add(1);
            } else {
                *item = item.saturating_sub(1);
            }
        }
    }

    let mut simhash: HashType = 0;
    for (i, item) in v.iter().enumerate() {
        if *item > 0 {
            simhash |= 1 << i;
        }
    }

    simhash
}

fn hamming_distance(x: HashType, y: HashType) -> u32 {
    (x ^ y).count_ones()
}

#[derive(PartialEq, Eq, Hash)]
struct Prefix(HashType);

struct Block {
    hashes: HashMap<Prefix, Vec<HashType>>,
    mask: HashType,
}

impl Block {
    fn new(block_idx: usize) -> Self {
        Self {
            hashes: Default::default(),
            mask: (HashType::MAX << (HashType::BITS as usize - BLOCK_SIZE))
                >> (BLOCK_SIZE * block_idx),
        }
    }
}

impl Block {
    fn insert(&mut self, hash: HashType) {
        let prefix = Prefix(hash & self.mask);

        self.hashes.entry(prefix).or_default().push(hash)
    }

    /// true iff. `Block` has indexed a hash that is within `K` distance away from the query hash.
    fn contains(&self, hash: &HashType) -> bool {
        let prefix = Prefix(*hash & self.mask);

        match self.hashes.get(&prefix) {
            Some(candidates) => candidates
                .iter()
                .any(|candidate| hamming_distance(*hash, *candidate) as usize <= K),
            None => false,
        }
    }
}

// TODO: When `generic_const_exprs` becomes stable, re-write this to use const-generics
const K: usize = 3;
const NUM_BLOCKS: usize = K + 1;
const BLOCK_SIZE: usize = HashType::BITS as usize / NUM_BLOCKS;

pub struct Table {
    blocks: [Block; NUM_BLOCKS],
}

impl Default for Table {
    fn default() -> Self {
        Self {
            blocks: std::array::from_fn(Block::new),
        }
    }
}

impl Table {
    pub fn insert(&mut self, hash: HashType) {
        for block in &mut self.blocks {
            block.insert(hash);
        }
    }

    /// true iff. `Table` has indexed a hash that is within `K` distance away from the query hash.
    pub fn contains(&self, hash: &HashType) -> bool {
        self.blocks.iter().any(|block| block.contains(hash))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Calculate similarity as `f64` of two hashes
    /// 0.0 means no similarity, 1.0 means identical
    pub fn similarity(hash1: HashType, hash2: HashType) -> f64 {
        let distance: f64 = hamming_distance(hash1, hash2) as f64;
        1.0 - (distance / (HashType::BITS as f64))
    }

    #[test]
    fn simhash_test() {
        assert_eq!(hash("The cat sat on the mat"), 1696787384511938835);
        assert_eq!(hash("The cat sat under the mat"), 1557175861565382659);
        assert_eq!(hash("Why the lucky stiff"), 2343560682201631264);
    }

    #[test]
    fn hamming_distance_test() {
        assert_eq!(
            hamming_distance(0b0000000u64 as HashType, 0b0000000u64 as HashType),
            0
        );
        assert_eq!(
            hamming_distance(0b1111111u64 as HashType, 0b0000000u64 as HashType),
            7
        );
        assert_eq!(
            hamming_distance(0b0100101u64 as HashType, 0b1100110u64 as HashType),
            3
        );
    }

    #[test]
    fn similarity_test() {
        assert_eq!(
            similarity(hash("Stop hammertime"), hash("Stop hammertime")),
            1.0
        );
        assert!(
            similarity(hash("Hocus pocus"), hash("Hocus pocus pilatus pas"))
                > similarity(hash("This should"), hash("not overlap"))
        );
    }

    #[test]
    fn table() {
        let mut table = Table::default();

        let h1 = 0b0000000u64 as HashType;
        let h2 = 0b0000001u64 as HashType;
        let h3 = 0b1111111u64 as HashType;

        assert!(!table.contains(&h1));

        table.insert(h1);
        assert!(table.contains(&h2));
        assert!(!table.contains(&h3));
    }
}
