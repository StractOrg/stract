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

use std::hash::{BuildHasher, Hash, Hasher};

use serde::{Deserialize, Serialize};

struct PrehashBuilder {}

impl BuildHasher for PrehashBuilder {
    type Hasher = Prehasher;

    fn build_hasher(&self) -> Self::Hasher {
        Prehasher { val: 0 }
    }
}

struct Prehasher {
    val: u128,
}

impl Hasher for Prehasher {
    fn finish(&self) -> u64 {
        self.val as u64
    }

    fn write(&mut self, _bytes: &[u8]) {
        unimplemented!("This hasher only supports u128")
    }

    fn write_u128(&mut self, i: u128) {
        self.val = i;
    }
}

#[derive(Debug, Eq, Clone, Copy, Serialize, Deserialize)]
pub struct Prehashed(pub u128);

impl From<u128> for Prehashed {
    fn from(val: u128) -> Self {
        Self(val)
    }
}

impl Hash for Prehashed {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_u128(self.0);
    }
}

impl PartialEq for Prehashed {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

pub fn hash<T: AsRef<[u8]>>(val: T) -> Prehashed {
    let digest = md5::compute(val);
    Prehashed(u128::from_be_bytes(*digest))
}

pub fn split_u128(num: u128) -> [u64; 2] {
    [(num >> 64) as u64, num as u64]
}

pub fn combine_u64s(nums: [u64; 2]) -> u128 {
    ((nums[0] as u128) << 64) | (nums[1] as u128)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_combine_u128() {
        for num in 0..10000_u128 {
            assert_eq!(combine_u64s(split_u128(num)), num);
        }
    }
}
