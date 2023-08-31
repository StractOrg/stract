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

pub trait Key: PartialEq + Copy + std::fmt::Debug {
    const BIG_PRIME: Self;

    fn wrapping_mul(self, rhs: Self) -> Self;
    fn bit_and(self, rhs: Self) -> Self;
    fn from_usize(val: usize) -> Self;
    fn as_usize(self) -> usize;
}

impl Key for u64 {
    const BIG_PRIME: Self = 11400714819323198549;

    fn wrapping_mul(self, rhs: Self) -> Self {
        self.wrapping_mul(rhs)
    }

    fn bit_and(self, rhs: Self) -> Self {
        self & rhs
    }

    fn from_usize(val: usize) -> Self {
        val as Self
    }

    fn as_usize(self) -> usize {
        self as usize
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct IntMap<K: Key, V> {
    bins: Vec<Vec<(K, V)>>,
    len: usize,
}

impl<K: Key, V: Clone> Clone for IntMap<K, V> {
    fn clone(&self) -> Self {
        Self {
            bins: self.bins.clone(),
            len: self.len,
        }
    }
}

impl<K: Key, V> IntMap<K, V> {
    pub fn new() -> Self {
        Self::with_capacity(2)
    }

    pub fn with_capacity(cap: usize) -> Self {
        let mut bins = Vec::with_capacity(cap);

        for _ in 0..cap {
            bins.push(Vec::new());
        }

        Self { bins, len: 0 }
    }

    fn bin_idx(&self, key: &K) -> usize {
        let mask = K::from_usize(self.bins.len() - 1);
        let key = key.wrapping_mul(K::BIG_PRIME);

        key.bit_and(mask).as_usize()
    }

    pub fn insert(&mut self, key: K, value: V) {
        if self.len >= (self.bins.len() as f64 * 1.2) as usize {
            self.grow();
        }

        let bin_idx = self.bin_idx(&key);
        let bin = &mut self.bins[bin_idx];

        if let Some(idx) = bin.iter().position(|(k, _)| *k == key) {
            bin[idx] = (key, value);
        } else {
            bin.push((key, value));
            self.len += 1;
        }
    }

    fn grow(&mut self) {
        let mut bins = Vec::new();

        for _ in 0..self.bins.len() * 2 {
            bins.push(Vec::new());
        }

        std::mem::swap(&mut self.bins, &mut bins);

        for bin in bins {
            for (key, value) in bin {
                let bin_idx = self.bin_idx(&key);
                self.bins[bin_idx].push((key, value));
            }
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let bin = self.bin_idx(key);
        self.bins[bin]
            .iter()
            .find(|(stored_key, _)| stored_key == key)
            .map(|(_, val)| val)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let bin = self.bin_idx(key);
        self.bins[bin]
            .iter_mut()
            .find(|(stored_key, _)| stored_key == key)
            .map(|(_, val)| val)
    }

    pub fn into_iter(self) -> impl Iterator<Item = (K, V)> {
        self.bins.into_iter().flat_map(|bin| bin.into_iter())
    }

    pub fn iter(&self) -> impl Iterator<Item = &(K, V)> {
        self.bins.iter().flat_map(|bin| bin.iter())
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.get(key).is_some()
    }
}

impl<K: Key, V> std::iter::FromIterator<(K, V)> for IntMap<K, V> {
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let iter = iter.into_iter();

        let (_, upper) = iter.size_hint();

        let mut map = if let Some(cap) = upper {
            Self::with_capacity(cap)
        } else {
            Self::new()
        };

        for (key, val) in iter {
            map.insert(key, val);
        }

        map
    }
}

impl<K: Key, V> Default for IntMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct IntSet<K: Key = u64> {
    map: IntMap<K, ()>,
}

impl<K: Key> Default for IntSet<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Key> IntSet<K> {
    pub fn new() -> Self {
        Self { map: IntMap::new() }
    }

    pub fn insert(&mut self, item: K) {
        self.map.insert(item, ());
    }

    pub fn contains(&self, item: &K) -> bool {
        self.map.contains_key(item)
    }

    pub fn into_iter(self) -> impl Iterator<Item = K> {
        self.map.into_iter().map(|(k, _)| k)
    }
}

impl<K: Key> std::iter::FromIterator<K> for IntSet<K> {
    fn from_iter<T: IntoIterator<Item = K>>(iter: T) -> Self {
        let mut set = Self::new();

        for num in iter {
            set.insert(num);
        }

        set
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let mut map = IntMap::new();

        assert_eq!(map.len, 0);

        map.insert(42, "test".to_string());

        assert_eq!(map.len, 1);
        assert_eq!(map.get(&42), Some(&"test".to_string()));
        assert_eq!(map.get(&43), None);

        map.insert(43, "kage".to_string());
        assert_eq!(map.get(&43), Some(&"kage".to_string()));

        for key in 0..1000 {
            map.insert(key, key.to_string());

            assert_eq!(map.get(&key), Some(&key.to_string()));
        }

        assert_eq!(map.len, 1000);
    }
}
