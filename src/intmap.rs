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

use serde::{Deserialize, Serialize};

type Key = u64;

#[derive(Serialize, Deserialize)]
pub struct IntMap<V> {
    bins: Vec<Vec<(Key, V)>>,
    len: usize,
}

impl<V: Clone> Clone for IntMap<V> {
    fn clone(&self) -> Self {
        Self {
            bins: self.bins.clone(),
            len: self.len,
        }
    }
}

impl<V> IntMap<V> {
    pub fn new() -> Self {
        let bins = vec![Vec::new(), Vec::new()];

        Self { bins, len: 0 }
    }

    fn bin_idx(&self, key: &Key) -> usize {
        let mask = (self.bins.len() - 1) as Key;
        let key = key.wrapping_mul(11400714819323198549 as Key);
        (key & mask) as usize
    }

    pub fn insert(&mut self, key: Key, value: V) {
        if self.len >= (self.bins.len() as f64 * 0.8) as usize {
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

    pub fn get(&self, key: &Key) -> Option<&V> {
        let bin = self.bin_idx(key);
        self.bins[bin]
            .iter()
            .find(|(stored_key, _)| stored_key == key)
            .map(|(_, val)| val)
    }

    pub fn get_mut(&mut self, key: &Key) -> Option<&mut V> {
        let bin = self.bin_idx(key);
        self.bins[bin]
            .iter_mut()
            .find(|(stored_key, _)| stored_key == key)
            .map(|(_, val)| val)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn into_iter(self) -> impl Iterator<Item = (Key, V)> {
        self.bins.into_iter().flat_map(|bin| bin.into_iter())
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut (Key, V)> {
        self.bins.iter_mut().flat_map(|bin| bin.iter_mut())
    }
}

impl<V> std::iter::FromIterator<(u64, V)> for IntMap<V> {
    fn from_iter<T: IntoIterator<Item = (u64, V)>>(iter: T) -> Self {
        let mut map = Self::new();

        for (key, val) in iter {
            map.insert(key, val);
        }

        map
    }
}

impl<V> Default for IntMap<V> {
    fn default() -> Self {
        Self::new()
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
