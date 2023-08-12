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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnumMap<K: Into<usize>, V> {
    inner: Vec<Option<V>>,
    len: usize,
    _phantom: std::marker::PhantomData<K>,
}

impl<K: Into<usize>, V> Default for EnumMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> EnumMap<K, V>
where
    K: Into<usize>,
{
    pub fn new() -> Self {
        Self {
            inner: vec![],
            len: 0,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        let key = key.into();

        if key >= self.inner.len() {
            self.inner.resize_with(key + 1, || None);
        }

        if self.inner[key].is_none() {
            self.len += 1;
        }

        self.inner[key] = Some(value);
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn get(&self, key: K) -> Option<&V> {
        let key = key.into();
        if key >= self.inner.len() {
            None
        } else {
            self.inner[key].as_ref()
        }
    }

    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        self.inner.iter_mut().filter_map(|value| value.as_mut())
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.inner.iter().filter_map(|value| value.as_ref())
    }

    pub fn get_mut(&mut self, key: K) -> Option<&mut V> {
        let key = key.into();
        if key >= self.inner.len() {
            None
        } else {
            self.inner[key].as_mut()
        }
    }
}

impl<K, V> FromIterator<(K, V)> for EnumMap<K, V>
where
    K: Into<usize>,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let mut map = Self::new();

        for (key, value) in iter {
            map.insert(key, value);
        }

        map
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnumSet<K: Into<usize>> {
    map: EnumMap<K, ()>,
}

impl<K: Into<usize>> Default for EnumSet<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Into<usize>> EnumSet<K> {
    pub fn new() -> Self {
        Self {
            map: EnumMap::new(),
        }
    }

    pub fn insert(&mut self, key: K) {
        self.map.insert(key, ());
    }

    pub fn contains(&self, key: K) -> bool {
        self.map.get(key).is_some()
    }

    pub fn is_empty(&self) -> bool {
        self.map.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    enum TestEnum {
        A,
        B,
        C,
    }

    impl From<TestEnum> for usize {
        fn from(val: TestEnum) -> Self {
            val as usize
        }
    }

    #[test]
    fn test_enum_map() {
        let mut map = EnumMap::new();
        map.insert(TestEnum::A, TestEnum::B);
        map.insert(TestEnum::B, TestEnum::C);
        map.insert(TestEnum::C, TestEnum::A);

        assert_eq!(map.get(TestEnum::A), Some(&TestEnum::B));
        assert_eq!(map.get(TestEnum::B), Some(&TestEnum::C));
        assert_eq!(map.get(TestEnum::C), Some(&TestEnum::A));
    }
}
