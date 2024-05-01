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

pub trait InsertEnumMapKey: Sized {
    fn into_usize(self) -> usize;
}

pub trait GetEnumMapKey: Sized {
    fn from_usize(value: usize) -> Option<Self>;
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct EnumMap<K: InsertEnumMapKey, V> {
    inner: Vec<Option<V>>,
    len: usize,
    _phantom: std::marker::PhantomData<K>,
}

impl<K: InsertEnumMapKey, V> Default for EnumMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> EnumMap<K, V>
where
    K: InsertEnumMapKey,
{
    pub fn new() -> Self {
        Self {
            inner: vec![],
            len: 0,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        let key = key.into_usize();

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
        let key = key.into_usize();
        if key >= self.inner.len() {
            None
        } else {
            self.inner[key].as_ref()
        }
    }

    pub fn contains_key(&self, key: K) -> bool {
        self.get(key).is_some()
    }

    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        self.inner.iter_mut().filter_map(|value| value.as_mut())
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.inner.iter().filter_map(|value| value.as_ref())
    }

    pub fn get_mut(&mut self, key: K) -> Option<&mut V> {
        let key = key.into_usize();
        if key >= self.inner.len() {
            None
        } else {
            self.inner[key].as_mut()
        }
    }
}

impl<K, V> EnumMap<K, V>
where
    K: GetEnumMapKey + InsertEnumMapKey,
{
    pub fn keys(&self) -> impl Iterator<Item = K> + '_ {
        self.inner
            .iter()
            .enumerate()
            .filter_map(|(key, value)| value.as_ref().and_then(|_| K::from_usize(key)))
    }

    pub fn iter(&self) -> impl Iterator<Item = (K, &V)> + '_ {
        self.inner.iter().enumerate().filter_map(|(key, value)| {
            value
                .as_ref()
                .map(|value| (K::from_usize(key).unwrap(), value))
        })
    }
}

impl<K, V> FromIterator<(K, V)> for EnumMap<K, V>
where
    K: InsertEnumMapKey,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let mut map = Self::new();

        for (key, value) in iter {
            map.insert(key, value);
        }

        map
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct EnumSet<K: InsertEnumMapKey> {
    map: EnumMap<K, ()>,
}

impl<K: InsertEnumMapKey> Default for EnumSet<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: InsertEnumMapKey> EnumSet<K> {
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

impl<K: InsertEnumMapKey + GetEnumMapKey> EnumSet<K> {
    pub fn iter(&self) -> impl Iterator<Item = K> + '_ {
        self.map.keys()
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

    impl InsertEnumMapKey for TestEnum {
        fn into_usize(self) -> usize {
            self as usize
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
