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

use std::hash::Hash;
use std::{collections::HashMap, sync::RwLock};

use serde::{de::DeserializeOwned, Serialize};

pub mod rocksdb_store;

pub trait Kv<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
    Self: Send + Sync,
{
    fn get_raw(&self, key: &[u8]) -> Option<Vec<u8>>;
    fn insert_raw(&self, key: Vec<u8>, value: Vec<u8>);
    fn flush(&self);
    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (K, V)> + 'a>;

    fn get(&self, key: &K) -> Option<V> {
        let key_bytes = bincode::serialize(key).expect("failed to serialize key");

        self.get_raw(&key_bytes)
            .map(|bytes| bincode::deserialize(&bytes).expect("failed to deserialize stored value"))
    }

    fn insert(&self, key: K, value: V) {
        let key_bytes = bincode::serialize(&key).expect("failed to serialize key");
        let val_bytes = bincode::serialize(&value).expect("failed to serialize value");

        self.insert_raw(key_bytes, val_bytes);
    }

    fn load_in_memory(&self) -> Memory<K, V>
    where
        K: Eq + Hash,
    {
        let mut map = HashMap::new();

        for (k, v) in self.iter() {
            map.insert(k, v);
        }

        Memory(RwLock::new(map))
    }
}

pub struct Memory<K, V>(RwLock<HashMap<K, V>>);

impl<K, V> Kv<K, V> for Memory<K, V>
where
    K: Serialize + DeserializeOwned + Send + Sync + Eq + Hash + Clone,
    V: Serialize + DeserializeOwned + Send + Sync + Clone,
{
    fn get_raw(&self, _key: &[u8]) -> Option<Vec<u8>> {
        unimplemented!()
    }

    fn insert_raw(&self, _key: Vec<u8>, _value: Vec<u8>) {
        unimplemented!()
    }

    fn flush(&self) {}

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (K, V)> + 'a> {
        todo!();
    }

    fn get(&self, key: &K) -> Option<V> {
        self.0.read().unwrap().get(key).cloned()
    }

    fn insert(&self, key: K, val: V) {
        self.0.write().unwrap().insert(key, val);
    }
}
