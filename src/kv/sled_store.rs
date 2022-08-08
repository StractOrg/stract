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

use std::marker::PhantomData;

use nom::AsBytes;
use serde::{de::DeserializeOwned, Serialize};

use crate::kv::Kv;

pub struct SledStore {}

impl<K, V> Kv<K, V> for sled::Tree
where
    K: Serialize + DeserializeOwned + 'static,
    V: Serialize + DeserializeOwned + 'static,
{
    fn get_raw(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.get(key)
            .expect("failed to retrieve key")
            .map(|v| v.as_bytes().to_vec())
    }

    fn insert_raw(&self, key: Vec<u8>, value: Vec<u8>) {
        self.insert(key, value).expect("failed to insert value");
    }

    fn flush(&self) {
        self.flush().expect("failed to flush");
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (K, V)>> {
        Box::new(IntoIter {
            inner: self.iter(),
            key: Default::default(),
            value: Default::default(),
        })
    }
}

pub struct IntoIter<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    inner: sled::Iter,
    key: PhantomData<K>,
    value: PhantomData<V>,
}

impl<K, V> Iterator for IntoIter<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|res| {
            let (key_bytes, value_bytes) = res.expect("Failed to get next record from sled tree");
            (
                bincode::deserialize(&key_bytes).expect("Failed to deserialize key"),
                bincode::deserialize(&value_bytes).expect("Failed to deserialize value"),
            )
        })
    }
}
