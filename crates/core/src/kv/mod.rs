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

pub mod rocksdb_store;

pub trait Kv<K, V>
where
    K: bincode::Encode + bincode::Decode,
    V: bincode::Encode + bincode::Decode,
    Self: Send + Sync,
{
    fn approx_len(&self) -> usize;
    fn get_raw(&self, key: &[u8]) -> Option<Vec<u8>>;
    fn insert_raw(&self, key: Vec<u8>, value: Vec<u8>);
    fn flush(&self);
    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (K, V)> + 'a>;

    fn get(&self, key: &K) -> Option<V> {
        let key_bytes = bincode::encode_to_vec(key, bincode::config::standard())
            .expect("failed to serialize key");

        self.get_raw(&key_bytes).map(|bytes| {
            let (res, _) = bincode::decode_from_slice(&bytes, bincode::config::standard())
                .expect("failed to deserialize stored value");
            res
        })
    }

    fn insert(&self, key: K, value: V) {
        let key_bytes = bincode::encode_to_vec(&key, bincode::config::standard())
            .expect("failed to serialize key");
        let val_bytes = bincode::encode_to_vec(&value, bincode::config::standard())
            .expect("failed to serialize value");

        self.insert_raw(key_bytes, val_bytes);
    }
}
