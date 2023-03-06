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

use std::{fs, marker::PhantomData};

use rocksdb::{
    DBIteratorWithThreadMode, DBWithThreadMode, IteratorMode, Options, SingleThreaded, DB,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::kv::Kv;

pub struct RocksDbStore {}

impl RocksDbStore {
    pub fn open<K, V, P>(path: P) -> Box<dyn Kv<K, V> + Send + Sync>
    where
        P: AsRef<std::path::Path>,
        K: Serialize + DeserializeOwned + 'static,
        V: Serialize + DeserializeOwned + 'static,
    {
        if !path.as_ref().exists() {
            fs::create_dir_all(path.as_ref()).expect("faild to create dir");
        }

        let mut options = Options::default();

        options.create_if_missing(true);
        options.set_max_open_files(1);
        options.set_max_file_opening_threads(1);

        Box::new(DB::open(&options, path).expect("unable to open rocks db"))
    }

    pub fn open_read_only<K, V, P>(path: P) -> Box<dyn Kv<K, V> + Send + Sync>
    where
        P: AsRef<std::path::Path>,
        K: Serialize + DeserializeOwned + 'static,
        V: Serialize + DeserializeOwned + 'static,
    {
        if !path.as_ref().exists() {
            fs::create_dir_all(path.as_ref()).expect("faild to create dir");
        }

        let mut options = Options::default();

        options.create_if_missing(true);
        options.set_max_open_files(1);
        options.set_max_file_opening_threads(1);

        Box::new(DB::open_for_read_only(&options, path, false).expect("unable to open rocks db"))
    }
}

impl<K, V> Kv<K, V> for rocksdb::DB
where
    K: Serialize + DeserializeOwned + 'static,
    V: Serialize + DeserializeOwned + 'static,
{
    fn get_raw(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.get(key).expect("failed to retrieve key")
    }

    fn insert_raw(&self, key: Vec<u8>, value: Vec<u8>) {
        self.put(key, value).expect("failed to insert value");
    }

    fn flush(&self) {
        if let Err(err) = self.flush() {
            match err.kind() {
                rocksdb::ErrorKind::NotSupported => {}
                _ => panic!("failed to flush: {err:?}"),
            }
        }
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (K, V)> + 'a> {
        let iter = self.iterator(IteratorMode::Start);

        Box::new(IntoIter {
            inner: iter,
            key: PhantomData::default(),
            value: PhantomData::default(),
        })
    }

    fn delete_raw(&mut self, key: &[u8]) {
        rocksdb::DB::delete(self, key).unwrap();
    }
}

pub struct IntoIter<'a, K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    inner: DBIteratorWithThreadMode<'a, DBWithThreadMode<SingleThreaded>>,
    key: PhantomData<K>,
    value: PhantomData<V>,
}

impl<'a, K, V> Iterator for IntoIter<'a, K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .and_then(|r| r.ok())
            .map(|(key_bytes, value_bytes)| {
                (
                    bincode::deserialize(&key_bytes).expect("Failed to deserialize key"),
                    bincode::deserialize(&value_bytes).expect("Failed to deserialize value"),
                )
            })
    }
}
