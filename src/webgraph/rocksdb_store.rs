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

use std::{cell::RefCell, fs, marker::PhantomData};

use rocksdb::{DBIteratorWithThreadMode, DBWithThreadMode, IteratorMode, SingleThreaded, DB};
use serde::{de::DeserializeOwned, Serialize};

use super::{
    graph_store::{Adjacency, BlockedCachedTree, CachedTree, GraphStore},
    kv::Kv,
    Store,
};

pub struct RocksDbStore {}

impl RocksDbStore {
    fn open_db<K, V, P>(path: P) -> Box<dyn Kv<K, V> + Send + Sync>
    where
        P: AsRef<std::path::Path>,
        K: Serialize + DeserializeOwned + 'static,
        V: Serialize + DeserializeOwned + 'static,
    {
        if !path.as_ref().exists() {
            fs::create_dir_all(path.as_ref()).expect("faild to create dir");
        }

        Box::new(DB::open_default(path).expect("unable to open rocks db"))
    }
}

impl Store for RocksDbStore {
    fn open<P: AsRef<std::path::Path>>(path: P) -> GraphStore<Self> {
        let adjacency = RocksDbStore::open_db(path.as_ref().join("adjacency"));
        let reversed_adjacency = RocksDbStore::open_db(path.as_ref().join("reversed_adjacency"));
        let node2id = RocksDbStore::open_db(path.as_ref().join("node2id"));
        let id2node = RocksDbStore::open_db(path.as_ref().join("id2node"));
        let meta = RocksDbStore::open_db(path.as_ref().join("meta"));

        GraphStore {
            adjacency: RefCell::new(Adjacency {
                tree: BlockedCachedTree {
                    inner: CachedTree::new(adjacency, 10_000),
                    block_size: 1_024,
                },
            }),
            reversed_adjacency: RefCell::new(Adjacency {
                tree: BlockedCachedTree {
                    inner: CachedTree::new(reversed_adjacency, 10_000),
                    block_size: 1_024,
                },
            }),
            node2id: RefCell::new(CachedTree::new(node2id, 100_000)),
            id2node: RefCell::new(BlockedCachedTree {
                inner: CachedTree::new(id2node, 100_000),
                block_size: 1_024,
            }),
            meta: RefCell::new(CachedTree::new(meta, 1_000)),
            store: Default::default(),
        }
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
        self.flush().expect("failed to flush");
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (K, V)> + 'a> {
        let iter = self.iterator(IteratorMode::Start);

        Box::new(IntoIter {
            inner: iter,
            key: Default::default(),
            value: Default::default(),
        })
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
        self.inner.next().map(|(key_bytes, value_bytes)| {
            (
                bincode::deserialize(&key_bytes).expect("Failed to deserialize key"),
                bincode::deserialize(&value_bytes).expect("Failed to deserialize value"),
            )
        })
    }
}
