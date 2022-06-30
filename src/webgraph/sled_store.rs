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

use std::{cell::RefCell, marker::PhantomData};

use nom::AsBytes;
use serde::{de::DeserializeOwned, Serialize};

use super::{
    graph_store::{Adjacency, BlockedCachedTree, CachedTree, GraphStore},
    kv::Kv,
    Store,
};

pub struct SledStore {}

impl Store for SledStore {
    fn open<P: AsRef<std::path::Path>>(path: P) -> GraphStore<Self> {
        let db = sled::Config::default()
            .path(path)
            .use_compression(true)
            .mode(sled::Mode::LowSpace)
            .open()
            .expect("Failed to open database");

        let adjacency = Box::new(db.open_tree("adjacency").expect("unable to open sled tree"));
        let reversed_adjacency = Box::new(
            db.open_tree("reversed_adjacency")
                .expect("unable to open sled tree"),
        );
        let node2id = Box::new(db.open_tree("node2id").expect("unable to open sled tree"));
        let id2node = Box::new(db.open_tree("id2node").expect("unable to open sled tree"));
        let meta = Box::new(db.open_tree("meta").expect("unable to open sled tree"));

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

impl<K, V> Kv<K, V> for sled::Tree
where
    K: Serialize + DeserializeOwned + 'static + std::fmt::Debug,
    V: Serialize + DeserializeOwned + 'static + std::fmt::Debug,
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
        dbg!(self.inner.next()).map(|res| {
            let (key_bytes, value_bytes) = res.expect("Failed to get next record from sled tree");
            (
                bincode::deserialize(&key_bytes).expect("Failed to deserialize key"),
                bincode::deserialize(&value_bytes).expect("Failed to deserialize value"),
            )
        })
    }
}
