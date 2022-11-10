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

use std::{
    collections::BTreeMap,
    fmt::Debug,
    marker::PhantomData,
    path::Path,
    sync::{Arc, RwLock},
};

use serde::{de::DeserializeOwned, Serialize};

use super::{Edge, EdgeIterator, Node, NodeID, Store, StoredEdge};
use crate::{
    intmap::IntMap,
    kv::{rocksdb_store::RocksDbStore, Kv},
};
pub(crate) struct Adjacency {
    pub(crate) tree: BlockedCachedTree<Vec<StoredEdge>>,
}

impl Adjacency {
    fn insert(&mut self, from: NodeID, to: NodeID, label: String) {
        self.tree.insert(from, &mut |block| {
            if !block.contains(&from) {
                block.insert(from, Vec::new());
            }

            block.get_mut(&from).unwrap().push(StoredEdge {
                other: to,
                label: label.clone(),
            })
        });
    }

    fn edges(&self, node: NodeID) -> Vec<StoredEdge> {
        self.tree.get(&node).unwrap_or_default()
    }
}

pub(crate) struct BlockedCachedTree<V>
where
    V: Serialize + DeserializeOwned + Clone + Debug,
{
    pub(crate) inner: CachedTree<u64, IntMap<V>>,
    pub(crate) block_size: u64,
}

impl<V> BlockedCachedTree<V>
where
    V: Serialize + DeserializeOwned + Clone + Debug,
{
    fn insert<B>(&mut self, key: NodeID, mutate_block: &mut B)
    where
        B: FnMut(&mut IntMap<V>),
    {
        let block_id = key / self.block_size;

        {
            if let Some(block) = self.inner.get(&block_id) {
                let mut block = block.write().unwrap();
                mutate_block(&mut block);
                return;
            }
        }

        let mut new_block = IntMap::new();
        mutate_block(&mut new_block);

        self.inner.insert(block_id, new_block);
    }

    fn get(&self, key: &NodeID) -> Option<V> {
        let block_id = key / self.block_size;
        self.inner
            .get(&block_id)
            .and_then(|block| block.read().unwrap().get(key).cloned())
    }
}

pub(crate) struct CachedTree<K, V>
where
    K: Ord + Eq + Serialize + DeserializeOwned + Clone,
    V: Serialize + DeserializeOwned + Clone,
{
    store: Box<dyn Kv<K, V> + Send + Sync>,
    cache: RwLock<BTreeMap<K, Arc<RwLock<V>>>>,
    cache_size: usize,
}

impl<K, V> CachedTree<K, V>
where
    K: Ord + Eq + Serialize + DeserializeOwned + Clone,
    V: Serialize + DeserializeOwned + Clone + Debug,
{
    pub(crate) fn new(store: Box<dyn Kv<K, V> + Send + Sync>, cache_size: usize) -> Self {
        Self {
            store,
            cache_size,
            cache: RwLock::new(BTreeMap::new()),
        }
    }

    fn update_cache(&self, key: &K) {
        if !self.cache.read().unwrap().contains_key(key) {
            let val = self.store.get(key);

            if let Some(val) = val {
                self.cache
                    .write()
                    .unwrap()
                    .insert(key.clone(), Arc::new(RwLock::new(val)));
            }
        }
    }

    pub(crate) fn get(&self, key: &K) -> Option<Arc<RwLock<V>>> {
        self.update_cache(key);
        let guard = self.cache.read().unwrap();
        guard.get(key).cloned()
    }

    fn insert(&mut self, key: K, value: V) {
        let cache = self.cache.get_mut().unwrap();

        cache.insert(key, Arc::new(RwLock::new(value)));

        if cache.len() >= self.cache_size {
            while cache.len() > self.cache_size / 2 {
                if let Some((key, value)) = cache.pop_first() {
                    self.store.insert(key, value.write().unwrap().clone());
                }
            }

            self.store.flush();
        }
    }

    pub(crate) fn flush(&mut self) {
        let cache = self.cache.get_mut().unwrap();

        while let Some((key, value)) = cache.pop_first() {
            self.store.insert(key, value.write().unwrap().clone());
        }

        self.store.flush();
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (K, V)> + '_ {
        self.store.iter()
    }
}

pub struct GraphStore<S> {
    pub(crate) adjacency: Adjacency,
    pub(crate) reversed_adjacency: Adjacency,
    pub(crate) node2id: CachedTree<Node, NodeID>,
    pub(crate) id2node: BlockedCachedTree<Node>,
    pub(crate) meta: CachedTree<String, u64>,
    pub(crate) store: PhantomData<S>,
}

impl<S: Store> GraphStore<S> {
    #[cfg(test)]
    pub(crate) fn temporary() -> GraphStore<S> {
        S::temporary()
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        S::open(path)
    }

    pub fn open_read_only<P: AsRef<Path>>(path: P) -> Self {
        S::open_read_only(path)
    }

    fn next_id(&self) -> NodeID {
        let key = &"next_id".to_string();
        self.meta
            .get(key)
            .map(|arc| *arc.read().unwrap())
            .unwrap_or(0)
    }

    fn increment_next_id(&mut self) {
        let current_id = self.next_id();
        let next_id = current_id + 1;
        self.meta.insert("next_id".to_string(), next_id);
    }

    fn id_and_increment(&mut self) -> NodeID {
        let id = self.next_id();
        self.increment_next_id();
        id
    }

    fn assign_id(&mut self, node: Node, id: NodeID) {
        self.node2id.insert(node.clone(), id);
        self.id2node.insert(id, &mut |block| {
            block.insert(id, node.clone());
        });
    }

    fn id_or_assign(&mut self, node: Node) -> NodeID {
        if let Some(id) = self.node2id.get(&node) {
            return *id.read().unwrap();
        }
        let id = self.id_and_increment();
        self.assign_id(node, id);
        id
    }

    #[allow(unused)]
    pub fn outgoing_edges(&self, node: NodeID) -> Vec<Edge> {
        self.adjacency
            .edges(node)
            .into_iter()
            .map(|edge| Edge {
                from: node,
                to: edge.other,
                label: edge.label,
            })
            .collect()
    }

    pub fn ingoing_edges(&self, node: NodeID) -> Vec<Edge> {
        self.reversed_adjacency
            .edges(node)
            .into_iter()
            .map(|edge| Edge {
                from: edge.other,
                to: node,
                label: edge.label,
            })
            .collect()
    }

    pub fn nodes(&self) -> impl Iterator<Item = NodeID> {
        self.node2id
            .iter()
            .map(|(_, id)| id)
            .collect::<Vec<u64>>()
            .into_iter()
    }

    pub fn insert(&mut self, from: Node, to: Node, label: String) {
        let from_id = self.id_or_assign(from);
        let to_id = self.id_or_assign(to);

        self.adjacency.insert(from_id, to_id, label.clone());
        self.reversed_adjacency.insert(to_id, from_id, label);
    }

    pub fn node2id(&self, node: &Node) -> Option<NodeID> {
        self.node2id.get(node).map(|arc| *arc.read().unwrap())
    }

    pub fn id2node(&self, id: &NodeID) -> Option<Node> {
        self.id2node.get(id)
    }

    pub fn flush(&mut self) {
        self.adjacency.tree.inner.flush();
        self.reversed_adjacency.tree.inner.flush();
        self.node2id.flush();
        self.id2node.inner.flush();
        self.meta.flush();
    }

    pub fn edges(&self) -> EdgeIterator<'_> {
        EdgeIterator::new(&self.adjacency)
    }

    pub fn append(&mut self, mut other: GraphStore<S>) {
        other.flush();

        for edge in other.edges() {
            let from = other.id2node(&edge.from).expect("node not found");
            let to = other.id2node(&edge.to).expect("node not found");

            self.insert(from, to, edge.label);
        }

        self.flush();
    }
}

impl Store for RocksDbStore {
    fn open<P: AsRef<std::path::Path>>(path: P) -> GraphStore<Self> {
        let adjacency = RocksDbStore::open(path.as_ref().join("adjacency"));
        let reversed_adjacency = RocksDbStore::open(path.as_ref().join("reversed_adjacency"));
        let node2id = RocksDbStore::open(path.as_ref().join("node2id"));
        let id2node = RocksDbStore::open(path.as_ref().join("id2node"));
        let meta = RocksDbStore::open(path.as_ref().join("meta"));

        GraphStore {
            adjacency: Adjacency {
                tree: BlockedCachedTree {
                    inner: CachedTree::new(adjacency, 10_000),
                    block_size: 1_024,
                },
            },
            reversed_adjacency: Adjacency {
                tree: BlockedCachedTree {
                    inner: CachedTree::new(reversed_adjacency, 10_000),
                    block_size: 1_024,
                },
            },
            node2id: CachedTree::new(node2id, 100_000),
            id2node: BlockedCachedTree {
                inner: CachedTree::new(id2node, 100_000),
                block_size: 1_024,
            },
            meta: CachedTree::new(meta, 1_000),
            store: Default::default(),
        }
    }

    fn open_read_only<P: AsRef<Path>>(path: P) -> GraphStore<Self> {
        let adjacency = RocksDbStore::open_read_only(path.as_ref().join("adjacency"));
        let reversed_adjacency =
            RocksDbStore::open_read_only(path.as_ref().join("reversed_adjacency"));
        let node2id = RocksDbStore::open_read_only(path.as_ref().join("node2id"));
        let id2node = RocksDbStore::open_read_only(path.as_ref().join("id2node"));
        let meta = RocksDbStore::open_read_only(path.as_ref().join("meta"));

        GraphStore {
            adjacency: Adjacency {
                tree: BlockedCachedTree {
                    inner: CachedTree::new(adjacency, 10_000),
                    block_size: 1_024,
                },
            },
            reversed_adjacency: Adjacency {
                tree: BlockedCachedTree {
                    inner: CachedTree::new(reversed_adjacency, 10_000),
                    block_size: 1_024,
                },
            },
            node2id: CachedTree::new(node2id, 100_000),
            id2node: BlockedCachedTree {
                inner: CachedTree::new(id2node, 100_000),
                block_size: 1_024,
            },
            meta: CachedTree::new(meta, 1_000),
            store: Default::default(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn simple_triangle_graph() {
        //     ┌────┐
        //     │    │
        // ┌───A◄─┐ │
        // │      │ │
        // ▼      │ │
        // B─────►C◄┘

        let mut store: GraphStore<RocksDbStore> = GraphStore::temporary();

        let a = Node {
            name: "A".to_string(),
        };
        let b = Node {
            name: "B".to_string(),
        };
        let c = Node {
            name: "C".to_string(),
        };

        store.insert(a.clone(), b.clone(), String::new());
        store.insert(b.clone(), c.clone(), String::new());
        store.insert(c.clone(), a.clone(), String::new());
        store.insert(a.clone(), c.clone(), String::new());

        store.flush();

        let nodes: Vec<Node> = store
            .nodes()
            .map(|id| store.id2node(&id).unwrap())
            .collect();
        assert_eq!(nodes, vec![a.clone(), b.clone(), c.clone()]);

        let a_id = store.node2id(&a).unwrap();
        let b_id = store.node2id(&b).unwrap();
        let c_id = store.node2id(&c).unwrap();

        assert_eq!(
            store.outgoing_edges(a_id),
            vec![
                Edge {
                    from: a_id,
                    to: b_id,
                    label: String::new()
                },
                Edge {
                    from: a_id,
                    to: c_id,
                    label: String::new()
                },
            ]
        );

        assert_eq!(
            store.outgoing_edges(b_id),
            vec![Edge {
                from: b_id,
                to: c_id,
                label: String::new()
            },]
        );

        assert_eq!(
            store.ingoing_edges(c_id),
            vec![
                Edge {
                    from: b_id,
                    to: c_id,
                    label: String::new()
                },
                Edge {
                    from: a_id,
                    to: c_id,
                    label: String::new()
                },
            ]
        );

        assert_eq!(
            store.ingoing_edges(a_id),
            vec![Edge {
                from: c_id,
                to: a_id,
                label: String::new()
            },]
        );

        assert_eq!(
            store.ingoing_edges(b_id),
            vec![Edge {
                from: a_id,
                to: b_id,
                label: String::new()
            },]
        );
    }
}
