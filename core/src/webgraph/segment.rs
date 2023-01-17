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

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashSet},
    fmt::Debug,
    marker::PhantomData,
    path::Path,
    sync::{Arc, RwLock},
};

use super::{Edge, NodeID, Store, StoredEdge};
use crate::kv::{rocksdb_store::RocksDbStore, Kv};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentNodeID(u64);

impl From<u64> for SegmentNodeID {
    fn from(val: u64) -> Self {
        Self(val)
    }
}

pub(crate) struct Adjacency {
    pub(crate) tree: CachedStore<SegmentNodeID, HashSet<StoredEdge>>,
}

impl Adjacency {
    fn insert(&mut self, from: SegmentNodeID, to: SegmentNodeID, label: String) {
        let edge = StoredEdge { other: to, label };

        if let Some(existing) = self.tree.get(&from) {
            existing.write().unwrap().insert(edge);
        } else {
            let mut set = HashSet::new();
            set.insert(edge);
            self.tree.insert(from, set);
        }
    }

    fn edges(&self, node: &SegmentNodeID) -> Vec<StoredEdge> {
        self.tree
            .get(node)
            .map(|r| r.read().unwrap().clone())
            .unwrap_or_default()
            .into_iter()
            .collect()
    }
}

pub struct CachedStore<K, V>
where
    K: Ord + Eq + Serialize + DeserializeOwned + Clone,
    V: Serialize + DeserializeOwned + Clone,
{
    store: Box<dyn Kv<K, V> + Send + Sync>,
    cache: RwLock<BTreeMap<K, Arc<RwLock<V>>>>,
    cache_size: usize,
}

impl<K, V> CachedStore<K, V>
where
    K: Ord + Eq + Serialize + DeserializeOwned + Clone,
    V: Serialize + DeserializeOwned + Clone + Debug,
{
    pub fn new(store: Box<dyn Kv<K, V> + Send + Sync>, cache_size: usize) -> Self {
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

    pub fn get(&self, key: &K) -> Option<Arc<RwLock<V>>> {
        self.update_cache(key);
        let guard = self.cache.read().unwrap();
        guard.get(key).cloned()
    }

    pub fn insert(&mut self, key: K, value: V) {
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

    pub fn flush(&mut self) {
        let cache = self.cache.get_mut().unwrap();

        while let Some((key, value)) = cache.pop_first() {
            self.store.insert(key, value.write().unwrap().clone());
        }

        self.store.flush();
    }

    pub fn iter(&self) -> impl Iterator<Item = (K, V)> + '_ {
        self.store.iter()
    }

    pub fn delete(&mut self, key: &K) {
        self.store.delete(key);
        self.cache.write().unwrap().remove(key);
    }
}

pub struct Segment<S: Store> {
    adjacency: Adjacency,
    reversed_adjacency: Adjacency,
    id_mapping: CachedStore<NodeID, SegmentNodeID>,
    rev_id_mapping: CachedStore<SegmentNodeID, NodeID>,
    meta: CachedStore<String, u64>,
    store: PhantomData<S>,
    id: String,
    path: String,
}

impl<S: Store> Segment<S> {
    #[cfg(test)]
    pub(crate) fn temporary() -> Segment<S> {
        S::temporary()
    }

    pub fn open<P: AsRef<Path>>(path: P, id: String) -> Self {
        S::open(path, id)
    }

    pub fn open_read_only<P: AsRef<Path>>(path: P, id: String) -> Self {
        S::open_read_only(path, id)
    }

    fn next_id(&self) -> SegmentNodeID {
        let key = &"next_id".to_string();
        self.meta
            .get(key)
            .map(|arc| *arc.read().unwrap())
            .unwrap_or(0)
            .into()
    }

    pub fn num_nodes(&self) -> usize {
        self.next_id().0 as usize
    }

    fn increment_next_id(&mut self) {
        let current_id = self.next_id().0;
        let next_id = current_id + 1;
        self.meta.insert("next_id".to_string(), next_id);
    }

    fn id_and_increment(&mut self) -> SegmentNodeID {
        let id = self.next_id();
        self.increment_next_id();
        id
    }

    fn id_mapping(&self, node: &NodeID) -> Option<SegmentNodeID> {
        self.id_mapping.get(node).map(|lock| *lock.read().unwrap())
    }

    fn rev_id_mapping(&self, node: &SegmentNodeID) -> Option<NodeID> {
        self.rev_id_mapping
            .get(node)
            .map(|lock| *lock.read().unwrap())
    }

    pub fn outgoing_edges(&self, node: &NodeID) -> Vec<Edge> {
        match self.id_mapping(node) {
            Some(segment_id) => self
                .adjacency
                .edges(&segment_id)
                .into_iter()
                .map(|edge| Edge {
                    from: *node,
                    to: self.rev_id_mapping(&edge.other).unwrap(),
                    label: edge.label,
                })
                .collect(),
            None => Vec::new(),
        }
    }

    pub fn ingoing_edges(&self, node: &NodeID) -> Vec<Edge> {
        match self.id_mapping(node) {
            Some(segment_id) => self
                .reversed_adjacency
                .edges(&segment_id)
                .into_iter()
                .map(|edge| Edge {
                    from: self.rev_id_mapping(&edge.other).unwrap(),
                    to: *node,
                    label: edge.label,
                })
                .collect(),
            None => Vec::new(),
        }
    }

    fn id_or_assign(&mut self, node: &NodeID) -> SegmentNodeID {
        match self.id_mapping(node) {
            Some(id) => id,
            None => {
                let id = self.id_and_increment();

                self.id_mapping.insert(*node, id);
                self.rev_id_mapping.insert(id, *node);

                id
            }
        }
    }

    pub fn insert(&mut self, from: NodeID, to: NodeID, label: String) {
        let from_id = self.id_or_assign(&from);
        let to_id = self.id_or_assign(&to);

        self.adjacency.insert(from_id, to_id, label.clone());
        self.reversed_adjacency.insert(to_id, from_id, label);
    }

    pub fn flush(&mut self) {
        self.adjacency.tree.flush();
        self.reversed_adjacency.tree.flush();
        self.id_mapping.flush();
        self.rev_id_mapping.flush();
        self.meta.flush();
    }

    pub fn id(&self) -> String {
        self.id.clone()
    }

    pub fn path(&self) -> &Path {
        Path::new(&self.path)
    }

    pub fn edges(&self) -> impl Iterator<Item = Edge> + '_ {
        self.adjacency
            .tree
            .iter()
            .flat_map(move |(node_id, edges)| {
                let from = self.rev_id_mapping(&node_id).unwrap();

                edges.into_iter().map(move |stored_edge| Edge {
                    from,
                    to: self.rev_id_mapping(&stored_edge.other).unwrap(),
                    label: stored_edge.label,
                })
            })
    }

    pub fn merge(&mut self, mut other: Segment<S>) {
        other.flush();

        for edge in other.edges() {
            self.insert(edge.from, edge.to, edge.label);
        }

        self.flush();
    }

    pub fn update_id_mapping(&mut self, mapping: Vec<(NodeID, NodeID)>) {
        let mut new_mappings = Vec::with_capacity(mapping.len());

        for (old_id, new_id) in mapping {
            if let Some(segment_id) = self.id_mapping(&old_id) {
                self.id_mapping.delete(&old_id);
                self.rev_id_mapping.delete(&segment_id);

                new_mappings.push((segment_id, new_id));
            }
        }

        for (segment_id, new_id) in new_mappings {
            self.id_mapping.insert(new_id, segment_id);
            self.rev_id_mapping.insert(segment_id, new_id);
        }

        self.flush();
    }
}

impl Store for RocksDbStore {
    fn open<P: AsRef<std::path::Path>>(path: P, id: String) -> Segment<Self> {
        let adjacency = RocksDbStore::open(path.as_ref().join("adjacency"));
        let reversed_adjacency = RocksDbStore::open(path.as_ref().join("reversed_adjacency"));
        let id_mapping = RocksDbStore::open(path.as_ref().join("id_mapping"));
        let rev_id_mapping = RocksDbStore::open(path.as_ref().join("rev_id_mapping"));
        let meta = RocksDbStore::open(path.as_ref().join("meta"));

        Segment {
            adjacency: Adjacency {
                tree: CachedStore::new(adjacency, 100),
            },
            reversed_adjacency: Adjacency {
                tree: CachedStore::new(reversed_adjacency, 100),
            },
            id_mapping: CachedStore::new(id_mapping, 100),
            rev_id_mapping: CachedStore::new(rev_id_mapping, 100),
            meta: CachedStore::new(meta, 1_000),
            store: Default::default(),
            path: path.as_ref().as_os_str().to_str().unwrap().to_string(),
            id,
        }
    }

    fn open_read_only<P: AsRef<Path>>(path: P, id: String) -> Segment<Self> {
        let adjacency = RocksDbStore::open_read_only(path.as_ref().join("adjacency"));
        let reversed_adjacency =
            RocksDbStore::open_read_only(path.as_ref().join("reversed_adjacency"));
        let id_mapping = RocksDbStore::open_read_only(path.as_ref().join("id_mapping"));
        let rev_id_mapping = RocksDbStore::open_read_only(path.as_ref().join("rev_id_mapping"));
        let meta = RocksDbStore::open_read_only(path.as_ref().join("meta"));

        Segment {
            adjacency: Adjacency {
                tree: CachedStore::new(adjacency, 100),
            },
            reversed_adjacency: Adjacency {
                tree: CachedStore::new(reversed_adjacency, 100),
            },
            id_mapping: CachedStore::new(id_mapping, 100),
            rev_id_mapping: CachedStore::new(rev_id_mapping, 100),
            meta: CachedStore::new(meta, 1_000),
            store: Default::default(),
            path: path.as_ref().as_os_str().to_str().unwrap().to_string(),
            id,
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
        // ┌───0◄─┐ │
        // │      │ │
        // ▼      │ │
        // 1─────►2◄┘

        let mut store: Segment<RocksDbStore> = Segment::temporary();

        let a = NodeID(0);
        let b = NodeID(1);
        let c = NodeID(2);

        store.insert(a, b, String::new());
        store.insert(b, c, String::new());
        store.insert(c, a, String::new());
        store.insert(a, c, String::new());

        store.flush();

        let mut out = store.outgoing_edges(&a);
        out.sort();

        assert_eq!(
            out,
            vec![
                Edge {
                    from: a,
                    to: b,
                    label: String::new()
                },
                Edge {
                    from: a,
                    to: c,
                    label: String::new()
                },
            ]
        );

        let mut out = store.outgoing_edges(&b);
        out.sort();
        assert_eq!(
            out,
            vec![Edge {
                from: b,
                to: c,
                label: String::new()
            },]
        );

        let mut out = store.ingoing_edges(&c);
        out.sort();
        assert_eq!(
            out,
            vec![
                Edge {
                    from: a,
                    to: c,
                    label: String::new()
                },
                Edge {
                    from: b,
                    to: c,
                    label: String::new()
                },
            ]
        );

        assert_eq!(
            store.ingoing_edges(&a),
            vec![Edge {
                from: c,
                to: a,
                label: String::new()
            },]
        );

        assert_eq!(
            store.ingoing_edges(&b),
            vec![Edge {
                from: a,
                to: b,
                label: String::new()
            },]
        );
    }
}
