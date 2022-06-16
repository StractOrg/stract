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
    cell::RefCell,
    collections::HashMap,
    hash::Hash,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

use lru::LruCache;
use serde::{de::DeserializeOwned, Serialize};

use super::{Edge, Node, NodeID, NodeIterator, StoredEdge};
use crate::directory;
use crate::webgraph::GraphStore;

struct Adjacency {
    tree: CachedTree<u64, HashMap<NodeID, Vec<StoredEdge>>>,
    block_size: u64,
}

impl Adjacency {
    fn new(store: sled::Tree) -> Self {
        Self {
            tree: CachedTree::new(store, 100),
            block_size: 1_024,
        }
    }

    fn retrieve_block(&mut self, block_id: u64) -> HashMap<NodeID, Vec<StoredEdge>> {
        self.tree.get(&block_id).cloned().unwrap_or_default()
    }

    fn save_block(&mut self, block_id: u64, block: HashMap<NodeID, Vec<StoredEdge>>) {
        self.tree.insert(block_id, block)
    }

    fn insert(&mut self, from: NodeID, to: NodeID, label: String) {
        let block_id = from / self.block_size;

        if let Some(block) = self.tree.get_mut(&block_id) {
            block
                .entry(from)
                .or_default()
                .push(StoredEdge { other: to, label });
            return;
        }

        let mut new_block = HashMap::new();
        new_block.insert(from, vec![StoredEdge { other: to, label }]);

        self.tree.insert(block_id, new_block);
    }

    fn edges(&mut self, node: NodeID) -> Vec<StoredEdge> {
        let block_id = node / self.block_size;
        match self.tree.get(&block_id) {
            Some(block) => block.get(&node).cloned().unwrap_or_default(),
            None => {
                let block = self.retrieve_block(block_id);
                let res = block.get(&node).cloned().unwrap_or_default();

                res
            }
        }
    }
}

struct CachedTree<K, V> {
    store: sled::Tree,
    cache: LruCache<K, V>,
}

impl<K, V> CachedTree<K, V>
where
    K: Hash + Eq + Serialize + Clone,
    V: Serialize + DeserializeOwned + Clone,
{
    fn new(store: sled::Tree, cache_size: usize) -> Self {
        Self {
            store,
            cache: LruCache::new(cache_size),
        }
    }

    fn update_cache(&mut self, key: &K) {
        if !self.cache.contains(key) {
            let key_bytes = bincode::serialize(key).expect("failed to serialize key");
            let val: Option<V> = self
                .store
                .get(&key_bytes)
                .expect("failed to retrieve block")
                .map(|bytes| bincode::deserialize(&bytes).expect("failed to deserialize value"));

            if let Some(val) = val {
                self.cache.put(key.clone(), val.clone());
            }
        }
    }

    fn get(&mut self, key: &K) -> Option<&V> {
        self.update_cache(key);
        self.cache.get(key)
    }

    fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.update_cache(key);
        self.cache.get_mut(key)
    }

    fn insert_persisted(&self, key: K, value: V) {
        let key_bytes = bincode::serialize(&key).expect("failed to serialize key");
        let value_bytes = bincode::serialize(&value).expect("failed to serialize value");

        self.store
            .insert(key_bytes, value_bytes)
            .expect("failed to save block");
    }

    fn insert(&mut self, key: K, value: V) {
        if self.cache.len() == self.cache.cap() {
            if let Some((key, value)) = self.cache.pop_lru() {
                self.insert_persisted(key, value);
            }
        }

        self.cache.push(key, value);
    }

    fn flush(&mut self) {
        while let Some((key, value)) = self.cache.pop_lru() {
            self.insert_persisted(key, value);
        }

        self.store.flush().expect("unable to flush tree");
    }

    fn iter(&self) -> sled::Iter {
        self.store.into_iter()
    }
}

// taken from https://docs.rs/sled/0.34.7/src/sled/config.rs.html#445
#[allow(unused)]
fn gen_temp_path() -> PathBuf {
    use std::time::SystemTime;

    static SALT_COUNTER: AtomicUsize = AtomicUsize::new(0);

    let seed = SALT_COUNTER.fetch_add(1, Ordering::SeqCst) as u128;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        << 48;

    let pid = u128::from(std::process::id());

    let salt = (pid << 16) + now + seed;

    if cfg!(target_os = "linux") {
        // use shared memory for temporary linux files
        format!("/dev/shm/pagecache.tmp.{}", salt).into()
    } else {
        std::env::temp_dir().join(format!("pagecache.tmp.{}", salt))
    }
}

pub struct SledStore {
    adjacency: RefCell<Adjacency>,
    reversed_adjacency: RefCell<Adjacency>,
    node2id: RefCell<CachedTree<Node, NodeID>>,
    id2node: RefCell<CachedTree<NodeID, Node>>,
    meta: RefCell<CachedTree<String, u64>>,
    pub(crate) path: String,
}

impl SledStore {
    #[cfg(test)]
    pub(crate) fn temporary() -> Self {
        Self::open(gen_temp_path())
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        let p = path
            .as_ref()
            .as_os_str()
            .to_str()
            .expect("unknown path")
            .to_string();

        let db = sled::Config::default()
            .path(path)
            .use_compression(true)
            .mode(sled::Mode::LowSpace)
            .open()
            .expect("Failed to open database");

        Self::from_db(db, p)
    }

    fn from_db(db: sled::Db, path: String) -> Self {
        Self {
            adjacency: RefCell::new(Adjacency::new(
                db.open_tree("adjacency")
                    .expect("Could not open adjacency tree"),
            )),
            reversed_adjacency: RefCell::new(Adjacency::new(
                db.open_tree("reversed_adjacency")
                    .expect("Could not open reversed adjacency tree"),
            )),
            node2id: RefCell::new(CachedTree::new(
                db.open_tree("node2id")
                    .expect("Could not open node2id tree"),
                1_000,
            )),
            id2node: RefCell::new(CachedTree::new(
                db.open_tree("id2node")
                    .expect("Could not open id2node tree"),
                1_000,
            )),
            meta: RefCell::new(CachedTree::new(
                db.open_tree("meta").expect("Could not open metadata tree"),
                1_000,
            )),
            path,
        }
    }

    fn next_id(&self) -> NodeID {
        self.meta
            .borrow_mut()
            .get(&"next_id".to_string())
            .cloned()
            .unwrap_or(0)
    }

    fn increment_next_id(&self) {
        let current_id = self.next_id();
        let next_id = current_id + 1;
        self.meta
            .borrow_mut()
            .insert("next_id".to_string(), next_id);
    }

    fn id_and_increment(&self) -> NodeID {
        let id = self.next_id();
        self.increment_next_id();
        id
    }

    fn assign_id(&self, node: Node, id: NodeID) {
        self.node2id.borrow_mut().insert(node.clone(), id);
        self.id2node.borrow_mut().insert(id, node);
    }

    fn id_or_assign(&self, node: Node) -> NodeID {
        if let Some(id) = self.node2id.borrow_mut().get(&node) {
            return *id;
        }
        let id = self.id_and_increment();
        self.assign_id(node, id);
        id
    }

    fn flush(&self) {
        self.adjacency.borrow_mut().tree.flush();
        self.reversed_adjacency.borrow_mut().tree.flush();
        self.node2id.borrow_mut().flush();
        self.id2node.borrow_mut().flush();
        self.meta.borrow_mut().flush();
    }
}

impl GraphStore for SledStore {
    fn outgoing_edges(&self, node: NodeID) -> Vec<Edge> {
        self.adjacency
            .borrow_mut()
            .edges(node)
            .into_iter()
            .map(|edge| Edge {
                from: node,
                to: edge.other,
                label: edge.label,
            })
            .collect()
    }

    fn ingoing_edges(&self, node: NodeID) -> Vec<Edge> {
        self.reversed_adjacency
            .borrow_mut()
            .edges(node)
            .into_iter()
            .map(|edge| Edge {
                from: edge.other,
                to: node,
                label: edge.label,
            })
            .collect()
    }

    fn nodes(&self) -> NodeIterator {
        self.flush();

        let iter = IntoIter {
            inner: self.id2node.borrow_mut().iter(),
        };

        NodeIterator::from(iter)
    }

    fn insert(&mut self, from: Node, to: Node, label: String) {
        let from_id = self.id_or_assign(from);
        let to_id = self.id_or_assign(to);

        self.adjacency
            .borrow_mut()
            .insert(from_id, to_id, label.clone());
        self.reversed_adjacency
            .borrow_mut()
            .insert(to_id, from_id, label.clone());
    }

    fn node2id(&self, node: &Node) -> Option<NodeID> {
        self.node2id.borrow_mut().get(node).cloned()
    }

    fn id2node(&self, id: &NodeID) -> Option<Node> {
        self.id2node.borrow_mut().get(id).cloned()
    }

    fn serialize(&self) -> Vec<u8> {
        self.flush();
        directory::serialize(self.path.clone()).unwrap()
    }

    fn deserialize(bytes: &[u8]) -> Self {
        let path = directory::deserialize(bytes).unwrap();
        Self::open(path)
    }
}

pub struct IntoIter {
    inner: sled::Iter,
}

impl Iterator for IntoIter {
    type Item = NodeID;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|res| {
            let (node_bytes, _) = res.expect("Failed to get next record from sled tree");
            bincode::deserialize(&node_bytes).expect("Failed to deserialize node")
        })
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

        let mut store = SledStore::temporary();

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
            .map(|id| store.id2node.borrow_mut().get(&id).unwrap().clone())
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

    #[test]
    fn serialize_deserialize() {
        let mut store = SledStore::temporary();

        let a = Node {
            name: "A".to_string(),
        };
        let b = Node {
            name: "B".to_string(),
        };

        store.insert(a.clone(), b.clone(), String::new());
        let a_id = store.node2id(&a).unwrap();
        let b_id = store.node2id(&b).unwrap();

        let bytes = store.serialize();

        assert!(bytes.len() > 0);

        std::fs::remove_dir_all(store.path).unwrap();

        let store2 = SledStore::deserialize(&bytes);
        let a_id2 = store2.node2id(&a).unwrap();
        let b_id2 = store2.node2id(&b).unwrap();

        assert_eq!(a_id2, a_id);
        assert_eq!(b_id2, b_id);

        assert_eq!(
            store2.outgoing_edges(a_id),
            vec![Edge {
                from: a_id,
                to: b_id,
                label: String::new()
            },]
        );

        assert_eq!(
            store2.ingoing_edges(b_id),
            vec![Edge {
                from: a_id,
                to: b_id,
                label: String::new()
            },]
        );
    }
}
