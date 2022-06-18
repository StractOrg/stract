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
use std::{cell::RefCell, collections::HashMap, hash::Hash, ops::Div, path::Path};

use lru::LruCache;
use serde::{de::DeserializeOwned, Serialize};

use super::{Edge, EdgeIterator, Node, NodeID, NodeIterator, StoredEdge};
use crate::webgraph::GraphStore;

struct Adjacency {
    tree: BlockedCachedTree<NodeID, Vec<StoredEdge>>,
}

impl Adjacency {
    fn insert(&mut self, from: NodeID, to: NodeID, label: String) {
        self.tree.insert(from, &mut |block| {
            block.entry(from).or_default().push(StoredEdge {
                other: to,
                label: label.clone(),
            })
        });
    }

    fn edges(&mut self, node: NodeID) -> Vec<StoredEdge> {
        self.tree.get(&node).cloned().unwrap_or_default()
    }

    fn iter(&self) -> impl Iterator<Item = (u64, HashMap<NodeID, Vec<StoredEdge>>)> {
        self.tree.inner.iter().map(|res| {
            let (block_id, block) = res.expect("failed to iterate tree");
            (
                bincode::deserialize(&block_id).unwrap(),
                bincode::deserialize(&block).unwrap(),
            )
        })
    }
}

struct BlockedCachedTree<K, V> {
    inner: CachedTree<u64, HashMap<K, V>>,
    block_size: u64,
}

impl<K, V> BlockedCachedTree<K, V>
where
    K: Hash + Eq + Serialize + Clone + Div<u64, Output = u64> + DeserializeOwned,
    V: Serialize + DeserializeOwned + Clone,
{
    fn insert<B>(&mut self, key: K, mutate_block: &mut B)
    where
        B: FnMut(&mut HashMap<K, V>),
    {
        let block_id = key / self.block_size;

        if let Some(block) = self.inner.get_mut(&block_id) {
            // block.entry(key).or_default().push(value);
            // block.insert(key, value);
            mutate_block(block);
            return;
        }

        let mut new_block = HashMap::new();
        mutate_block(&mut new_block);

        self.inner.insert(block_id, new_block);
    }

    fn get(&mut self, key: &K) -> Option<&V> {
        let block_id = key.clone() / self.block_size;
        self.inner.get(&block_id).and_then(|block| block.get(key))
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
    fn new(db: &sled::Db, name: &str, cache_size: usize) -> Self {
        let name = name.to_string();

        let store = db
            .open_tree(name.clone())
            .expect("unable to open sled tree");

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
                self.cache.put(key.clone(), val);
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
        for (key, value) in self.cache.iter() {
            self.insert_persisted(key.clone(), value.clone());
        }

        self.store.flush().expect("unable to flush tree");
    }

    fn iter(&self) -> sled::Iter {
        self.store.into_iter()
    }
}

pub struct SledStore {
    adjacency: RefCell<Adjacency>,
    reversed_adjacency: RefCell<Adjacency>,
    node2id: RefCell<CachedTree<Node, NodeID>>,
    id2node: RefCell<BlockedCachedTree<NodeID, Node>>,
    meta: RefCell<CachedTree<String, u64>>,
}

impl SledStore {
    #[cfg(test)]
    pub(crate) fn temporary() -> Self {
        Self::open(super::gen_temp_path())
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        let db = sled::Config::default()
            .path(path)
            .use_compression(true)
            .mode(sled::Mode::LowSpace)
            .open()
            .expect("Failed to open database");

        Self::from_db(db)
    }

    fn from_db(db: sled::Db) -> Self {
        Self {
            adjacency: RefCell::new(Adjacency {
                tree: BlockedCachedTree {
                    inner: CachedTree::new(&db, "adjacency", 100),
                    block_size: 1_024,
                },
            }),
            reversed_adjacency: RefCell::new(Adjacency {
                tree: BlockedCachedTree {
                    inner: CachedTree::new(&db, "reversed_adjacency", 100),
                    block_size: 1_024,
                },
            }),
            node2id: RefCell::new(CachedTree::new(&db, "node2id", 1_000)),
            id2node: RefCell::new(BlockedCachedTree {
                inner: CachedTree::new(&db, "id2node", 1_000),
                block_size: 1_024,
            }),
            meta: RefCell::new(CachedTree::new(&db, "meta", 1_000)),
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
        self.id2node.borrow_mut().insert(id, &mut |block| {
            block.insert(id, node.clone());
        });
    }

    fn id_or_assign(&self, node: Node) -> NodeID {
        if let Some(id) = self.node2id.borrow_mut().get(&node) {
            return *id;
        }
        let id = self.id_and_increment();
        self.assign_id(node, id);
        id
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
            inner: self.node2id.borrow_mut().iter(),
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
            .insert(to_id, from_id, label);
    }

    fn node2id(&self, node: &Node) -> Option<NodeID> {
        self.node2id.borrow_mut().get(node).cloned()
    }

    fn id2node(&self, id: &NodeID) -> Option<Node> {
        self.id2node.borrow_mut().get(id).cloned()
    }

    fn flush(&self) {
        self.adjacency.borrow_mut().tree.inner.flush();
        self.reversed_adjacency.borrow_mut().tree.inner.flush();
        self.node2id.borrow_mut().flush();
        self.id2node.borrow_mut().inner.flush();
        self.meta.borrow_mut().flush();
    }

    fn edges(&self) -> EdgeIterator<'_> {
        self.flush();

        EdgeIterator::from(
            self.adjacency
                .borrow()
                .iter()
                .flat_map(|(_block_id, block)| {
                    block.into_iter().flat_map(|(node_id, edges)| {
                        edges.into_iter().map(move |edge| Edge {
                            from: node_id,
                            to: edge.other,
                            label: edge.label,
                        })
                    })
                }),
        )
    }
}

pub struct IntoIter {
    inner: sled::Iter,
}

impl Iterator for IntoIter {
    type Item = NodeID;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|res| {
            let (_, id_bytes) = res.expect("Failed to get next record from sled tree");
            bincode::deserialize(&id_bytes).expect("Failed to deserialize node")
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
}
