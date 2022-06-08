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
use std::{collections::HashMap, path::Path, sync::Mutex};

use lru::LruCache;

use super::{Edge, Node, NodeID, NodeIterator, StoredEdge};
use crate::webgraph::GraphStore;

const DEFAULT_CACHE_SIZE: usize = 10;
const DEFAULT_BLOCK_SIZE: u64 = 1_024;

struct Adjacency {
    store: sled::Tree,
    cache: Mutex<LruCache<u64, HashMap<NodeID, Vec<StoredEdge>>>>,
}

impl Adjacency {
    fn new(store: sled::Tree) -> Self {
        Self {
            store,
            cache: Mutex::new(LruCache::new(DEFAULT_CACHE_SIZE)),
        }
    }

    fn invalidate_cache(&mut self, block_id: u64) {
        self.cache.lock().unwrap().pop(&block_id);
    }

    fn retrieve_block(&self, block_id: u64) -> HashMap<NodeID, Vec<StoredEdge>> {
        let block_bytes = bincode::serialize(&block_id).expect("failed to serialize block id");
        self.store
            .get(&block_bytes)
            .expect("failed to retrieve block")
            .map(|bytes| bincode::deserialize(&bytes).expect("failed to deserialize block"))
            .unwrap_or_default()
    }

    fn save_block(&mut self, block_id: u64, block: HashMap<NodeID, Vec<StoredEdge>>) {
        let block_id_bytes = bincode::serialize(&block_id).expect("failed to serialize block id");
        let block_bytes = bincode::serialize(&block).expect("failed to serialize block");

        self.store
            .insert(block_id_bytes, block_bytes)
            .expect("failed to save block");
    }

    fn insert(&mut self, from: NodeID, to: NodeID, label: String) {
        let block_id = from / DEFAULT_BLOCK_SIZE;
        self.invalidate_cache(block_id);

        let mut block = self.retrieve_block(block_id);
        block
            .entry(from)
            .or_default()
            .push(StoredEdge { other: to, label });

        self.save_block(block_id, block);
    }

    fn edges(&self, node: NodeID) -> Vec<StoredEdge> {
        let block_id = node / DEFAULT_BLOCK_SIZE;
        let mut cache_lock = self.cache.lock().unwrap();
        match cache_lock.get(&block_id) {
            Some(block) => block.get(&node).map(|v| v.clone()).unwrap_or_default(),
            None => {
                let block = self.retrieve_block(block_id);
                let res = block.get(&node).map(|v| v.clone()).unwrap_or_default();
                cache_lock.push(block_id, block);

                res
            }
        }
    }
}

pub struct SledStore {
    adjacency: Adjacency,
    reversed_adjacency: Adjacency,
    node2id: sled::Tree,
    id2node: sled::Tree,
    meta: sled::Tree,
}

impl SledStore {
    #[cfg(test)]
    pub(crate) fn temporary() -> Self {
        let db = sled::Config::default()
            .temporary(true)
            .use_compression(true)
            .mode(sled::Mode::LowSpace)
            .open()
            .expect("Failed to open database");

        Self::from_db(db)
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
            adjacency: Adjacency::new(
                db.open_tree("adjacency")
                    .expect("Could not open adjacency tree"),
            ),
            reversed_adjacency: Adjacency::new(
                db.open_tree("reversed_adjacency")
                    .expect("Could not open reversed adjacency tree"),
            ),
            node2id: db
                .open_tree("node2id")
                .expect("Could not open node2id tree"),
            id2node: db
                .open_tree("id2node")
                .expect("Could not open id2node tree"),
            meta: db.open_tree("meta").expect("Could not open metadata tree"),
        }
    }

    fn next_id(&self) -> NodeID {
        match self
            .meta
            .get("next_id")
            .expect("Encountered error when retrieving next_id from metadata tree")
        {
            Some(next_id) => bincode::deserialize(&next_id).expect("Could not desialize"),
            None => 0,
        }
    }

    fn increment_next_id(&mut self) {
        let current_id = self.next_id();
        let next_id = current_id + 1;
        self.meta
            .insert(
                "next_id",
                bincode::serialize(&next_id).expect("Failed to serialize integer"),
            )
            .expect("Failed to insert next_id into metadata tree");
        self.meta.flush().expect("Failed to flush");
    }

    fn id_and_increment(&mut self) -> NodeID {
        let id = self.next_id();
        self.increment_next_id();
        id
    }

    fn assign_id(&self, node: &Node, id: NodeID) {
        let node_bytes = bincode::serialize(node).expect("Failed to serialize node");
        let id_bytes = bincode::serialize(&id).expect("Failed to serialize integer");

        self.node2id
            .insert(node_bytes.clone(), id_bytes.clone())
            .expect("Failed to assign id to node");

        self.id2node
            .insert(id_bytes, node_bytes)
            .expect("Failed to assign id to node");
    }

    fn get_id(&self, node: &Node) -> Option<NodeID> {
        let serialized_node = bincode::serialize(node).expect("Failed to serialize node");
        self.node2id
            .get(serialized_node)
            .expect("Failed to use node2id tree")
            .map(|id| bincode::deserialize(&id).expect("Failed to deserialize integer"))
    }

    fn id_or_assign(&mut self, node: &Node) -> NodeID {
        match self.get_id(node) {
            Some(id) => id,
            None => {
                let id = self.id_and_increment();
                self.assign_id(node, id);
                id
            }
        }
    }

    fn get_node(&self, id: &NodeID) -> Option<Node> {
        let id_bytes = bincode::serialize(id).expect("Failed to serialize id");
        self.id2node
            .get(&id_bytes)
            .expect("Failed to retrieve node by id")
            .map(|bytes| bincode::deserialize(&bytes).expect("Failed to deserialize node"))
    }
}

impl GraphStore for SledStore {
    fn outgoing_edges(&self, node: NodeID) -> Vec<Edge> {
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

    fn ingoing_edges(&self, node: NodeID) -> Vec<Edge> {
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

    fn nodes(&self) -> NodeIterator {
        let iter = IntoIter {
            inner: self.id2node.into_iter(),
        };

        NodeIterator::from(iter)
    }

    fn insert(&mut self, from: Node, to: Node, label: String) {
        let from_id = self.id_or_assign(&from);
        let to_id = self.id_or_assign(&to);

        self.adjacency.insert(from_id, to_id, label.clone());
        self.reversed_adjacency
            .insert(to_id, from_id, label.clone());
    }

    fn node2id(&self, node: &Node) -> Option<NodeID> {
        self.get_id(node)
    }

    fn id2node(&self, id: &NodeID) -> Option<Node> {
        self.get_node(id)
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

        let nodes: Vec<Node> = store
            .nodes()
            .map(|id| store.get_node(&id).unwrap())
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
