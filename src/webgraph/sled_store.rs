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
use std::path::Path;

use super::{Edge, InternalEdge, Node, NodeID, NodeIterator};
use crate::webgraph::GraphStore;

pub struct SledStore {
    adjacency: sled::Tree,
    reversed_adjacency: sled::Tree,
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
            adjacency: db
                .open_tree("adjacency")
                .expect("Could not open adjacency tree"),
            reversed_adjacency: db
                .open_tree("reversed_adjacency")
                .expect("Could not open reversed adjacency tree"),
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

    fn insert_adjacency(&mut self, from: NodeID, to: NodeID, label: String) {
        let from_bytes = bincode::serialize(&from).expect("Failed to serialize id");

        let mut adjacency_list: Vec<_> = self
            .adjacency
            .get(&from_bytes)
            .expect("Failed to retrieve adjancecy list")
            .map(|bytes| {
                bincode::deserialize(&bytes).expect("Failed to deserialize adjacency list")
            })
            .unwrap_or_default();

        let internal_edge = InternalEdge { to_node: to, label };
        adjacency_list.push(internal_edge);

        let serialized_adjacency =
            bincode::serialize(&adjacency_list).expect("Failed to serialize adjacency list");

        self.adjacency
            .insert(from_bytes, serialized_adjacency)
            .expect("Failed to store new adjacency list");
    }

    fn insert_reverse_adjacency(&mut self, from: NodeID, to: NodeID, label: String) {
        let to_bytes = bincode::serialize(&to).expect("Failed to serialize id");

        let mut adjacency_list: Vec<_> = self
            .reversed_adjacency
            .get(&to_bytes)
            .expect("Failed to retrieve adjacency list")
            .map(|bytes| {
                bincode::deserialize(&bytes).expect("Failed to deserialize adjacency list")
            })
            .unwrap_or_default();

        let internal_edge = InternalEdge {
            to_node: from,
            label,
        };
        adjacency_list.push(internal_edge);

        let serialized_adjacency =
            bincode::serialize(&adjacency_list).expect("Failed to serialize adjacency list");

        self.reversed_adjacency
            .insert(to_bytes, serialized_adjacency)
            .expect("Failed to store new adjacency list");
    }

    fn out_edges(&self, node: Node) -> Vec<InternalEdge> {
        let id = self.get_id(&node);
        if id.is_none() {
            return Vec::new();
        }
        let id = id.expect("ID was deemed to not be None");
        let id_bytes = bincode::serialize(&id).expect("Failed to serialize id");

        match self
            .adjacency
            .get(&id_bytes)
            .expect("Failed to retrieve adjacency list")
        {
            Some(bytes) => {
                bincode::deserialize(&bytes).expect("Failed to deserialize adjacency list")
            }
            None => Vec::new(),
        }
    }

    fn in_edges(&self, node: Node) -> Vec<InternalEdge> {
        let id = self.get_id(&node);
        if id.is_none() {
            return Vec::new();
        }
        let id = id.expect("ID was deemed to not be None");
        let id_bytes = bincode::serialize(&id).expect("Failed to serialize id");

        match self
            .reversed_adjacency
            .get(&id_bytes)
            .expect("Failed to retrieve adjacency list")
        {
            Some(bytes) => {
                bincode::deserialize(&bytes).expect("Failed to deserialize adjacency list")
            }
            None => Vec::new(),
        }
    }

    fn get_node(&self, id: NodeID) -> Option<Node> {
        let id_bytes = bincode::serialize(&id).expect("Failed to serialize id");
        self.id2node
            .get(&id_bytes)
            .expect("Failed to retrieve node by id")
            .map(|bytes| bincode::deserialize(&bytes).expect("Failed to deserialize node"))
    }
}

impl GraphStore for SledStore {
    fn outgoing_edges(&self, node: Node) -> Vec<Edge> {
        self.out_edges(node.clone())
            .into_iter()
            .map(|internal_edge| {
                let to = self
                    .get_node(internal_edge.to_node)
                    .expect("Node and ids are out of sync");
                let from = node.clone();

                Edge {
                    from,
                    to,
                    label: internal_edge.label,
                }
            })
            .collect()
    }

    fn ingoing_edges(&self, node: Node) -> Vec<Edge> {
        self.in_edges(node.clone())
            .into_iter()
            .map(|internal_edge| {
                let from = self
                    .get_node(internal_edge.to_node)
                    .expect("Node and ids are out of sync");
                let to = node.clone();

                Edge {
                    from,
                    to,
                    label: internal_edge.label,
                }
            })
            .collect()
    }

    fn nodes(&self) -> NodeIterator {
        let iter = IntoIter {
            inner: self.node2id.into_iter(),
        };

        NodeIterator::from(iter)
    }

    fn insert(&mut self, edge: Edge) {
        let from_id = self.id_or_assign(&edge.from);
        let to_id = self.id_or_assign(&edge.to);

        self.insert_adjacency(from_id, to_id, edge.label.clone());
        self.insert_reverse_adjacency(from_id, to_id, edge.label);
    }
}

pub struct IntoIter {
    inner: sled::Iter,
}

impl Iterator for IntoIter {
    type Item = Node;

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

        store.insert(Edge {
            from: a.clone(),
            to: b.clone(),
            label: String::new(),
        });
        store.insert(Edge {
            from: b.clone(),
            to: c.clone(),
            label: String::new(),
        });
        store.insert(Edge {
            from: c.clone(),
            to: a.clone(),
            label: String::new(),
        });
        store.insert(Edge {
            from: a.clone(),
            to: c.clone(),
            label: String::new(),
        });

        let nodes: Vec<Node> = store.nodes().collect();
        assert_eq!(nodes, vec![a.clone(), b.clone(), c.clone()]);
        assert_eq!(
            store.outgoing_edges(a.clone()),
            vec![
                Edge {
                    from: a.clone(),
                    to: b.clone(),
                    label: String::new()
                },
                Edge {
                    from: a.clone(),
                    to: c.clone(),
                    label: String::new()
                },
            ]
        );
        assert_eq!(
            store.outgoing_edges(b.clone()),
            vec![Edge {
                from: b.clone(),
                to: c.clone(),
                label: String::new()
            },]
        );
        assert_eq!(
            store.ingoing_edges(c.clone()),
            vec![
                Edge {
                    from: b.clone(),
                    to: c.clone(),
                    label: String::new()
                },
                Edge {
                    from: a.clone(),
                    to: c.clone(),
                    label: String::new()
                },
            ]
        );
        assert_eq!(
            store.ingoing_edges(a.clone()),
            vec![Edge {
                from: c,
                to: a.clone(),
                label: String::new()
            },]
        );
        assert_eq!(
            store.ingoing_edges(b.clone()),
            vec![Edge {
                from: a,
                to: b,
                label: String::new()
            },]
        );
    }
}
