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
mod graph_store;

use serde::{Deserialize, Serialize};
use std::collections::{BinaryHeap, HashMap};
use std::path::Path;
use std::sync::Mutex;
use std::{cmp, fs};

use graph_store::GraphStore;

use crate::directory::{self, DirEntry};
use crate::webpage::Url;

use self::graph_store::Adjacency;
use crate::kv::rocksdb_store::RocksDbStore;

pub mod centrality;

type NodeID = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct StoredEdge {
    other: NodeID,
    label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Node {
    pub name: String,
}

impl Node {
    pub fn into_host(self) -> Node {
        let url = Url::from(self.name);

        let host = url.host_without_specific_subdomains_and_query();

        Node {
            name: host.to_lowercase(),
        }
    }

    pub fn from_url(url: &Url) -> Self {
        Node::from(url.full())
    }
}

impl From<String> for Node {
    fn from(name: String) -> Self {
        Self {
            name: name.to_lowercase(),
        }
    }
}

impl From<&Url> for Node {
    fn from(url: &Url) -> Self {
        Self {
            name: url.without_protocol().to_lowercase(),
        }
    }
}

impl From<&str> for Node {
    fn from(name: &str) -> Self {
        Self::from(name.to_lowercase())
    }
}

impl From<Url> for Node {
    fn from(url: Url) -> Self {
        Self::from(&url)
    }
}

pub struct EdgeIterator<'a> {
    current_block_idx: usize,
    blocks: Vec<u64>,
    adjacency: &'a Mutex<Adjacency>,
    current_block: Option<Box<dyn Iterator<Item = Edge>>>,
}

impl<'a> EdgeIterator<'a> {
    fn new(adjacency: &'a Mutex<Adjacency>) -> EdgeIterator<'a> {
        let blocks: Vec<_> = adjacency
            .lock()
            .unwrap()
            .tree
            .inner
            .store
            .iter()
            .map(|(key, _)| key)
            .collect();

        EdgeIterator {
            current_block_idx: 0,
            blocks,
            adjacency,
            current_block: None,
        }
    }

    fn load_next_block(&mut self) {
        if self.current_block_idx < self.blocks.len() {
            let block_id = self.blocks[self.current_block_idx];
            let block = self
                .adjacency
                .lock()
                .unwrap()
                .tree
                .inner
                .get(&block_id)
                .unwrap()
                .clone();

            self.current_block = Some(Box::new(block.into_iter().flat_map(|(node_id, edges)| {
                edges.into_iter().map(move |edge| Edge {
                    from: node_id,
                    to: edge.other,
                    label: edge.label,
                })
            })));

            self.current_block_idx += 1;
        }
    }
}

impl<'a> Iterator for EdgeIterator<'a> {
    type Item = Edge;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(res) = self.current_block.as_mut().and_then(|it| it.next()) {
            return Some(res);
        }

        if self.current_block.is_none() {
            self.load_next_block();
        }

        self.current_block.as_mut().and_then(|it| it.next())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edge {
    from: NodeID,
    to: NodeID,
    label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FullEdge {
    pub from: Node,
    pub to: Node,
    pub label: String,
}

pub struct WebgraphBuilder {
    path: Box<Path>,
    full_graph_path: Option<Box<Path>>,
    host_graph_path: Option<Box<Path>>,
    read_only: bool,
}

impl WebgraphBuilder {
    #[cfg(test)]
    pub fn new_memory() -> Self {
        use crate::gen_temp_path;

        let path = gen_temp_path();
        Self::new(path)
    }

    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().into(),
            full_graph_path: None,
            host_graph_path: None,
            read_only: false,
        }
    }

    pub fn with_full_graph(mut self) -> Self {
        self.full_graph_path = Some(self.path.join("full").into());
        self
    }

    pub fn with_host_graph(mut self) -> Self {
        self.host_graph_path = Some(self.path.join("host").into());
        self
    }

    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;

        self
    }

    pub fn open(self) -> Webgraph {
        if self.read_only {
            Webgraph {
                full: self.full_graph_path.map(GraphStore::open_read_only),
                host: self.host_graph_path.map(GraphStore::open_read_only),
                path: self.path.to_str().unwrap().to_string(),
            }
        } else {
            Webgraph {
                full: self.full_graph_path.map(GraphStore::open),
                host: self.host_graph_path.map(GraphStore::open),
                path: self.path.to_str().unwrap().to_string(),
            }
        }
    }
}

pub trait Store
where
    Self: Sized,
{
    fn open<P: AsRef<Path>>(path: P) -> GraphStore<Self>;
    fn open_read_only<P: AsRef<Path>>(path: P) -> GraphStore<Self>;

    fn temporary() -> GraphStore<Self> {
        Self::open(crate::gen_temp_path())
    }
}

pub struct Webgraph<S: Store = RocksDbStore> {
    pub path: String,
    pub full: Option<GraphStore<S>>,
    pub host: Option<GraphStore<S>>,
}

impl<S: Store> Webgraph<S> {
    pub fn insert(&mut self, from: Node, to: Node, label: String) {
        if let Some(full_graph) = &mut self.full {
            full_graph.insert(from.clone(), to.clone(), label.clone());
        }

        if let Some(host_graph) = &mut self.host {
            host_graph.insert(from.into_host(), to.into_host(), label);
        }
    }

    pub fn merge(&mut self, other: Webgraph<S>) {
        match (&mut self.full, other.full) {
            (Some(self_graph), Some(other_graph)) => self_graph.append(other_graph),
            (None, Some(other_graph)) => self.full = Some(other_graph),
            (Some(_), None) | (None, None) => {}
        }

        match (&mut self.host, other.host) {
            (Some(self_graph), Some(other_graph)) => self_graph.append(other_graph),
            (None, Some(other_graph)) => self.host = Some(other_graph),
            (Some(_), None) | (None, None) => {}
        }

        self.flush();
    }

    fn dijkstra<F1, F2>(
        source: Node,
        node_edges: F1,
        edge_node: F2,
        store: &GraphStore<S>,
    ) -> HashMap<NodeID, usize>
    where
        F1: Fn(NodeID) -> Vec<Edge>,
        F2: Fn(&Edge) -> NodeID,
    {
        let source_id = store.node2id(&source);
        if source_id.is_none() {
            return HashMap::new();
        }

        let source_id = source_id.unwrap();
        let mut distances: HashMap<NodeID, usize> = HashMap::default();

        let mut queue = BinaryHeap::new();

        queue.push(cmp::Reverse((0_usize, source_id)));
        distances.insert(source_id, 0);

        while let Some(state) = queue.pop() {
            let (cost, v) = state.0;
            let current_dist = distances.get(&v).unwrap_or(&usize::MAX);

            if cost > *current_dist {
                continue;
            }

            for edge in node_edges(v) {
                if cost + 1 < *distances.get(&edge_node(&edge)).unwrap_or(&usize::MAX) {
                    let next = cmp::Reverse((cost + 1, edge_node(&edge)));
                    queue.push(next);
                    distances.insert(edge_node(&edge), cost + 1);
                }
            }
        }

        distances
    }

    #[allow(unused)]
    pub fn distances(&self, source: Node) -> HashMap<Node, usize> {
        self.full
            .as_ref()
            .map(|full_graph| {
                let distances = Webgraph::dijkstra(
                    source,
                    |node_id| full_graph.outgoing_edges(node_id),
                    |edge| edge.to,
                    full_graph,
                );

                distances
                    .into_iter()
                    .map(|(id, dist)| (full_graph.id2node(&id).expect("unknown node"), dist))
                    .collect()
            })
            .unwrap_or_default()
    }

    #[allow(unused)]
    fn raw_reversed_distances(&self, source: Node) -> HashMap<NodeID, usize> {
        self.full
            .as_ref()
            .map(|full_graph| {
                Webgraph::dijkstra(
                    source,
                    |node| full_graph.ingoing_edges(node),
                    |edge| edge.from,
                    full_graph,
                )
            })
            .unwrap_or_default()
    }

    #[allow(unused)]
    pub fn reversed_distances(&self, source: Node) -> HashMap<Node, usize> {
        self.full
            .as_ref()
            .map(|full_graph| {
                self.raw_reversed_distances(source)
                    .into_iter()
                    .map(|(id, dist)| (full_graph.id2node(&id).expect("unknown node"), dist))
                    .collect()
            })
            .unwrap_or_default()
    }

    #[allow(unused)]
    pub fn host_distances(&self, source: Node) -> HashMap<Node, usize> {
        self.host
            .as_ref()
            .map(|host_graph| {
                self.raw_host_distances(source)
                    .into_iter()
                    .map(|(id, dist)| (host_graph.id2node(&id).expect("unknown node"), dist))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn raw_host_distances(&self, source: Node) -> HashMap<NodeID, usize> {
        self.host
            .as_ref()
            .map(|host_graph| {
                Webgraph::dijkstra(
                    source,
                    |node| host_graph.outgoing_edges(node),
                    |edge| edge.to,
                    host_graph,
                )
            })
            .unwrap_or_default()
    }

    pub fn raw_host_reversed_distances(&self, source: Node) -> HashMap<NodeID, usize> {
        self.host
            .as_ref()
            .map(|host_graph| {
                Webgraph::dijkstra(
                    source,
                    |node| host_graph.ingoing_edges(node),
                    |edge| edge.from,
                    host_graph,
                )
            })
            .unwrap_or_default()
    }

    #[allow(unused)]
    pub fn host_reversed_distances(&self, source: Node) -> HashMap<Node, usize> {
        self.host
            .as_ref()
            .map(|host_graph| {
                self.raw_host_reversed_distances(source)
                    .into_iter()
                    .map(|(id, dist)| (host_graph.id2node(&id).expect("unknown node"), dist))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn flush(&self) {
        if let Some(full_graph) = &self.full {
            full_graph.flush();
        }
        if let Some(host_graph) = &self.host {
            host_graph.flush();
        }
    }

    pub fn ingoing_edges(&self, node: Node) -> Vec<FullEdge> {
        if let Some(graph) = &self.full {
            if let Some(node_id) = graph.node2id(&node) {
                graph
                    .ingoing_edges(node_id)
                    .into_iter()
                    .map(|edge| FullEdge {
                        from: graph.id2node(&edge.from).unwrap(),
                        to: graph.id2node(&edge.to).unwrap(),
                        label: edge.label,
                    })
                    .collect()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        }
    }
}

impl From<FrozenWebgraph> for Webgraph {
    fn from(frozen: FrozenWebgraph) -> Self {
        let path = match &frozen.root {
            DirEntry::Folder { name, entries: _ } => name.clone(),
            DirEntry::File {
                name: _,
                content: _,
            } => {
                panic!("Cannot open webgraph from a file - must be directory.")
            }
        };

        if Path::new(&path).exists() {
            fs::remove_dir_all(&path).unwrap();
        }

        directory::recreate_folder(&frozen.root).unwrap();

        let mut builder = WebgraphBuilder::new(path);

        if frozen.has_full {
            builder = builder.with_full_graph();
        }

        if frozen.has_host {
            builder = builder.with_host_graph();
        }

        builder.open()
    }
}

impl From<Webgraph> for FrozenWebgraph {
    fn from(graph: Webgraph) -> Self {
        graph.flush();
        let path = graph.path.clone();
        let has_full = graph.full.is_some();
        let has_host = graph.host.is_some();
        drop(graph);
        let root = directory::scan_folder(path).unwrap();

        Self {
            root,
            has_full,
            has_host,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FrozenWebgraph {
    pub root: DirEntry,
    has_full: bool,
    has_host: bool,
}

#[cfg(test)]
mod test {
    use super::*;

    fn test_graph() -> Webgraph {
        //     ┌────┐
        //     │    │
        // ┌───A◄─┐ │
        // │      │ │
        // ▼      │ │
        // B─────►C◄┘
        //        ▲
        //        │
        //        │
        //        D

        let mut graph = WebgraphBuilder::new_memory()
            .with_full_graph()
            .with_host_graph()
            .open();

        graph.insert(Node::from("A"), Node::from("B"), String::new());
        graph.insert(Node::from("B"), Node::from("C"), String::new());
        graph.insert(Node::from("A"), Node::from("C"), String::new());
        graph.insert(Node::from("C"), Node::from("A"), String::new());
        graph.insert(Node::from("D"), Node::from("C"), String::new());

        graph.flush();

        graph
    }

    #[test]
    fn distance_calculation() {
        let graph = test_graph();

        let distances = graph.distances(Node::from("D"));

        assert_eq!(distances.get(&Node::from("C")), Some(&1));
        assert_eq!(distances.get(&Node::from("A")), Some(&2));
        assert_eq!(distances.get(&Node::from("B")), Some(&3));
    }

    #[test]
    fn nonexisting_node() {
        let graph = test_graph();
        assert_eq!(graph.distances(Node::from("E")).len(), 0);
        assert_eq!(graph.reversed_distances(Node::from("E")).len(), 0);
    }

    #[test]
    fn reversed_distance_calculation() {
        let graph = test_graph();

        let distances = graph.reversed_distances(Node::from("D"));

        assert_eq!(distances.get(&Node::from("C")), None);
        assert_eq!(distances.get(&Node::from("A")), None);
        assert_eq!(distances.get(&Node::from("B")), None);

        let distances = graph.reversed_distances(Node::from("A"));

        assert_eq!(distances.get(&Node::from("C")), Some(&1));
        assert_eq!(distances.get(&Node::from("D")), Some(&2));
        assert_eq!(distances.get(&Node::from("B")), Some(&2));
    }

    #[test]
    fn merge() {
        let mut graph1 = WebgraphBuilder::new_memory()
            .with_full_graph()
            .with_host_graph()
            .open();

        graph1.insert(Node::from("A"), Node::from("B"), String::new());

        let mut graph2 = WebgraphBuilder::new_memory()
            .with_full_graph()
            .with_host_graph()
            .open();
        graph2.insert(Node::from("B"), Node::from("C"), String::new());

        graph1.merge(graph2);

        assert_eq!(
            graph1.distances(Node::from("A")).get(&Node::from("C")),
            Some(&2)
        )
    }

    #[test]
    fn serialize_deserialize_bincode() {
        let graph = test_graph();
        let path = graph.path.clone();
        let frozen: FrozenWebgraph = graph.into();
        let bytes = bincode::serialize(&frozen).unwrap();

        std::fs::remove_dir_all(path).unwrap();

        let deserialized_frozen: FrozenWebgraph = bincode::deserialize(&bytes).unwrap();
        let graph: Webgraph = deserialized_frozen.into();

        let distances = graph.distances(Node::from("D"));

        assert_eq!(distances.get(&Node::from("C")), Some(&1));
        assert_eq!(distances.get(&Node::from("A")), Some(&2));
        assert_eq!(distances.get(&Node::from("B")), Some(&3));
    }

    #[test]
    fn node_lowercase_name() {
        let n = Node::from("TEST".to_string());
        assert_eq!(&n.name, "test");
    }

    #[test]
    fn host_node_cleanup() {
        let n = Node::from("https://www.example.com?test").into_host();
        assert_eq!(&n.name, "example.com");
    }
}
