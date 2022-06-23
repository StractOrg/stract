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
pub mod sled_store;

use indicatif::{ProgressBar, ProgressIterator, ProgressStyle};
use serde::{Deserialize, Serialize};
use std::cmp;
use std::collections::{BinaryHeap, HashMap};
use std::path::Path;

pub use sled_store::SledStore;

use crate::directory::{self, DirEntry};
use crate::webpage;

type NodeID = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StoredEdge {
    other: NodeID,
    label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Node {
    pub name: String,
}

impl Node {
    fn into_host(self) -> Node {
        Node {
            name: webpage::host(&self.name).to_string(),
        }
    }
}

impl From<String> for Node {
    fn from(name: String) -> Self {
        Self { name }
    }
}

impl From<&str> for Node {
    fn from(name: &str) -> Self {
        Self::from(name.to_string())
    }
}

pub trait GraphStore {
    fn node2id(&self, node: &Node) -> Option<NodeID>;
    fn id2node(&self, id: &NodeID) -> Option<Node>;
    fn outgoing_edges(&self, node: NodeID) -> Vec<Edge>;
    fn ingoing_edges(&self, node: NodeID) -> Vec<Edge>;
    fn nodes(&self) -> NodeIterator;
    fn insert(&mut self, from: Node, to: Node, label: String);
    fn flush(&self);

    fn edges(&self) -> EdgeIterator<'_> {
        EdgeIterator::from(self.nodes().flat_map(|node| self.outgoing_edges(node)))
    }

    fn append<S: GraphStore>(&mut self, other: S) {
        for edge in other.edges() {
            let from = other.id2node(&edge.from).expect("node not found");
            let to = other.id2node(&edge.to).expect("node not found");

            self.insert(from, to, edge.label);
        }
    }
}

pub struct EdgeIterator<'a> {
    inner: Box<dyn Iterator<Item = Edge> + 'a>,
}

impl<'a> EdgeIterator<'a> {
    fn from<T: 'a + Iterator<Item = Edge>>(iterator: T) -> EdgeIterator<'a> {
        EdgeIterator {
            inner: Box::new(iterator),
        }
    }
}

impl<'a> Iterator for EdgeIterator<'a> {
    type Item = Edge;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub struct NodeIterator<'a> {
    inner: Box<dyn Iterator<Item = NodeID> + 'a + Send>,
}

impl<'a> NodeIterator<'a> {
    fn from<T: 'a + Iterator<Item = NodeID> + Send>(iterator: T) -> NodeIterator<'a> {
        NodeIterator {
            inner: Box::new(iterator),
        }
    }
}

impl<'a> Iterator for NodeIterator<'a> {
    type Item = NodeID;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edge {
    from: NodeID,
    to: NodeID,
    label: String,
}

pub struct WebgraphBuilder {
    path: Box<Path>,
    full_graph_path: Option<Box<Path>>,
    host_graph_path: Option<Box<Path>>,
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

    pub fn open(self) -> Webgraph {
        Webgraph {
            full_graph: self.full_graph_path.map(SledStore::open),
            host_graph: self.host_graph_path.map(SledStore::open),
            path: self.path.to_str().unwrap().to_string(),
        }
    }
}

pub struct Webgraph<S: GraphStore = SledStore> {
    pub path: String,
    full_graph: Option<S>,
    host_graph: Option<S>,
}

impl<S: GraphStore> Webgraph<S> {
    pub fn insert(&mut self, from: Node, to: Node, label: String) {
        if let Some(full_graph) = &mut self.full_graph {
            full_graph.insert(from.clone(), to.clone(), label.clone());
        }

        if let Some(host_graph) = &mut self.host_graph {
            host_graph.insert(from.into_host(), to.into_host(), label);
        }
    }

    pub fn merge(&mut self, other: Webgraph<S>) {
        match (&mut self.full_graph, other.full_graph) {
            (Some(self_graph), Some(other_graph)) => self_graph.append(other_graph),
            (None, Some(other_graph)) => self.full_graph = Some(other_graph),
            (Some(_), None) | (None, None) => {}
        }

        match (&mut self.host_graph, other.host_graph) {
            (Some(self_graph), Some(other_graph)) => self_graph.append(other_graph),
            (None, Some(other_graph)) => self.host_graph = Some(other_graph),
            (Some(_), None) | (None, None) => {}
        }
    }

    fn dijkstra<F1, F2>(
        source: Node,
        node_edges: F1,
        edge_node: F2,
        store: &S,
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

    pub fn distances(&self, source: Node) -> HashMap<Node, usize> {
        self.full_graph
            .as_ref()
            .map(|full_graph| {
                let distances = Webgraph::<S>::dijkstra(
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

    fn raw_reversed_distances(&self, source: Node) -> HashMap<NodeID, usize> {
        self.full_graph
            .as_ref()
            .map(|full_graph| {
                Webgraph::<S>::dijkstra(
                    source,
                    |node| full_graph.ingoing_edges(node),
                    |edge| edge.from,
                    full_graph,
                )
            })
            .unwrap_or_default()
    }

    pub fn reversed_distances(&self, source: Node) -> HashMap<Node, usize> {
        self.full_graph
            .as_ref()
            .map(|full_graph| {
                self.raw_reversed_distances(source)
                    .into_iter()
                    .map(|(id, dist)| (full_graph.id2node(&id).expect("unknown node"), dist))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn host_distances(&self, source: Node) -> HashMap<Node, usize> {
        self.host_graph
            .as_ref()
            .map(|host_graph| {
                let distances = Webgraph::<S>::dijkstra(
                    source,
                    |node| host_graph.outgoing_edges(node),
                    |edge| edge.to,
                    host_graph,
                );

                distances
                    .into_iter()
                    .map(|(id, dist)| (host_graph.id2node(&id).expect("unknown node"), dist))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn raw_host_reversed_distances(&self, source: Node) -> HashMap<NodeID, usize> {
        self.host_graph
            .as_ref()
            .map(|host_graph| {
                Webgraph::<S>::dijkstra(
                    source,
                    |node| host_graph.ingoing_edges(node),
                    |edge| edge.from,
                    host_graph,
                )
            })
            .unwrap_or_default()
    }

    pub fn host_reversed_distances(&self, source: Node) -> HashMap<Node, usize> {
        self.host_graph
            .as_ref()
            .map(|host_graph| {
                self.raw_host_reversed_distances(source)
                    .into_iter()
                    .map(|(id, dist)| (host_graph.id2node(&id).expect("unknown node"), dist))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn calculate_centrality<F>(graph: &S, node_distances: F) -> HashMap<Node, f64>
    where
        F: Fn(Node) -> HashMap<NodeID, usize>,
    {
        let nodes: Vec<_> = graph.nodes().collect();
        let pb = ProgressBar::new(nodes.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{wide_bar}] {pos:>7}/{len:7} ({eta})",
                )
                .progress_chars("#>-"),
        );
        let norm_factor = (nodes.len() - 1) as f64;
        nodes
            .iter()
            .progress_with(pb)
            .map(|node_id| {
                let node = graph.id2node(node_id).expect("unknown node");
                let centrality_values: HashMap<NodeID, f64> = node_distances(node.clone())
                    .into_iter()
                    .filter(|(other_id, _)| *other_id != *node_id)
                    .map(|(other_node, dist)| (other_node, 1f64 / dist as f64))
                    .collect();

                let centrality = centrality_values
                    .into_iter()
                    .map(|(_, val)| val)
                    .sum::<f64>()
                    / norm_factor;

                (node, centrality)
            })
            .collect()
    }

    pub fn harmonic_centrality(&self) -> HashMap<Node, f64> {
        self.full_graph
            .as_ref()
            .map(|full_graph| {
                Webgraph::<S>::calculate_centrality(full_graph, |node| {
                    self.raw_reversed_distances(node)
                })
            })
            .unwrap_or_default()
    }

    pub fn host_harmonic_centrality(&self) -> HashMap<Node, f64> {
        self.host_graph
            .as_ref()
            .map(|host_graph| {
                Webgraph::<S>::calculate_centrality(host_graph, |node| {
                    self.raw_host_reversed_distances(node)
                })
            })
            .unwrap_or_default()
    }

    pub fn flush(&self) {
        if let Some(full_graph) = &self.full_graph {
            full_graph.flush();
        }
        if let Some(host_graph) = &self.host_graph {
            host_graph.flush();
        }
    }
}

impl From<FrozenWebgraph> for Webgraph {
    fn from(frozen: FrozenWebgraph) -> Self {
        directory::recreate_folder(&frozen.root).unwrap();

        match frozen.root {
            DirEntry::Folder { name, entries: _ } => {
                let mut builder = WebgraphBuilder::new(name);

                if frozen.has_full {
                    builder = builder.with_full_graph();
                }

                if frozen.has_host {
                    builder = builder.with_host_graph();
                }

                builder.open()
            }
            DirEntry::File {
                name: _,
                content: _,
            } => {
                panic!("Cannot open webgraph from a file - must be directory.")
            }
        }
    }
}

impl From<Webgraph> for FrozenWebgraph {
    fn from(graph: Webgraph) -> Self {
        graph.flush();
        let path = graph.path.clone();
        let has_full = graph.full_graph.is_some();
        let has_host = graph.host_graph.is_some();
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
    fn harmonic_centrality() {
        let graph = test_graph();

        let centrality = graph.harmonic_centrality();

        assert_eq!(centrality.get(&Node::from("C")).unwrap(), &1.0);
        assert_eq!(centrality.get(&Node::from("D")).unwrap(), &0.0);
        assert_eq!(
            (*centrality.get(&Node::from("A")).unwrap() * 100.0).round() / 100.0,
            0.67
        );
        assert_eq!(
            (*centrality.get(&Node::from("B")).unwrap() * 100.0).round() / 100.0,
            0.61
        );
    }

    #[test]
    fn host_harmonic_centrality() {
        let mut graph = WebgraphBuilder::new_memory()
            .with_full_graph()
            .with_host_graph()
            .open();

        graph.insert(Node::from("A.com/1"), Node::from("A.com/2"), String::new());
        graph.insert(Node::from("A.com/1"), Node::from("A.com/3"), String::new());
        graph.insert(Node::from("A.com/1"), Node::from("A.com/4"), String::new());
        graph.insert(Node::from("A.com/2"), Node::from("A.com/1"), String::new());
        graph.insert(Node::from("A.com/2"), Node::from("A.com/3"), String::new());
        graph.insert(Node::from("A.com/2"), Node::from("A.com/4"), String::new());
        graph.insert(Node::from("A.com/3"), Node::from("A.com/1"), String::new());
        graph.insert(Node::from("A.com/3"), Node::from("A.com/2"), String::new());
        graph.insert(Node::from("A.com/3"), Node::from("A.com/4"), String::new());
        graph.insert(Node::from("A.com/4"), Node::from("A.com/1"), String::new());
        graph.insert(Node::from("A.com/4"), Node::from("A.com/2"), String::new());
        graph.insert(Node::from("A.com/4"), Node::from("A.com/3"), String::new());
        graph.insert(Node::from("C.com"), Node::from("B.com"), String::new());
        graph.insert(Node::from("D.com"), Node::from("B.com"), String::new());

        let centrality = graph.harmonic_centrality();
        assert!(
            centrality.get(&Node::from("A.com/1")).unwrap()
                > centrality.get(&Node::from("B.com")).unwrap()
        );

        let host_centrality = graph.host_harmonic_centrality();
        assert!(
            host_centrality.get(&Node::from("B.com")).unwrap()
                > host_centrality.get(&Node::from("A.com")).unwrap()
        );
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
}
