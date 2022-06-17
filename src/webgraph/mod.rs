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

use serde::{Deserialize, Serialize};
use std::cmp;
use std::collections::{BinaryHeap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

pub use sled_store::SledStore;

use crate::directory::{self, DirEntry};
use crate::webpage;

type NodeID = u64;

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StoredEdge {
    other: NodeID,
    label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Node {
    name: String,
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
    fn nodes<'a>(&'a self) -> NodeIterator;
    fn insert(&mut self, from: Node, to: Node, label: String);
    fn flush(&self);

    fn edges<'a>(&'a self) -> EdgeIterator<'a> {
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
    inner: Box<dyn Iterator<Item = NodeID> + 'a>,
}

impl<'a> NodeIterator<'a> {
    fn from<T: 'a + Iterator<Item = NodeID>>(iterator: T) -> NodeIterator<'a> {
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

pub struct Webgraph<S: GraphStore = SledStore> {
    path: String,
    full_graph: S,
    host_graph: S,
}

impl<S: GraphStore> Webgraph<S> {
    #[cfg(test)]
    pub fn new_memory() -> Webgraph {
        let path = gen_temp_path();
        Self::open(path)
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Webgraph {
        Webgraph {
            full_graph: SledStore::open(path.as_ref().join("full")),
            host_graph: SledStore::open(path.as_ref().join("host")),
            path: path.as_ref().to_str().unwrap().to_string(),
        }
    }

    pub fn insert(&mut self, from: Node, to: Node, label: String) {
        self.full_graph
            .insert(from.clone(), to.clone(), label.clone());

        self.host_graph
            .insert(from.into_host(), to.into_host(), label);
    }

    pub fn merge(&mut self, other: Webgraph<S>) {
        self.full_graph.append(other.full_graph);
        self.host_graph.append(other.host_graph);
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
        let distances = Webgraph::<S>::dijkstra(
            source,
            |node_id| self.full_graph.outgoing_edges(node_id),
            |edge| edge.to.clone(),
            &self.full_graph,
        );

        distances
            .into_iter()
            .map(|(id, dist)| (self.full_graph.id2node(&id).expect("unknown node"), dist))
            .collect()
    }

    fn raw_reversed_distances(&self, source: Node) -> HashMap<NodeID, usize> {
        Webgraph::<S>::dijkstra(
            source,
            |node| self.full_graph.ingoing_edges(node),
            |edge| edge.from.clone(),
            &self.full_graph,
        )
    }

    pub fn reversed_distances(&self, source: Node) -> HashMap<Node, usize> {
        self.raw_reversed_distances(source)
            .into_iter()
            .map(|(id, dist)| (self.full_graph.id2node(&id).expect("unknown node"), dist))
            .collect()
    }

    pub fn host_distances(&self, source: Node) -> HashMap<Node, usize> {
        let distances = Webgraph::<S>::dijkstra(
            source,
            |node| self.host_graph.outgoing_edges(node),
            |edge| edge.to.clone(),
            &self.host_graph,
        );

        distances
            .into_iter()
            .map(|(id, dist)| (self.host_graph.id2node(&id).expect("unknown node"), dist))
            .collect()
    }

    fn raw_host_reversed_distances(&self, source: Node) -> HashMap<NodeID, usize> {
        Webgraph::<S>::dijkstra(
            source,
            |node| self.host_graph.ingoing_edges(node),
            |edge| edge.from.clone(),
            &self.host_graph,
        )
    }

    pub fn host_reversed_distances(&self, source: Node) -> HashMap<Node, usize> {
        self.raw_host_reversed_distances(source)
            .into_iter()
            .map(|(id, dist)| (self.host_graph.id2node(&id).expect("unknown node"), dist))
            .collect()
    }

    fn calculate_centrality<F>(graph: &S, node_distances: F) -> HashMap<Node, f64>
    where
        F: Fn(Node) -> HashMap<NodeID, usize>,
    {
        let norm_factor = (graph.nodes().count() - 1) as f64;
        graph
            .nodes()
            .into_iter()
            .map(|node_id| {
                let node = graph.id2node(&node_id).expect("unknown node");
                let mut centrality_values: HashMap<NodeID, f64> = node_distances(node.clone())
                    .into_iter()
                    .filter(|(other_id, _)| *other_id != node_id)
                    .map(|(other_node, dist)| (other_node, 1f64 / dist as f64))
                    .collect();

                for other_id in graph.nodes() {
                    centrality_values.entry(other_id).or_insert(0f64);
                }

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
        Webgraph::<S>::calculate_centrality(&self.full_graph, |node| {
            self.raw_reversed_distances(node)
        })
    }

    pub fn host_harmonic_centrality(&self) -> HashMap<Node, f64> {
        Webgraph::<S>::calculate_centrality(&self.host_graph, |node| {
            self.raw_host_reversed_distances(node)
        })
    }

    pub fn flush(&self) {
        self.full_graph.flush();
        self.host_graph.flush();
    }
}

impl From<FrozenWebgraph> for Webgraph<SledStore> {
    fn from(frozen: FrozenWebgraph) -> Self {
        directory::recreate_folder(&frozen.root).unwrap();
        match frozen.root {
            DirEntry::Folder { name, entries: _ } => Webgraph::<SledStore>::open(name),
            DirEntry::File { name, content: _ } => Webgraph::<SledStore>::open(name),
        }
    }
}

impl From<Webgraph> for FrozenWebgraph {
    fn from(graph: Webgraph) -> Self {
        graph.flush();
        let path = graph.path.clone();
        drop(graph);
        let root = directory::scan_folder(path).unwrap();

        Self { root }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FrozenWebgraph {
    root: DirEntry,
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

        let mut graph = Webgraph::<SledStore>::new_memory();

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
        let mut graph = Webgraph::<SledStore>::new_memory();

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
        let mut graph1 = Webgraph::<SledStore>::new_memory();

        graph1.insert(Node::from("A"), Node::from("B"), String::new());

        let mut graph2 = Webgraph::<SledStore>::new_memory();
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
