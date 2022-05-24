mod memory_store;
mod sled_store;
use serde::{Deserialize, Serialize};
use std::cmp;
use std::collections::{BinaryHeap, HashMap};
use std::path::Path;

use sled_store::SledStore;

type NodeName = String;
type NodeID = u64;

#[derive(Debug, Serialize, Deserialize)]
struct InternalEdge {
    to_node: NodeID,
    label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Node {
    name: String,
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
    type Iter: Iterator<Item = Node>;

    fn outgoing_edges(&self, node: Node) -> Vec<Edge>;
    fn ingoing_edges(&self, node: Node) -> Vec<Edge>;
    fn nodes(&self) -> Self::Iter;
    fn insert(&mut self, edge: Edge);
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Edge {
    from: Node,
    to: Node,
    label: String,
}

impl Edge {
    pub fn new(from: Node, to: Node, label: String) -> Self {
        Edge { from, to, label }
    }
}

pub struct WebGraph<S: GraphStore> {
    internal_store: S,
}

impl<S: GraphStore> WebGraph<S> {
    pub fn new_memory() -> WebGraph<SledStore> {
        WebGraph {
            internal_store: SledStore::temporary(),
        }
    }

    pub fn open<P: AsRef<Path>>(path: P) -> WebGraph<SledStore> {
        WebGraph {
            internal_store: SledStore::open(path),
        }
    }

    pub fn insert(&mut self, edge: Edge) {
        self.internal_store.insert(edge)
    }

    fn dijkstra<F1, F2>(&self, source: Node, node_edges: F1, edge_node: F2) -> HashMap<Node, usize>
    where
        F1: Fn(Node) -> Vec<Edge>,
        F2: Fn(&Edge) -> Node,
    {
        let mut distances: HashMap<Node, usize> = HashMap::default();
        let mut queue = BinaryHeap::new();

        queue.push(cmp::Reverse((0_usize, source.clone())));
        distances.insert(source, 0);

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
        self.dijkstra(
            source,
            |node| self.internal_store.outgoing_edges(node),
            |edge| edge.to.clone(),
        )
    }

    pub fn reversed_distances(&self, source: Node) -> HashMap<Node, usize> {
        self.dijkstra(
            source,
            |node| self.internal_store.ingoing_edges(node),
            |edge| edge.from.clone(),
        )
    }

    pub fn harmonic_centrality(&self) -> HashMap<Node, f64> {
        let norm_factor = (self.internal_store.nodes().count() - 1) as f64;
        self.internal_store
            .nodes()
            .into_iter()
            .map(|node| {
                let mut centrality_values: HashMap<Node, f64> = self
                    .reversed_distances(node.clone())
                    .into_iter()
                    .filter(|(other_node, _)| other_node != &node)
                    .map(|(other_node, dist)| (other_node, 1f64 / dist as f64))
                    .collect();

                for other_node in self.internal_store.nodes() {
                    centrality_values.entry(other_node).or_insert(0f64);
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
}

#[cfg(test)]
mod test {
    use super::*;

    fn test_graph() -> WebGraph<SledStore> {
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

        let mut graph = WebGraph::<SledStore>::new_memory();

        graph.insert(Edge::new(Node::from("A"), Node::from("B"), String::new()));
        graph.insert(Edge::new(Node::from("B"), Node::from("C"), String::new()));
        graph.insert(Edge::new(Node::from("A"), Node::from("C"), String::new()));
        graph.insert(Edge::new(Node::from("C"), Node::from("A"), String::new()));
        graph.insert(Edge::new(Node::from("D"), Node::from("C"), String::new()));

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
}
