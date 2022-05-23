mod memory_store;
mod sled_store;
use std::cmp;
use std::collections::{BinaryHeap, HashMap};

use itertools::TakeWhileRef;
use memory_store::MemoryStore;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub fn new_memory() -> WebGraph<MemoryStore> {
        WebGraph {
            internal_store: MemoryStore::default(),
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

        queue.push(cmp::Reverse((0 as usize, source.clone())));
        distances.insert(source.clone(), 0);

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
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn distance_calculation() {
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

        let mut graph = WebGraph::<MemoryStore>::new_memory();

        graph.insert(Edge::new(Node::from("A"), Node::from("B"), String::new()));
        graph.insert(Edge::new(Node::from("B"), Node::from("C"), String::new()));
        graph.insert(Edge::new(Node::from("A"), Node::from("C"), String::new()));
        graph.insert(Edge::new(Node::from("C"), Node::from("A"), String::new()));
        graph.insert(Edge::new(Node::from("D"), Node::from("C"), String::new()));

        let distances = graph.distances(Node::from("D"));

        assert_eq!(distances.get(&Node::from("C")), Some(&1));
        assert_eq!(distances.get(&Node::from("A")), Some(&2));
        assert_eq!(distances.get(&Node::from("B")), Some(&3));
    }

    #[test]
    fn reversed_distance_calculation() {
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

        let mut graph = WebGraph::<MemoryStore>::new_memory();

        graph.insert(Edge::new(Node::from("A"), Node::from("B"), String::new()));
        graph.insert(Edge::new(Node::from("B"), Node::from("C"), String::new()));
        graph.insert(Edge::new(Node::from("A"), Node::from("C"), String::new()));
        graph.insert(Edge::new(Node::from("C"), Node::from("A"), String::new()));
        graph.insert(Edge::new(Node::from("D"), Node::from("C"), String::new()));

        let distances = graph.reversed_distances(Node::from("D"));

        assert_eq!(distances.get(&Node::from("C")), None);
        assert_eq!(distances.get(&Node::from("A")), None);
        assert_eq!(distances.get(&Node::from("B")), None);

        let distances = graph.reversed_distances(Node::from("A"));

        assert_eq!(distances.get(&Node::from("C")), Some(&1));
        assert_eq!(distances.get(&Node::from("D")), Some(&2));
        assert_eq!(distances.get(&Node::from("B")), Some(&2));
    }
}
