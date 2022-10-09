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

//! this is an implementation of the algorithm
//! described in "A Faster Algorithm for Betweenness Centrality"

use std::collections::{HashMap, VecDeque};

use crate::webgraph::{graph_store::GraphStore, Node, NodeID, Store, Webgraph};

fn calculate<S: Store>(store: &GraphStore<S>) -> HashMap<Node, f64> {
    let mut centrality: HashMap<NodeID, f64> = HashMap::new();
    let mut n = 0;

    for s in store.nodes() {
        n += 1;
        centrality.entry(s).or_default();

        let mut stack = Vec::new();
        let mut predecessors: HashMap<u64, Vec<u64>> = HashMap::new();

        let mut sigma = HashMap::new();
        sigma.insert(s, 1);

        let mut distances = HashMap::new();
        distances.insert(s, 0);

        let mut q = VecDeque::new();
        q.push_back(s);

        while let Some(v) = q.pop_front() {
            stack.push(v);
            for edge in store.outgoing_edges(v) {
                let w = edge.to;
                let dist_v = *distances.get(&v).unwrap();
                distances.entry(w).or_insert_with(|| {
                    q.push_back(w);
                    dist_v + 1
                });

                if *distances.get(&w).unwrap() == *distances.get(&v).unwrap() + 1 {
                    let sigma_v = *sigma.get(&v).unwrap_or(&0);
                    *sigma.entry(w).or_insert(0) += sigma_v;
                    predecessors.entry(w).or_default().push(v);
                }
            }
        }

        let mut delta = HashMap::new();
        while let Some(w) = stack.pop() {
            if let Some(pred) = predecessors.get(&w) {
                for v in pred {
                    *delta.entry(v).or_default() += (*sigma.get(v).unwrap() as f64
                        / *sigma.get(&w).unwrap() as f64)
                        * (1.0 + delta.get(&w).unwrap_or(&0.0));
                }
            }

            if w != s {
                *centrality.entry(w).or_insert(0.0) += *delta.get(&w).unwrap_or(&0.0);
            }
        }
    }

    let n = n as f64;
    let norm = n * (n - 1.0);

    centrality
        .into_iter()
        .map(|(id, centrality)| (store.id2node(&id).unwrap(), centrality / norm))
        .collect()
}

#[derive(Debug)]
pub struct Betweenness {
    pub full: HashMap<Node, f64>,
    pub host: HashMap<Node, f64>,
}

impl Betweenness {
    pub fn calculate(graph: &Webgraph) -> Self {
        Self {
            full: graph.full.as_ref().map(calculate).unwrap_or_default(),
            host: graph.host.as_ref().map(calculate).unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use maplit::hashmap;

    use crate::webgraph::WebgraphBuilder;

    use super::*;

    fn create_path_graph(n: usize) -> Webgraph {
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

        for i in 0..n - 1 {
            graph.insert(
                Node::from(i.to_string()),
                Node::from((i + 1).to_string()),
                String::new(),
            );
        }

        graph.flush();

        graph
    }

    #[test]
    fn path() {
        let p = create_path_graph(5);
        let centrality = Betweenness::calculate(&p);

        assert_eq!(
            centrality.host,
            hashmap! {
                Node::from("0".to_string()) => 0.0,
                Node::from("1".to_string()) => 0.15,
                Node::from("2".to_string()) => 0.2,
                Node::from("3".to_string()) => 0.15,
                Node::from("4".to_string()) => 0.0,
            }
        );
    }
}
