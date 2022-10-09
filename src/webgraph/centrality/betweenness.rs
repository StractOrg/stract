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
        let mut predecessors: Vec<Vec<u64>> = Vec::new();

        let mut sigma = Vec::new();

        while s as usize >= sigma.len() {
            sigma.push(0);
        }

        sigma[s as usize] = 1;

        let mut distances = Vec::new();
        while s as usize >= distances.len() {
            distances.push(-1);
        }
        distances[s as usize] = 0;

        let mut q = VecDeque::new();
        q.push_back(s);

        while let Some(v) = q.pop_front() {
            stack.push(v);
            for edge in store.outgoing_edges(v) {
                let w = edge.to;

                while w as usize >= distances.len() {
                    distances.push(-1);
                }

                if distances[w as usize] == -1 {
                    let dist_v = distances[v as usize];
                    q.push_back(w);
                    distances[w as usize] = dist_v + 1;
                }

                if distances[w as usize] == distances[v as usize] + 1 {
                    let sigma_v = *sigma.get(v as usize).unwrap_or(&0);

                    while w as usize >= sigma.len() {
                        sigma.push(0);
                    }

                    sigma[w as usize] += sigma_v;

                    while w as usize >= predecessors.len() {
                        predecessors.push(Vec::new());
                    }

                    predecessors[w as usize].push(v);
                }
            }
        }

        let mut delta = Vec::new();
        while let Some(w) = stack.pop() {
            if let Some(pred) = predecessors.get(w as usize) {
                for v in pred {
                    while *v as usize >= delta.len() {
                        delta.push(0.0);
                    }

                    delta[*v as usize] += (sigma[*v as usize] as f64 / sigma[w as usize] as f64)
                        * (1.0 + delta.get(w as usize).unwrap_or(&0.0));
                }
            }

            if w != s {
                *centrality.entry(w).or_insert(0.0) += *delta.get(w as usize).unwrap_or(&0.0);
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
    pub host: HashMap<Node, f64>,
}

impl Betweenness {
    pub fn calculate(graph: &Webgraph) -> Self {
        Self {
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
