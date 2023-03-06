// Stract is an open source web search engine.
// Copyright (C) 2023 Stract ApS
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

use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};

use crate::{
    intmap::IntMap,
    webgraph::{Node, NodeID, Webgraph},
};

fn calculate(graph: &Webgraph, with_progress: bool) -> (HashMap<Node, f64>, i32) {
    let mut centrality: HashMap<NodeID, f64> = HashMap::new();
    let mut n = 0;
    let mut max_dist = 0;

    let nodes: Vec<_> = graph.nodes().take(100_000).collect();

    let pb =
        if with_progress {
            let pb = ProgressBar::new(nodes.len() as u64);
            pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{wide_bar}] {pos:>7}/{len:7} ({eta})")
            .progress_chars("#>-"),
    );
            Some(pb)
        } else {
            None
        };

    for s in nodes.into_iter() {
        if let Some(pb) = &pb {
            pb.inc(1);
        }

        n += 1;
        centrality.entry(s).or_default();

        let mut stack = Vec::new();
        let mut predecessors: IntMap<Vec<u64>> = IntMap::new();

        let mut sigma = IntMap::new();

        sigma.insert(s.0, 1);

        let mut distances = IntMap::new();
        distances.insert(s.0, 0);

        let mut q = VecDeque::new();
        q.push_back(s);

        while let Some(v) = q.pop_front() {
            stack.push(v);
            for edge in graph.raw_outgoing_edges(&v) {
                let w = edge.to;

                if !distances.contains(&w.0) {
                    let dist_v = distances.get(&v.0).unwrap();
                    q.push_back(w);
                    distances.insert(w.0, dist_v + 1);
                }

                if *distances.get(&w.0).unwrap() == distances.get(&v.0).unwrap() + 1 {
                    let sigma_v = *sigma.get(&v.0).unwrap_or(&0);

                    if !sigma.contains(&w.0) {
                        sigma.insert(w.0, 0);
                    }
                    *sigma.get_mut(&w.0).unwrap() += sigma_v;

                    if !predecessors.contains(&w.0) {
                        predecessors.insert(w.0, Vec::new());
                    }

                    predecessors.get_mut(&w.0).unwrap().push(v.0);
                }
            }
        }

        max_dist = max_dist.max(*distances.iter().map(|(_, dist)| dist).max().unwrap_or(&0));

        let mut delta = IntMap::new();
        while let Some(w) = stack.pop() {
            if let Some(pred) = predecessors.get(&w.0) {
                for v in pred {
                    let dv = delta.get(v).copied().unwrap_or(0.0);

                    delta.insert(
                        *v,
                        dv + (*sigma.get(v).unwrap() as f64 / *sigma.get(&w.0).unwrap() as f64)
                            * (1.0 + delta.get(&w.0).unwrap_or(&0.0)),
                    );
                }
            }

            if w != s {
                *centrality.entry(w).or_insert(0.0) += *delta.get(&w.0).unwrap_or(&0.0);
            }
        }
    }

    if let Some(pb) = &pb {
        pb.finish_and_clear();
    }

    let n = n as f64;
    let norm = n * (n - 1.0);

    (
        centrality
            .into_iter()
            .map(|(id, centrality)| (graph.id2node(&id).unwrap(), centrality / norm))
            .collect(),
        max_dist,
    )
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Betweenness {
    pub centrality: HashMap<Node, f64>,
    pub max_dist: usize,
}

impl Betweenness {
    #[allow(unused)]
    pub fn calculate(graph: &Webgraph) -> Self {
        let (host, max_dist) = calculate(graph, false);
        Self {
            centrality: host,
            max_dist: max_dist.max(0) as usize,
        }
    }

    pub fn calculate_with_progress(graph: &Webgraph) -> Self {
        let (host, max_dist) = calculate(graph, true);
        Self {
            centrality: host,
            max_dist: max_dist.max(0) as usize,
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

        let mut graph = WebgraphBuilder::new_memory().open();

        for i in 0..n - 1 {
            graph.insert(
                Node::from(i.to_string()),
                Node::from((i + 1).to_string()),
                String::new(),
            );
        }

        graph.commit();

        graph
    }

    #[test]
    fn path() {
        let p = create_path_graph(5);
        let centrality = Betweenness::calculate(&p);

        assert_eq!(
            centrality.centrality,
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
