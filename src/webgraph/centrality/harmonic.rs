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

use std::collections::HashMap;

use indicatif::{ProgressBar, ProgressIterator, ProgressStyle};
use tracing::info;

use crate::webgraph::{graph_store::GraphStore, Node, NodeID, Store, Webgraph};

pub struct HarmonicCentrality {
    pub full: HashMap<Node, f64>,
    pub host: HashMap<Node, f64>,
}

fn calculate_centrality<S, F>(graph: &GraphStore<S>, node_distances: F) -> HashMap<Node, f64>
where
    S: Store,
    F: Fn(Node) -> HashMap<NodeID, usize>,
{
    let nodes: Vec<_> = graph.nodes().collect();
    info!("Found {} nodes in the graph", nodes.len());
    let pb = ProgressBar::new(nodes.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{wide_bar}] {pos:>7}/{len:7} ({eta})")
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
        .filter(|(_, centrality)| *centrality > 0.0)
        .collect()
}

fn calculate_full(graph: &Webgraph) -> HashMap<Node, f64> {
    graph
        .full
        .as_ref()
        .map(|full_graph| {
            calculate_centrality(full_graph, |node| graph.raw_reversed_distances(node))
        })
        .unwrap_or_default()
}

fn calculate_host(graph: &Webgraph) -> HashMap<Node, f64> {
    graph
        .host
        .as_ref()
        .map(|host_graph| {
            calculate_centrality(host_graph, |node| graph.raw_host_reversed_distances(node))
        })
        .unwrap_or_default()
}

impl HarmonicCentrality {
    pub fn calculate(graph: &Webgraph) -> Self {
        Self {
            host: calculate_host(graph),
            full: calculate_full(graph),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::webgraph::WebgraphBuilder;

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

        graph.flush();

        let centrality = HarmonicCentrality::calculate(&graph);
        assert!(
            centrality.full.get(&Node::from("A.com/1")).unwrap()
                > centrality.full.get(&Node::from("B.com")).unwrap()
        );

        assert!(
            centrality.host.get(&Node::from("B.com")).unwrap()
                > centrality.host.get(&Node::from("A.com")).unwrap_or(&0.0)
        );
    }

    #[test]
    fn harmonic_centrality() {
        let graph = test_graph();

        let centrality = HarmonicCentrality::calculate(&graph);

        assert_eq!(centrality.full.get(&Node::from("C")).unwrap(), &1.0);
        assert_eq!(centrality.full.get(&Node::from("D")), None);
        assert_eq!(
            (*centrality.full.get(&Node::from("A")).unwrap() * 100.0).round() / 100.0,
            0.67
        );
        assert_eq!(
            (*centrality.full.get(&Node::from("B")).unwrap() * 100.0).round() / 100.0,
            0.61
        );
    }

    #[test]
    fn www_subdomain_ignored() {
        let mut graph = WebgraphBuilder::new_memory()
            .with_full_graph()
            .with_host_graph()
            .open();

        graph.insert(Node::from("B.com"), Node::from("A.com"), String::new());
        graph.insert(Node::from("B.com"), Node::from("www.A.com"), String::new());

        graph.flush();
        let centrality = HarmonicCentrality::calculate(&graph);

        assert_eq!(centrality.host.get(&Node::from("A.com")), Some(&1.0));
        assert_eq!(centrality.host.get(&Node::from("www.A.com")), None);
    }
}
