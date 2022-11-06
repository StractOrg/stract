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

use tracing::info;

use crate::{
    hyperloglog::HyperLogLog,
    intmap::IntMap,
    webgraph::{graph_store::GraphStore, Node, Store, Webgraph},
};

pub struct HarmonicCentrality {
    pub full: HashMap<Node, f64>,
    pub host: HashMap<Node, f64>,
}

fn calculate_centrality<S>(graph: &GraphStore<S>) -> HashMap<Node, f64>
where
    S: Store,
{
    let nodes: Vec<_> = graph.nodes().collect();
    info!("Found {} nodes in the graph", nodes.len());
    let norm_factor = (nodes.len() - 1) as f64;

    let mut counters: IntMap<HyperLogLog<16>> = nodes
        .iter()
        .map(|node| {
            let mut counter = HyperLogLog::default();
            counter.add(*node);

            (*node, counter)
        })
        .collect();

    let mut counter_changes = counters.len();
    let mut t = 0;
    let mut centralities: IntMap<f64> = nodes.iter().map(|node| (*node, 0.0)).collect();

    loop {
        if counter_changes == 0 {
            break;
        }

        counter_changes = 0;
        let mut new_counters: IntMap<HyperLogLog<16>> = counters.clone();

        for edge in graph.edges() {
            let counter_to = new_counters.get_mut(&edge.to).unwrap();
            let counter_from = counters.get(&edge.from).unwrap();

            if counter_to
                .registers()
                .iter()
                .zip(counter_from.registers().iter())
                .any(|(to, from)| *from > *to)
            {
                counter_to.merge(counter_from);
                counter_changes += 1;
            }
        }

        for (node, score) in centralities.iter_mut() {
            *score += new_counters
                .get(node)
                .map(|counter| counter.size())
                .unwrap_or_default()
                .checked_sub(
                    counters
                        .get(node)
                        .map(|counter| counter.size())
                        .unwrap_or_default(),
                )
                .unwrap_or_default() as f64
                / (t + 1) as f64;
        }

        counters = new_counters;
        t += 1;
    }

    centralities
        .into_iter()
        .filter(|(_, centrality)| *centrality > 0.0)
        .map(|(node_id, centrality)| (graph.id2node(&node_id).unwrap(), centrality / norm_factor))
        .collect()
}

fn calculate_full(graph: &Webgraph) -> HashMap<Node, f64> {
    graph
        .full
        .as_ref()
        .map(calculate_centrality)
        .unwrap_or_default()
}

fn calculate_host(graph: &Webgraph) -> HashMap<Node, f64> {
    graph
        .host
        .as_ref()
        .map(calculate_centrality)
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

        assert!(
            centrality.full.get(&Node::from("C")).unwrap()
                > centrality.full.get(&Node::from("A")).unwrap()
        );
        assert!(
            centrality.full.get(&Node::from("A")).unwrap()
                > centrality.full.get(&Node::from("B")).unwrap()
        );
        assert_eq!(centrality.full.get(&Node::from("D")), None);
    }

    #[test]
    fn www_subdomain_ignored() {
        let mut graph = WebgraphBuilder::new_memory()
            .with_full_graph()
            .with_host_graph()
            .open();

        graph.insert(Node::from("B.com"), Node::from("A.com"), String::new());
        graph.insert(Node::from("B.com"), Node::from("www.A.com"), String::new());
        graph.insert(Node::from("C.com"), Node::from("A.com"), String::new());
        graph.insert(Node::from("C.com"), Node::from("www.A.com"), String::new());

        graph.flush();
        let centrality = HarmonicCentrality::calculate(&graph);

        assert!(centrality.host.get(&Node::from("A.com")).is_some());
        assert_eq!(centrality.host.get(&Node::from("www.A.com")), None);
    }

    #[test]
    fn additional_edges_ignored() {
        let mut graph = test_graph();

        graph.insert(Node::from("A"), Node::from("B"), String::new());
        graph.insert(Node::from("A"), Node::from("B"), String::new());
        graph.insert(Node::from("A"), Node::from("B"), String::new());
        graph.insert(Node::from("A"), Node::from("B"), String::new());
        graph.insert(Node::from("A"), Node::from("B"), String::new());
        graph.insert(Node::from("A"), Node::from("B"), String::new());
        graph.insert(Node::from("A"), Node::from("B"), String::new());

        graph.flush();
        let centrality = HarmonicCentrality::calculate(&graph);

        assert!(
            centrality.full.get(&Node::from("C")).unwrap()
                > centrality.full.get(&Node::from("A")).unwrap()
        );
        assert!(
            centrality.full.get(&Node::from("A")).unwrap()
                > centrality.full.get(&Node::from("B")).unwrap()
        );
        assert_eq!(centrality.full.get(&Node::from("D")), None);
    }
}
