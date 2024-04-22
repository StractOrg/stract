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

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::atomic::AtomicBool,
};

use std::sync::atomic::Ordering;

use crate::bloom::U64BloomFilter;
use tracing::info;

use crate::{
    hyperloglog::HyperLogLog,
    kahan_sum::KahanSum,
    webgraph::{NodeID, Webgraph},
};

const HYPERLOGLOG_COUNTERS: usize = 64;

type Counter = BTreeMap<NodeID, HyperLogLog<HYPERLOGLOG_COUNTERS>>;

fn initialize(
    graph: &Webgraph,
    counters: &mut Counters,
    centralities: &mut BTreeMap<NodeID, KahanSum>,
) -> u64 {
    let mut num_nodes = 0;

    for node in graph.nodes() {
        let mut counter = HyperLogLog::default();
        counter.add(node.as_u64());

        counters.old.insert(node, counter);
        centralities.insert(node, KahanSum::default());

        num_nodes += 1;
    }

    counters.new = counters.old.clone();

    num_nodes
}

fn update_changed_counters(
    graph: &Webgraph,
    exact_changed_nodes: &mut BTreeSet<NodeID>,
    counters: &mut Counters,
    new_changed_nodes: &mut U64BloomFilter,
) -> bool {
    let mut new_exact_changed_nodes = BTreeSet::default();
    let has_changes = AtomicBool::new(false);

    exact_changed_nodes.iter().for_each(|changed_node| {
        for edge in graph.raw_outgoing_edges(changed_node) {
            if let (Some(counter_to), Some(counter_from)) =
                (counters.new.get_mut(&edge.to), counters.old.get(&edge.from))
            {
                if counter_to
                    .registers()
                    .iter()
                    .zip(counter_from.registers().iter())
                    .any(|(to, from)| *from > *to)
                {
                    counter_to.merge(counter_from);
                    new_changed_nodes.insert(edge.to.as_u64());

                    new_exact_changed_nodes.insert(edge.to);

                    has_changes.store(true, Ordering::Relaxed);
                }
            }
        }
    });

    *exact_changed_nodes = new_exact_changed_nodes;

    has_changes.load(Ordering::Relaxed)
}

fn update_all_counters(
    graph: &Webgraph,
    mut exact_changed_nodes: Option<&mut BTreeSet<NodeID>>,
    counters: &mut Counters,
    changed_nodes: &U64BloomFilter,
    new_changed_nodes: &mut U64BloomFilter,
) -> bool {
    let has_changes = AtomicBool::new(false);

    if let Some(exact_changed_nodes) = &mut exact_changed_nodes {
        **exact_changed_nodes = BTreeSet::default();
    }

    graph.edges().for_each(|edge| {
        if changed_nodes.contains(edge.from.as_u64()) {
            if let (Some(counter_to), Some(counter_from)) =
                (counters.new.get_mut(&edge.to), counters.old.get(&edge.from))
            {
                if counter_to
                    .registers()
                    .iter()
                    .zip(counter_from.registers().iter())
                    .any(|(to, from)| *from > *to)
                {
                    counter_to.merge(counter_from);
                    new_changed_nodes.insert(edge.to.as_u64());

                    if let Some(exact_changed_nodes) = &mut exact_changed_nodes {
                        exact_changed_nodes.insert(edge.to);
                    }

                    has_changes.store(true, Ordering::Relaxed);
                }
            }
        }
    });

    has_changes.load(Ordering::Relaxed)
}

fn update_centralities(centralities: &mut BTreeMap<NodeID, KahanSum>, counters: &Counters, t: u64) {
    centralities.iter_mut().for_each(|(node, score)| {
        *score += counters
            .new
            .get(node)
            .map(|counter| counter.size())
            .unwrap_or(1)
            .checked_sub(
                counters
                    .old
                    .get(node)
                    .map(|counter| counter.size())
                    .unwrap_or(1),
            )
            .unwrap_or_default() as f64
            / (t + 1) as f64;
    });
}

fn normalize_centralities(
    centralities: BTreeMap<NodeID, KahanSum>,
    norm_factor: f64,
) -> BTreeMap<NodeID, f64> {
    centralities
        .into_iter()
        .map(|(node_id, sum)| (node_id, f64::from(sum)))
        .filter(|(_, centrality)| *centrality > 0.0)
        .map(|(node_id, centrality)| (node_id, centrality / norm_factor))
        .map(|(node_id, centrality)| {
            if !centrality.is_finite() {
                (node_id, 0.0)
            } else {
                (node_id, centrality)
            }
        })
        .collect()
}

struct Counters {
    old: Counter,
    new: Counter,
}

impl Counters {
    fn new() -> Self {
        Self {
            old: BTreeMap::default(),
            new: BTreeMap::default(),
        }
    }

    fn step(&mut self) {
        self.old = self.new.clone();
    }
}

fn calculate_centrality(graph: &Webgraph) -> BTreeMap<NodeID, f64> {
    let mut counters = Counters::new();
    let mut centralities: BTreeMap<NodeID, KahanSum> = BTreeMap::new();

    let num_nodes = initialize(graph, &mut counters, &mut centralities);

    let mut changed_nodes = U64BloomFilter::new(num_nodes, 0.05);

    for node in graph.nodes() {
        changed_nodes.insert(node.as_u64());
    }

    info!("Found {} nodes in the graph", num_nodes);
    let exact_counting_threshold = (num_nodes as f64).sqrt().max(0.0).round() as u64;
    let norm_factor = (num_nodes - 1) as f64;

    let mut exact_counting = false;
    let mut has_changes = true;
    let mut t = 0;

    let mut exact_changed_nodes: BTreeSet<NodeID> = BTreeSet::default();

    loop {
        if !has_changes {
            break;
        }

        let mut new_changed_nodes = U64BloomFilter::new(num_nodes, 0.05);

        if !exact_changed_nodes.is_empty()
            && exact_changed_nodes.len() as u64 <= exact_counting_threshold
        {
            has_changes = update_changed_counters(
                graph,
                &mut exact_changed_nodes,
                &mut counters,
                &mut new_changed_nodes,
            );
        } else if exact_counting {
            has_changes = update_all_counters(
                graph,
                Some(&mut exact_changed_nodes),
                &mut counters,
                &changed_nodes,
                &mut new_changed_nodes,
            );
        } else {
            has_changes = update_all_counters(
                graph,
                None,
                &mut counters,
                &changed_nodes,
                &mut new_changed_nodes,
            );
        }

        update_centralities(&mut centralities, &counters, t);

        counters.step();
        changed_nodes = new_changed_nodes;
        t += 1;

        if changed_nodes.estimate_card() <= exact_counting_threshold {
            exact_counting = true;
        }
    }

    let res = normalize_centralities(centralities, norm_factor);

    info!("Harmonic centrality calculated");

    res
}

pub struct HarmonicCentrality(BTreeMap<NodeID, f64>);

impl HarmonicCentrality {
    pub fn calculate(graph: &Webgraph) -> Self {
        Self(calculate_centrality(graph))
    }

    pub fn get(&self, node: &NodeID) -> Option<f64> {
        self.0.get(node).copied()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&NodeID, f64)> {
        self.0.iter().map(|(node, centrality)| (node, *centrality))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::webgraph::{Node, WebgraphWriter};

    fn test_edges() -> Vec<(Node, Node, String)> {
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
        vec![
            (Node::from("A"), Node::from("B"), String::new()),
            (Node::from("B"), Node::from("C"), String::new()),
            (Node::from("A"), Node::from("C"), String::new()),
            (Node::from("C"), Node::from("A"), String::new()),
            (Node::from("D"), Node::from("C"), String::new()),
        ]
    }

    fn test_graph() -> Webgraph {
        let mut writer = WebgraphWriter::new(
            crate::gen_temp_path(),
            crate::executor::Executor::single_thread(),
            crate::webgraph::Compression::default(),
        );

        for (from, to, label) in test_edges() {
            writer.insert(from, to, label);
        }

        writer.finalize()
    }

    #[test]
    fn host_harmonic_centrality() {
        let mut writer = WebgraphWriter::new(
            crate::gen_temp_path(),
            crate::executor::Executor::single_thread(),
            crate::webgraph::Compression::default(),
        );

        writer.insert(
            Node::from("A.com/1").into_host(),
            Node::from("A.com/2").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("A.com/1").into_host(),
            Node::from("A.com/3").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("A.com/1").into_host(),
            Node::from("A.com/4").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("A.com/2").into_host(),
            Node::from("A.com/1").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("A.com/2").into_host(),
            Node::from("A.com/3").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("A.com/2").into_host(),
            Node::from("A.com/4").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("A.com/3").into_host(),
            Node::from("A.com/1").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("A.com/3").into_host(),
            Node::from("A.com/2").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("A.com/3").into_host(),
            Node::from("A.com/4").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("A.com/4").into_host(),
            Node::from("A.com/1").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("A.com/4").into_host(),
            Node::from("A.com/2").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("A.com/4").into_host(),
            Node::from("A.com/3").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("C.com").into_host(),
            Node::from("B.com").into_host(),
            String::new(),
        );
        writer.insert(
            Node::from("D.com").into_host(),
            Node::from("B.com").into_host(),
            String::new(),
        );

        let graph = writer.finalize();

        let centrality = HarmonicCentrality::calculate(&graph);

        assert!(
            centrality.get(&Node::from("B.com").id()).unwrap()
                > centrality.get(&Node::from("A.com").id()).unwrap_or(0.0)
        );
    }

    #[test]
    fn harmonic_centrality() {
        let graph = test_graph();
        let centrality = HarmonicCentrality::calculate(&graph);

        assert!(
            centrality.get(&Node::from("C").id()).unwrap()
                > centrality.get(&Node::from("A").id()).unwrap()
        );
        assert!(
            centrality.get(&Node::from("A").id()).unwrap()
                > centrality.get(&Node::from("B").id()).unwrap()
        );
        assert_eq!(centrality.get(&Node::from("D").id()), None);
    }

    #[test]
    fn additional_edges_ignored() {
        let graph = test_graph();
        let centrality = HarmonicCentrality::calculate(&graph);

        let mut other = WebgraphWriter::new(
            crate::gen_temp_path(),
            crate::executor::Executor::single_thread(),
            crate::webgraph::Compression::default(),
        );

        for (from, to, label) in test_edges() {
            other.insert(from, to, label);
        }

        other.insert(Node::from("A"), Node::from("B"), "1".to_string());
        other.commit();
        other.insert(Node::from("A"), Node::from("B"), "2".to_string());
        other.commit();
        other.insert(Node::from("A"), Node::from("B"), "3".to_string());
        other.commit();
        other.insert(Node::from("A"), Node::from("B"), "4".to_string());
        other.commit();
        other.insert(Node::from("A"), Node::from("B"), "5".to_string());
        other.commit();
        other.insert(Node::from("A"), Node::from("B"), "6".to_string());
        other.commit();
        other.insert(Node::from("A"), Node::from("B"), "7".to_string());
        other.commit();

        let mut graph = other.finalize();
        graph.optimize_read();

        let centrality_extra = HarmonicCentrality::calculate(&graph);

        assert_eq!(centrality.0, centrality_extra.0);
    }
}
