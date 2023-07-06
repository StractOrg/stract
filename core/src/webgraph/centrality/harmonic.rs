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

use std::collections::HashMap;

use bitvec::vec::BitVec;
use tracing::info;

use crate::{
    hyperloglog::HyperLogLog,
    intmap::{IntMap, IntSet},
    kahan_sum::KahanSum,
    webgraph::{Node, NodeID, Webgraph},
};

const HYPERLOGLOG_COUNTERS: usize = 64;
const EXACT_COUNTING_THRESHOLD: u64 = 1_000_000;

#[derive(Clone)]
struct JankyBloomFilter {
    bit_vec: BitVec,
    num_bits: u64,
}

impl JankyBloomFilter {
    pub fn new(estimated_items: u64, fp: f64) -> Self {
        let num_bits = Self::num_bits(estimated_items, fp);
        Self {
            bit_vec: BitVec::repeat(false, num_bits as usize),
            num_bits,
        }
    }

    fn num_bits(estimated_items: u64, fp: f64) -> u64 {
        ((estimated_items as f64) * fp.ln() / (-8.0 * 2.0_f64.ln().powi(2))).ceil() as u64
    }

    fn hash(item: &u64) -> usize {
        item.wrapping_mul(11400714819323198549) as usize
    }

    pub fn insert(&mut self, item: u64) {
        let h = Self::hash(&item);
        self.bit_vec.set(h % self.num_bits as usize, true);
    }

    pub fn contains(&self, item: &u64) -> bool {
        let h = Self::hash(item);
        self.bit_vec[h % self.num_bits as usize]
    }

    pub fn estimate_card(&self) -> u64 {
        let num_ones = self.bit_vec.count_ones() as u64;

        if num_ones == 0 || self.num_bits == 0 {
            return 0;
        }

        if num_ones == self.num_bits {
            return u64::MAX;
        }

        (-(self.num_bits as i64) * (1.0 - (num_ones as f64) / (self.num_bits as f64)).ln() as i64)
            .try_into()
            .unwrap_or_default()
    }
}

pub struct HarmonicCentrality {
    pub host: HashMap<Node, f64>,
}

fn calculate_centrality(graph: &Webgraph) -> HashMap<Node, f64> {
    let nodes: Vec<_> = graph.nodes().collect();
    info!("Found {} nodes in the graph", nodes.len());
    let norm_factor = (nodes.len() - 1) as f64;

    let mut counters: IntMap<HyperLogLog<HYPERLOGLOG_COUNTERS>> = nodes
        .iter()
        .map(|node| {
            let mut counter = HyperLogLog::default();
            counter.add(node.0);

            (node.0, counter)
        })
        .collect();

    let mut exact_counting = false;
    let mut has_changes = true;
    let mut t = 0;
    let mut centralities: IntMap<KahanSum> = nodes
        .iter()
        .map(|node| (node.0, KahanSum::default()))
        .collect();

    let mut exact_changed_nodes = IntSet::default();
    let mut changed_nodes = JankyBloomFilter::new(nodes.len() as u64, 0.05);
    for node in &nodes {
        changed_nodes.insert(node.0);
    }

    loop {
        if !has_changes {
            break;
        }

        let mut new_counters: IntMap<_> = counters.clone();

        has_changes = false;
        let mut new_changed_nodes = JankyBloomFilter::new(nodes.len() as u64, 0.05);

        if exact_changed_nodes.len() > 0 {
            let mut new_exact_changed_nodes = IntSet::default();

            for changed_node in exact_changed_nodes.into_iter() {
                for edge in graph.raw_outgoing_edges(&NodeID(changed_node)) {
                    if let (Some(counter_to), Some(counter_from)) =
                        (new_counters.get_mut(&edge.to.0), counters.get(&edge.from.0))
                    {
                        if counter_to
                            .registers()
                            .iter()
                            .zip(counter_from.registers().iter())
                            .any(|(to, from)| *from > *to)
                        {
                            counter_to.merge(counter_from);
                            new_changed_nodes.insert(edge.to.0);

                            new_exact_changed_nodes.insert(edge.to.0);

                            has_changes = true;
                        }
                    }
                }
            }

            exact_changed_nodes = new_exact_changed_nodes;
        } else {
            exact_changed_nodes = IntSet::default();
            for edge in graph.edges() {
                if !changed_nodes.contains(&edge.from.0) {
                    continue;
                }

                if let (Some(counter_to), Some(counter_from)) =
                    (new_counters.get_mut(&edge.to.0), counters.get(&edge.from.0))
                {
                    if counter_to
                        .registers()
                        .iter()
                        .zip(counter_from.registers().iter())
                        .any(|(to, from)| *from > *to)
                    {
                        counter_to.merge(counter_from);
                        new_changed_nodes.insert(edge.to.0);

                        if exact_counting {
                            exact_changed_nodes.insert(edge.to.0);
                        }

                        has_changes = true;
                    }
                }
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
        changed_nodes = new_changed_nodes;
        t += 1;

        if changed_nodes.estimate_card() <= EXACT_COUNTING_THRESHOLD {
            exact_counting = true;
        }
    }

    centralities
        .into_iter()
        .map(|(node_id, sum)| (node_id, f64::from(sum)))
        .filter(|(_, centrality)| *centrality > 0.0)
        .map(|(node_id, centrality)| {
            (
                graph.id2node(&NodeID::from(node_id)).unwrap(),
                centrality / norm_factor,
            )
        })
        .collect()
}

fn calculate_host(graph: &Webgraph) -> HashMap<Node, f64> {
    calculate_centrality(graph)
}

impl HarmonicCentrality {
    pub fn calculate(graph: &Webgraph) -> Self {
        Self {
            host: calculate_host(graph),
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

        let mut graph = WebgraphBuilder::new_memory().open();

        graph.insert(Node::from("A"), Node::from("B"), String::new());
        graph.insert(Node::from("B"), Node::from("C"), String::new());
        graph.insert(Node::from("A"), Node::from("C"), String::new());
        graph.insert(Node::from("C"), Node::from("A"), String::new());
        graph.insert(Node::from("D"), Node::from("C"), String::new());

        graph.commit();

        graph
    }

    #[test]
    fn host_harmonic_centrality() {
        let mut graph = WebgraphBuilder::new_memory().open();

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

        graph.commit();

        let centrality = HarmonicCentrality::calculate(&graph);

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
            centrality.host.get(&Node::from("C")).unwrap()
                > centrality.host.get(&Node::from("A")).unwrap()
        );
        assert!(
            centrality.host.get(&Node::from("A")).unwrap()
                > centrality.host.get(&Node::from("B")).unwrap()
        );
        assert_eq!(centrality.host.get(&Node::from("D")), None);
    }

    #[test]
    fn www_subdomain_ignored() {
        let mut graph = WebgraphBuilder::new_memory().open();

        graph.insert(
            Node::from("B.com").into_host(),
            Node::from("A.com").into_host(),
            String::new(),
        );
        graph.insert(
            Node::from("B.com").into_host(),
            Node::from("www.A.com").into_host(),
            String::new(),
        );
        graph.insert(
            Node::from("C.com").into_host(),
            Node::from("A.com").into_host(),
            String::new(),
        );
        graph.insert(
            Node::from("C.com").into_host(),
            Node::from("www.A.com").into_host(),
            String::new(),
        );

        graph.commit();
        let centrality = HarmonicCentrality::calculate(&graph);

        assert!(centrality.host.get(&Node::from("A.com")).is_some());
        assert_eq!(centrality.host.get(&Node::from("www.A.com")), None);
    }

    #[test]
    fn additional_edges_ignored() {
        let mut graph = test_graph();

        let centrality = HarmonicCentrality::calculate(&graph);

        graph.insert(Node::from("A"), Node::from("B"), "1".to_string());
        graph.insert(Node::from("A"), Node::from("B"), "2".to_string());
        graph.insert(Node::from("A"), Node::from("B"), "3".to_string());
        graph.insert(Node::from("A"), Node::from("B"), "4".to_string());
        graph.insert(Node::from("A"), Node::from("B"), "5".to_string());
        graph.insert(Node::from("A"), Node::from("B"), "6".to_string());
        graph.insert(Node::from("A"), Node::from("B"), "7".to_string());

        graph.commit();

        let centrality_extra = HarmonicCentrality::calculate(&graph);

        assert_eq!(centrality.host, centrality_extra.host);
    }

    #[test]
    fn same_centrality_after_segment_merge() {
        let mut graph = WebgraphBuilder::new_memory().open();

        graph.insert(Node::from("A"), Node::from("B"), String::new());
        graph.commit();
        graph.insert(Node::from("B"), Node::from("C"), String::new());
        graph.commit();
        graph.insert(Node::from("A"), Node::from("C"), String::new());
        graph.commit();
        graph.insert(Node::from("C"), Node::from("A"), String::new());
        graph.commit();
        graph.insert(Node::from("D"), Node::from("C"), String::new());
        graph.commit();

        graph.merge_segments(1);
        let centrality = HarmonicCentrality::calculate(&graph);

        let orig_graph = test_graph();
        let orig_centrality = HarmonicCentrality::calculate(&orig_graph);

        assert_eq!(centrality.host, orig_centrality.host);
    }
}
