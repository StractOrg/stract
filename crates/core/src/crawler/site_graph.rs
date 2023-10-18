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

//! In-memory graph that the worker constructs for the site during crawl.

use hashbrown::{HashMap, HashSet};
use url::Url;

use crate::{hyperloglog::HyperLogLog, kahan_sum::KahanSum};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Node {
    url: Url,
}

impl From<Url> for Node {
    fn from(url: Url) -> Self {
        Self { url }
    }
}

impl From<Node> for Url {
    fn from(node: Node) -> Self {
        node.url
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct NodeRef(usize);

#[derive(Default)]
pub struct SiteGraph {
    nodes: Vec<Node>,
    node_refs: HashMap<Node, NodeRef>,
    edges: Vec<(NodeRef, NodeRef)>,
}

impl SiteGraph {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_node(&mut self, node: Node) -> NodeRef {
        if let Some(node_ref) = self.node_refs.get(&node) {
            return *node_ref;
        }

        let node_ref = NodeRef(self.nodes.len());
        self.nodes.push(node.clone());
        self.node_refs.insert(node, node_ref);

        node_ref
    }

    pub fn add_edge(&mut self, from: NodeRef, to: NodeRef) {
        self.edges.push((from, to));
    }

    pub fn get_node(&self, node_ref: NodeRef) -> Option<&Node> {
        self.nodes.get(node_ref.0)
    }

    pub fn compute_centralities(&self) -> HashMap<NodeRef, f64> {
        let mut counters: HashMap<_, _> = self
            .node_refs
            .iter()
            .map(|(_, node_ref)| {
                let mut counter: HyperLogLog<64> = HyperLogLog::default();
                counter.add(node_ref.0 as u64);
                (*node_ref, counter)
            })
            .collect();

        let mut centralities: HashMap<_, _> = self
            .node_refs
            .iter()
            .map(|(_, node_ref)| {
                let kahan = KahanSum::default();

                (*node_ref, kahan)
            })
            .collect();

        let mut changed_nodes: HashSet<_> = self.node_refs.values().copied().collect();
        let mut t = 0;

        loop {
            if changed_nodes.is_empty() {
                break;
            }

            let mut new_counters = counters.clone();
            let mut new_changed_nodes = HashSet::new();

            for (from, to) in &self.edges {
                if changed_nodes.contains(from) {
                    let counter_to = new_counters.get_mut(to).unwrap();
                    let counter_from = &counters[from];

                    if counter_to
                        .registers()
                        .iter()
                        .zip(counter_from.registers().iter())
                        .any(|(to, from)| *from > *to)
                    {
                        counter_to.merge(counter_from);
                        new_changed_nodes.insert(*to);
                    }
                }
            }

            centralities.iter_mut().for_each(|(node, score)| {
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
            });

            counters = new_counters;
            changed_nodes = new_changed_nodes;
            t += 1;
        }

        centralities
            .into_iter()
            .map(|(node_ref, kahan)| (node_ref, f64::from(kahan)))
            .filter(|(_, centrality)| *centrality > 0.0)
            .map(|(node_ref, centrality)| {
                if !centrality.is_finite() {
                    (node_ref, 0.0)
                } else {
                    (node_ref, centrality)
                }
            })
            .collect()
    }
}
