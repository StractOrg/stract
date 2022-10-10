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

//! Algorithm in broad strokes
//! We first calculate the betweenness centrality for all nodes an choose top k as proxy nodes.
//! The distances from every node to every proxy node and reverse is then calculated and stored.
//!
//! During search the user can choose a set of trusted node. Every proxy node then gets a weight
//! of weight(p) = 1 / sum(dist(p, t) for t in trusted_nodes). The top s proxy nodes are then chosen to be used during search.
//! For each search candidate, u, they get a score of score(u) = 1 / (1 + sum(weight(p) * d(p, u) for p in best_proxy_nodes))

use std::collections::HashMap;
use std::path::Path;

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::webgraph::{centrality::betweenness::Betweenness, NodeID, Webgraph};
use crate::Result;

const NUM_PROXY_NODES: usize = 1_000;
const NUM_PROXY_NODES_FOR_SEARCH: usize = 10;

#[derive(Serialize, Deserialize)]
struct ProxyNode {
    id: NodeID,
    dist_from_node: HashMap<NodeID, usize>, // from node to proxy node
    dist_to_node: HashMap<NodeID, usize>,   // from proxy node to other node
}

struct WeightedProxyNode<'a> {
    weight: f64,
    node: &'a ProxyNode,
    max_dist: usize,
}

impl<'a> WeightedProxyNode<'a> {
    fn new(proxy: &'a ProxyNode, trusted_nodes: &[NodeID], max_dist: usize) -> Self {
        let weight = 1.0
            / trusted_nodes
                .iter()
                .map(|node| *proxy.dist_from_node.get(node).unwrap_or(&max_dist) as f64)
                .sum::<f64>();

        Self {
            node: proxy,
            weight,
            max_dist,
        }
    }

    fn score(&self, node: NodeID) -> f64 {
        self.weight
            * *self
                .node
                .dist_to_node
                .get(&node)
                .unwrap_or(&(self.max_dist + 1)) as f64
    }
}

pub struct Scorer<'a> {
    proxy_nodes: Vec<WeightedProxyNode<'a>>,
}

impl<'a> Scorer<'a> {
    fn new(proxy_nodes: &'a [ProxyNode], trusted_nodes: &[NodeID], max_dist: usize) -> Self {
        let mut proxy_nodes: Vec<_> = proxy_nodes
            .into_iter()
            .map(|node| WeightedProxyNode::new(node, trusted_nodes, max_dist))
            .collect();

        proxy_nodes.sort_by(|a, b| {
            b.weight
                .partial_cmp(&a.weight)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Self {
            proxy_nodes: proxy_nodes
                .into_iter()
                .take(NUM_PROXY_NODES_FOR_SEARCH)
                .collect(),
        }
    }

    pub fn score(&self, node: NodeID) -> f64 {
        1.0 / (1.0
            + self
                .proxy_nodes
                .iter()
                .map(|proxy| proxy.score(node))
                .sum::<f64>())
    }
}

#[derive(Serialize, Deserialize)]
pub struct TrustedCentrality {
    proxy_nodes: Vec<ProxyNode>,
    max_dist: usize,
}

impl TrustedCentrality {
    pub fn new(graph: &Webgraph) -> Self {
        let betweenness = Betweenness::calculate(graph);
        let mut nodes = betweenness.centrality.into_iter().collect_vec();
        nodes.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let proxy_nodes = nodes
            .into_iter()
            .map(|(node, _)| node)
            .map(|node| (graph.host.as_ref().unwrap().node2id(&node).unwrap(), node))
            .map(|(id, node)| ProxyNode {
                id,
                dist_to_node: graph
                    .host_distances(node.clone())
                    .into_iter()
                    .map(|(node, dist)| {
                        (graph.host.as_ref().unwrap().node2id(&node).unwrap(), dist)
                    })
                    .collect(),
                dist_from_node: graph
                    .host_reversed_distances(node.clone())
                    .into_iter()
                    .map(|(node, dist)| {
                        (graph.host.as_ref().unwrap().node2id(&node).unwrap(), dist)
                    })
                    .collect(),
            })
            .take(NUM_PROXY_NODES)
            .collect_vec();

        Self {
            proxy_nodes,
            max_dist: betweenness.max_dist,
        }
    }

    pub fn scorer(&self, trusted_nodes: &[NodeID]) -> Scorer<'_> {
        Scorer::new(&self.proxy_nodes, trusted_nodes, self.max_dist)
    }

    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        todo!();
    }

    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        todo!();
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn trusted_centrality() {
        todo!("example from ipad");
    }
}
