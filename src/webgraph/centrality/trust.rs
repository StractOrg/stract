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

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::Path;

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::webgraph::Node;
use crate::webgraph::{centrality::betweenness::Betweenness, NodeID, Webgraph};
use crate::webpage::Webpage;
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
                .map(|node| (proxy.dist_from_node.get(node).unwrap_or(&max_dist) + 1) as f64)
                .sum::<f64>();

        Self {
            node: proxy,
            weight,
            max_dist,
        }
    }

    fn score(&self, node: NodeID) -> f64 {
        self.weight
            * (self
                .node
                .dist_to_node
                .get(&node)
                .unwrap_or(&(self.max_dist + 1))
                + 1) as f64
    }
}

pub struct Scorer<'a> {
    proxy_nodes: Vec<WeightedProxyNode<'a>>,
    trusted_node_ids: HashSet<NodeID>,
}

impl<'a> Scorer<'a> {
    fn new(proxy_nodes: &'a [ProxyNode], trusted_nodes: &[NodeID], max_dist: usize) -> Self {
        let mut proxy_nodes: Vec<_> = proxy_nodes
            .iter()
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
            trusted_node_ids: trusted_nodes.iter().copied().collect(),
        }
    }

    pub fn score(&self, node: NodeID) -> f64 {
        if self.trusted_node_ids.contains(&node) {
            1.0
        } else {
            1.0 / (1.0
                + self
                    .proxy_nodes
                    .iter()
                    .map(|proxy| proxy.score(node))
                    .sum::<f64>())
        }
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct TrustedCentrality {
    node2id: HashMap<Node, NodeID>,
    proxy_nodes: Vec<ProxyNode>,
    max_dist: usize,
}

impl TrustedCentrality {
    pub fn new(graph: &Webgraph) -> Self {
        Self::new_with_num_proxy(graph, NUM_PROXY_NODES)
    }

    fn new_with_num_proxy(graph: &Webgraph, num_proxy_nodes: usize) -> Self {
        let betweenness = Betweenness::calculate_with_progress(graph);

        let mut nodes = betweenness.centrality.into_iter().collect_vec();
        nodes.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let node2id: HashMap<Node, NodeID> = graph
            .host
            .as_ref()
            .map(|store| {
                store
                    .nodes()
                    .map(|id| (store.id2node(&id).unwrap(), id))
                    .collect::<HashMap<Node, NodeID>>()
            })
            .unwrap_or_default();

        let proxy_nodes = nodes
            .into_iter()
            .map(|(node, _)| node)
            .map(|node| ProxyNode {
                id: *node2id.get(&node).unwrap(),
                dist_to_node: graph
                    .host_distances(node.clone())
                    .into_iter()
                    .map(|(node, dist)| (*node2id.get(&node).unwrap(), dist))
                    .collect(),
                dist_from_node: graph
                    .host_reversed_distances(node)
                    .into_iter()
                    .map(|(node, dist)| (*node2id.get(&node).unwrap(), dist))
                    .collect(),
            })
            .take(num_proxy_nodes)
            .collect_vec();

        Self {
            node2id,
            proxy_nodes,
            max_dist: betweenness.max_dist,
        }
    }

    pub fn website_to_id(&self, website: &Webpage) -> Option<NodeID> {
        self.node2id
            .get(&Node::from_website(website).into_host())
            .copied()
    }

    pub fn scorer(&self, trusted_nodes: &[Node]) -> Scorer<'_> {
        let trusted_nodes = trusted_nodes
            .into_iter()
            .filter_map(|node| self.node2id.get(node).copied())
            .collect_vec();

        Scorer::new(&self.proxy_nodes, &trusted_nodes, self.max_dist)
    }

    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let mut file = File::options()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;

        let buf = bincode::serialize(&self)?;
        file.write_all(&buf)?;

        Ok(())
    }

    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::options().open(path)?;
        let mut reader = BufReader::new(file);

        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;

        Ok(bincode::deserialize(&buf)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::webgraph::WebgraphBuilder;

    fn test_graph() -> Webgraph {
        /*
           G ◄---- F
           |       |
           |       ▼
           ------► E -------► H
                   ▲          |
           A --    |          |
               |   |          ▼
           B ----► D ◄------- C
        */

        let mut graph = WebgraphBuilder::new_memory()
            .with_full_graph()
            .with_host_graph()
            .open();

        graph.insert(Node::from("A"), Node::from("D"), String::new());
        graph.insert(Node::from("B"), Node::from("D"), String::new());
        graph.insert(Node::from("C"), Node::from("D"), String::new());
        graph.insert(Node::from("D"), Node::from("E"), String::new());
        graph.insert(Node::from("E"), Node::from("H"), String::new());
        graph.insert(Node::from("H"), Node::from("C"), String::new());
        graph.insert(Node::from("F"), Node::from("E"), String::new());
        graph.insert(Node::from("F"), Node::from("G"), String::new());
        graph.insert(Node::from("G"), Node::from("E"), String::new());

        graph.flush();

        graph
    }

    #[test]
    fn proxy_nodes_selection() {
        // Trusted nodes: E, C
        let graph = test_graph();
        let centrality = TrustedCentrality::new_with_num_proxy(&graph, 3);
        let id2node: HashMap<NodeID, Node> = centrality
            .node2id
            .clone()
            .into_iter()
            .map(|(node, id)| (id, node))
            .collect();

        assert_eq!(centrality.proxy_nodes.len(), 3);
        let mut proxy_nodes = centrality
            .proxy_nodes
            .iter()
            .map(|node| id2node.get(&node.id).unwrap().clone())
            .collect_vec();
        proxy_nodes.sort();

        assert_eq!(
            proxy_nodes,
            vec![
                Node::from("D".to_string()),
                Node::from("E".to_string()),
                Node::from("H".to_string())
            ]
        );
    }

    #[test]
    fn trusted_nodes_centrality() {
        let graph = test_graph();
        let centrality = TrustedCentrality::new_with_num_proxy(&graph, 3);

        let trusted_nodes = vec![Node::from("D".to_string()), Node::from("E".to_string())];

        let scorer = centrality.scorer(&trusted_nodes);

        for node in &trusted_nodes {
            assert_eq!(scorer.score(*centrality.node2id.get(node).unwrap()), 1.0);
        }
    }

    #[test]
    fn ordering() {
        let graph = test_graph();
        let centrality = TrustedCentrality::new_with_num_proxy(&graph, 3);
        assert_eq!(centrality.max_dist, 4);

        let trusted_nodes = vec![Node::from("B".to_string()), Node::from("E".to_string())];

        let scorer = centrality.scorer(&trusted_nodes);

        assert!(
            scorer.score(
                *centrality
                    .node2id
                    .get(&Node::from("E".to_string()))
                    .unwrap()
            ) > scorer.score(
                *centrality
                    .node2id
                    .get(&Node::from("H".to_string()))
                    .unwrap()
            )
        );
        assert!(
            scorer.score(
                *centrality
                    .node2id
                    .get(&Node::from("H".to_string()))
                    .unwrap()
            ) > scorer.score(
                *centrality
                    .node2id
                    .get(&Node::from("C".to_string()))
                    .unwrap()
            )
        );

        assert!(
            scorer.score(
                *centrality
                    .node2id
                    .get(&Node::from("C".to_string()))
                    .unwrap()
            ) > scorer.score(
                *centrality
                    .node2id
                    .get(&Node::from("A".to_string()))
                    .unwrap()
            )
        );
    }
}
