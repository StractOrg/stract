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
//! During search the user can choose a set of liked and disliked nodes. For each user node, we find the best k proxy nodes.
//! For all proxy nodes we then estimate the harmonic centrality from the liked nodes - disliked nodes  by estimating
//! the shortest distance as the distance through their best proxy node. This distance estimate is very fast since
//! all distances are pre-computed for the proxy nodes.

use std::cell::RefCell;
use std::collections::{BinaryHeap, HashMap};
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::Path;

use boomphf::hashmap::BoomHashMap;
use itertools::Itertools;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::webgraph::Node;
use crate::webgraph::{centrality::betweenness::Betweenness, NodeID, Webgraph};
use crate::Result;

const NUM_PROXY_NODES: usize = 10_000;
const BEST_PROXY_NODES_PER_USER_NODE: usize = 3;
const USER_NODES_LIMIT: usize = 20; // if the user specifies more than this number of nodes, the remaining nodes will be merged into existing

#[derive(Serialize, Deserialize)]
pub struct ProxyNode {
    pub id: NodeID,
    dist_from_node: BoomHashMap<NodeID, usize>, // from node to proxy node
    dist_to_node: BoomHashMap<NodeID, usize>,   // from proxy node to other node
}

impl ProxyNode {
    fn dist(&self, from: &NodeID, to: &NodeID) -> Option<usize> {
        if let Some(from_node_to_proxy) = self.dist_from_node.get(from) {
            if let Some(from_proxy_to_node) = self.dist_to_node.get(to) {
                return Some(from_node_to_proxy + from_proxy_to_node);
            }
        }

        None
    }
}

struct WeightedProxyNode<'a> {
    node: &'a ProxyNode,
    dist: usize,
}

impl<'a> PartialOrd for WeightedProxyNode<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.dist.partial_cmp(&other.dist)
    }
}
impl<'a> Ord for WeightedProxyNode<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl<'a> PartialEq for WeightedProxyNode<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.dist == other.dist
    }
}

impl<'a> Eq for WeightedProxyNode<'a> {}

pub struct Scorer<'a> {
    fixed_scores: HashMap<NodeID, f64>,
    liked_nodes: Vec<UserNode<'a>>,
    cache: RefCell<HashMap<NodeID, f64>>,
    num_liked_nodes: usize,
}

impl<'a> Scorer<'a> {
    fn new(
        proxy_nodes: &'a [ProxyNode],
        liked_nodes: &[NodeID],
        fixed_scores: HashMap<NodeID, f64>,
        betweenness: &HashMap<NodeID, f64>,
    ) -> Self {
        let mut liked_nodes_user = Vec::new();

        let num_liked_nodes = liked_nodes.len();
        let mut liked_nodes = liked_nodes.to_vec();
        liked_nodes.sort_by(|a, b| {
            betweenness
                .get(b)
                .unwrap_or(&0.0)
                .partial_cmp(betweenness.get(a).unwrap_or(&0.0))
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        for id in liked_nodes {
            let user_node = UserNode::new(id, proxy_nodes);
            if liked_nodes_user.len() < USER_NODES_LIMIT {
                liked_nodes_user.push(user_node);
            } else {
                let mut best = liked_nodes_user
                    .iter_mut()
                    .min_by_key(|curr| user_node.best_dist(&curr.id).unwrap_or(1_000_000))
                    .unwrap();

                best.weight += 1;
            }
        }

        Self {
            fixed_scores,
            num_liked_nodes,
            liked_nodes: liked_nodes_user,
            cache: RefCell::new(HashMap::new()),
        }
    }

    pub fn score(&self, node: NodeID) -> f64 {
        if let Some(score) = self.fixed_scores.get(&node) {
            *score
        } else {
            if let Some(cached) = self.cache.borrow().get(&node) {
                return *cached;
            }

            let res = self
                .liked_nodes
                .iter()
                .filter_map(|liked_node| liked_node.best_dist(&node).map(|dist| (dist, liked_node)))
                .map(|(d, liked_node)| liked_node.weight as f64 / d as f64)
                .sum::<f64>()
                / self.num_liked_nodes as f64;

            self.cache.borrow_mut().insert(node, res);

            res
        }
    }
}

struct UserNode<'a> {
    id: NodeID,
    proxy_nodes: Vec<&'a ProxyNode>,
    weight: usize,
}

impl<'a> UserNode<'a> {
    fn new(id: NodeID, proxy_nodes: &'a [ProxyNode]) -> Self {
        let mut heap = BinaryHeap::with_capacity(BEST_PROXY_NODES_PER_USER_NODE);
        for proxy in proxy_nodes {
            if let Some(dist_to_proxy) = proxy.dist_from_node.get(&id) {
                let weighted_node = WeightedProxyNode {
                    node: proxy,
                    dist: *dist_to_proxy,
                };

                if heap.len() == BEST_PROXY_NODES_PER_USER_NODE {
                    if let Some(mut worst) = heap.peek_mut() {
                        *worst = weighted_node;
                    }
                } else {
                    heap.push(weighted_node);
                }
            }
        }

        Self {
            id,
            weight: 1,
            proxy_nodes: heap.into_iter().map(|weighted| weighted.node).collect(),
        }
    }

    fn best_dist(&self, node: &NodeID) -> Option<usize> {
        let mut best = None;

        for proxy in &self.proxy_nodes {
            if let Some(dist) = proxy.dist(&self.id, node) {
                best = match best {
                    Some((best_dist, best_proxy)) => {
                        if dist < best_dist {
                            Some((dist, *proxy))
                        } else {
                            Some((best_dist, best_proxy))
                        }
                    }
                    None => Some((dist, *proxy)),
                }
            }
        }

        best.map(|(dist, _)| dist)
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct ApproximatedHarmonicCentrality {
    pub node2id: HashMap<Node, NodeID>,
    pub proxy_nodes: Vec<ProxyNode>,
    betweenness: HashMap<NodeID, f64>,
}

impl ApproximatedHarmonicCentrality {
    pub fn new(graph: &Webgraph) -> Self {
        Self::new_with_num_proxy(graph, NUM_PROXY_NODES)
    }

    fn new_with_num_proxy(graph: &Webgraph, num_proxy_nodes: usize) -> Self {
        let betweenness = Betweenness::calculate_with_progress(graph);

        let mut nodes = betweenness.centrality.iter().collect_vec();
        nodes.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap_or(std::cmp::Ordering::Equal));

        let mut node2id: HashMap<Node, NodeID> = HashMap::new();

        if let Some(store) = graph.host.as_ref() {
            for id in store.nodes() {
                if let Some(node) = store.id2node(&id) {
                    node2id.insert(node, id);
                }
            }
        }

        let proxy_nodes: Vec<_> = nodes
            .into_par_iter()
            .take(num_proxy_nodes)
            .map(|(node, _)| node)
            .filter(|node| node2id.contains_key(node))
            .map(|node| {
                let (ids, dist) = graph.raw_host_distances(node.clone()).into_iter().unzip();
                let (rev_ids, rev_dist) = graph
                    .raw_host_reversed_distances(node.clone())
                    .into_iter()
                    .unzip();

                ProxyNode {
                    id: *node2id.get(node).unwrap(),
                    dist_to_node: BoomHashMap::new(ids, dist),
                    dist_from_node: BoomHashMap::new(rev_ids, rev_dist),
                }
            })
            .collect();

        Self {
            proxy_nodes,
            betweenness: betweenness
                .centrality
                .into_iter()
                .filter_map(|(node, centrality)| node2id.get(&node).map(|id| (*id, centrality)))
                .collect(),
            node2id,
        }
    }

    pub fn scorer(&self, liked_nodes: &[Node]) -> Scorer<'_> {
        let liked_nodes = liked_nodes
            .iter()
            .map(|node| node.clone().into_host())
            .filter_map(|node| self.node2id.get(&node).copied())
            .collect_vec();

        let fixed_scores: HashMap<_, _> = liked_nodes.iter().map(|node| (*node, 1.0)).collect();

        Scorer::new(
            &self.proxy_nodes,
            &liked_nodes,
            fixed_scores,
            &self.betweenness,
        )
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
        let file = File::open(path)?;
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
        // Liked nodes: E, C
        let graph = test_graph();
        let centrality = ApproximatedHarmonicCentrality::new_with_num_proxy(&graph, 3);
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
    fn liked_nodes_centrality() {
        let graph = test_graph();
        let centrality = ApproximatedHarmonicCentrality::new_with_num_proxy(&graph, 3);

        let liked_nodes = vec![Node::from("D".to_string()), Node::from("E".to_string())];

        let scorer = centrality.scorer(&liked_nodes);

        for node in &liked_nodes {
            assert_eq!(scorer.score(*centrality.node2id.get(node).unwrap()), 1.0);
        }
    }

    #[test]
    fn ordering() {
        let graph = test_graph();
        let centrality = ApproximatedHarmonicCentrality::new_with_num_proxy(&graph, 3);

        let liked_nodes = vec![Node::from("B".to_string()), Node::from("E".to_string())];

        let scorer = centrality.scorer(&liked_nodes);

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
