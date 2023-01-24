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

use std::cmp;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read};
use std::path::Path;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::intmap::IntMap;
use crate::ranking::centrality_store::HarmonicCentralityStore;
use crate::webgraph::{Edge, Node};
use crate::webgraph::{NodeID, Webgraph};
use crate::Result;

const NUM_PROXY_NODES: usize = 500;
const BEST_PROXY_NODES_PER_USER_NODE: usize = 3;
const USER_NODES_LIMIT: usize = 100; // if the user specifies more than this number of nodes, the remaining nodes will be merged into existing
const MAX_DIST_PROXY: u8 = 3;
const MAX_NUM_DISTANCE_NODES: usize = 10_000;
pub const SHIFT: f64 = 1.0;

#[derive(Serialize, Deserialize)]
pub struct ProxyNode {
    pub id: NodeID,
    dist_from_node: IntMap<u8>, // from node to proxy node
    dist_to_node: IntMap<u8>,   // from proxy node to other node
}

impl ProxyNode {
    fn dist(&self, from: &NodeID, to: &NodeID) -> Option<u8> {
        if let Some(from_node_to_proxy) = self.dist_from_node.get(&from.0) {
            if let Some(from_proxy_to_node) = self.dist_to_node.get(&to.0) {
                return Some(from_node_to_proxy + from_proxy_to_node);
            }
        }

        None
    }
}

struct WeightedProxyNode {
    node: Arc<ProxyNode>,
    dist: u8,
}

impl PartialOrd for WeightedProxyNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.dist.partial_cmp(&other.dist)
    }
}
impl Ord for WeightedProxyNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl PartialEq for WeightedProxyNode {
    fn eq(&self, other: &Self) -> bool {
        self.dist == other.dist
    }
}

impl Eq for WeightedProxyNode {}

pub struct Scorer {
    fixed_scores: HashMap<NodeID, f64>,
    liked_nodes: Vec<UserNode>,
    disliked_nodes: Vec<UserNode>,
    cache: Mutex<HashMap<NodeID, f64>>,
    num_liked_nodes: usize,
}

fn create_user_nodes(nodes: &[NodeID], proxy_nodes: &[Arc<ProxyNode>]) -> Vec<UserNode> {
    let mut res = Vec::new();

    let nodes = nodes.to_vec();

    for id in nodes {
        let user_node = UserNode::new(id, proxy_nodes);
        if res.len() < USER_NODES_LIMIT {
            res.push(user_node);
        } else {
            let mut best = res
                .iter_mut()
                .min_by_key(|curr| user_node.best_dist(&curr.id).unwrap_or(255))
                .unwrap();

            best.weight += 1;
        }
    }

    res
}

impl Scorer {
    fn new(
        proxy_nodes: &[Arc<ProxyNode>],
        liked_nodes: &[NodeID],
        disliked_nodes: &[NodeID],
        fixed_scores: HashMap<NodeID, f64>,
    ) -> Self {
        let num_liked_nodes = liked_nodes.len();

        Self {
            fixed_scores,
            num_liked_nodes,
            liked_nodes: create_user_nodes(liked_nodes, proxy_nodes),
            disliked_nodes: create_user_nodes(disliked_nodes, proxy_nodes),
            cache: Mutex::new(HashMap::new()),
        }
    }

    pub fn score(&self, node: NodeID) -> f64 {
        if let Some(score) = self.fixed_scores.get(&node) {
            *score
        } else {
            if let Some(cached) = self.cache.lock().unwrap().get(&node) {
                return *cached;
            }

            let res = (SHIFT
                + (self
                    .liked_nodes
                    .iter()
                    .filter_map(|liked_node| {
                        liked_node.best_dist(&node).map(|dist| (dist, liked_node))
                    })
                    .map(|(dist, liked_node)| liked_node.weight as f64 / (dist as f64 + 1.0))
                    .sum::<f64>()
                    - self
                        .disliked_nodes
                        .iter()
                        .filter_map(|disliked_node| {
                            disliked_node
                                .best_dist(&node)
                                .map(|dist| (dist, disliked_node))
                        })
                        .map(|(dist, disliked_node)| {
                            disliked_node.weight as f64 / (dist as f64 + 1.0)
                        })
                        .sum::<f64>())
                    / self.num_liked_nodes as f64)
                .max(0.0);

            self.cache.lock().unwrap().insert(node, res);

            res
        }
    }
}

struct UserNode {
    id: NodeID,
    proxy_nodes: Vec<Arc<ProxyNode>>,
    weight: usize,
}

impl UserNode {
    fn new(id: NodeID, proxy_nodes: &[Arc<ProxyNode>]) -> Self {
        let mut heap = BinaryHeap::with_capacity(BEST_PROXY_NODES_PER_USER_NODE);
        for proxy in proxy_nodes {
            if let Some(dist_to_proxy) = proxy.dist_from_node.get(&id.0) {
                let weighted_node = WeightedProxyNode {
                    node: Arc::clone(proxy),
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

    fn best_dist(&self, node: &NodeID) -> Option<u8> {
        let mut best = None;

        for proxy in &self.proxy_nodes {
            if let Some(dist) = proxy.dist(&self.id, node) {
                best = match best {
                    Some((best_dist, best_proxy)) => {
                        if dist < best_dist {
                            Some((dist, proxy))
                        } else {
                            Some((best_dist, best_proxy))
                        }
                    }
                    None => Some((dist, proxy)),
                }
            }
        }

        best.map(|(dist, _)| dist)
    }
}

struct ProxyNodeCandidate {
    node: Node,
    score: f64,
}

impl PartialOrd for ProxyNodeCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.score.partial_cmp(&other.score)
    }
}

impl PartialEq for ProxyNodeCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl Ord for ProxyNodeCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl Eq for ProxyNodeCandidate {}

fn modified_dijkstra<F1, F2>(
    source: Node,
    node_edges: F1,
    edge_node: F2,
    graph: &Webgraph,
) -> BTreeMap<NodeID, u8>
where
    F1: Fn(NodeID) -> Vec<Edge>,
    F2: Fn(&Edge) -> NodeID,
{
    let source_id = graph.node2id(&source);
    if source_id.is_none() {
        return BTreeMap::new();
    }

    let source_id = source_id.unwrap();
    let mut distances: BTreeMap<NodeID, u8> = BTreeMap::default();

    let mut queue = BinaryHeap::new();

    queue.push(cmp::Reverse((0, source_id)));
    distances.insert(source_id, 0);

    while let Some(state) = queue.pop() {
        let (cost, v) = state.0;

        if cost >= MAX_DIST_PROXY {
            continue;
        }

        let current_dist = distances.get(&v).unwrap_or(&u8::MAX);

        if cost > *current_dist {
            continue;
        }

        for edge in node_edges(v) {
            if cost + 1 < *distances.get(&edge_node(&edge)).unwrap_or(&u8::MAX) {
                let d = cost + 1;

                if d > MAX_DIST_PROXY {
                    continue;
                }

                let next = cmp::Reverse((d, edge_node(&edge)));
                queue.push(next);
                distances.insert(edge_node(&edge), d);
            }
        }

        if distances.len() > MAX_NUM_DISTANCE_NODES {
            break;
        }
    }

    distances
}

fn distances(graph: &Webgraph, source: Node) -> BTreeMap<NodeID, u8> {
    modified_dijkstra(
        source,
        |node| graph.raw_outgoing_edges(&node),
        |edge| edge.to,
        graph,
    )
}

fn reversed_distances(graph: &Webgraph, source: Node) -> BTreeMap<NodeID, u8> {
    modified_dijkstra(
        source,
        |node| graph.raw_ingoing_edges(&node),
        |edge| edge.to,
        graph,
    )
}

#[derive(Serialize, Deserialize, Default)]
pub struct OnlineHarmonicCentrality {
    pub proxy_nodes: Vec<Arc<ProxyNode>>,
}

impl OnlineHarmonicCentrality {
    pub fn new(graph: &Webgraph, centrality: &HarmonicCentralityStore) -> Self {
        Self::new_with_num_proxy(graph, centrality, NUM_PROXY_NODES)
    }

    fn new_with_num_proxy(
        graph: &Webgraph,
        centrality: &HarmonicCentralityStore,
        num_proxy_nodes: usize,
    ) -> Self {
        let mut node2id: BTreeMap<Node, NodeID> = BTreeMap::new();

        // we should probably choose the proxy nodes based on their
        // betweenness centrality, but I don't know how we can approximate betweenness
        // on a graph that cannot be in memory.
        // For now, we will choose the nodes with highest harmonic centrality
        // as an estimate

        let mut nodes: BinaryHeap<ProxyNodeCandidate> = BinaryHeap::new();
        for (node, id) in graph.node_ids() {
            node2id.insert(node.clone(), id);

            let score = centrality.host.get(&node.name).unwrap_or(0.0);
            let candidate = ProxyNodeCandidate { node, score };

            if nodes.len() >= num_proxy_nodes {
                if let Some(mut existing_node) = nodes.peek_mut() {
                    if candidate.score > existing_node.score {
                        *existing_node = candidate;
                    }
                }
            } else {
                nodes.push(candidate);
            }
        }

        let proxy_nodes: Vec<_> = nodes
            .into_iter()
            .map(|candidate| candidate.node)
            .filter(|node| node2id.contains_key(node))
            .take(num_proxy_nodes)
            .map(|node| {
                let dist_to_node = distances(graph, node.clone())
                    .into_iter()
                    .map(|(n, v)| (n.0, v))
                    .collect();

                let dist_from_node = reversed_distances(graph, node.clone())
                    .into_iter()
                    .map(|(n, v)| (n.0, v))
                    .collect();

                Arc::new(ProxyNode {
                    id: *node2id.get(&node).unwrap(),
                    dist_to_node,
                    dist_from_node,
                })
            })
            .collect();

        Self { proxy_nodes }
    }

    pub fn scorer(&self, liked_nodes: &[NodeID], disliked_nodes: &[NodeID]) -> Scorer {
        Scorer::new(
            &self.proxy_nodes,
            liked_nodes,
            disliked_nodes,
            HashMap::new(),
        )
    }

    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let mut file = BufWriter::new(
            File::options()
                .create(true)
                .truncate(true)
                .write(true)
                .open(path)?,
        );

        bincode::serialize_into(&mut file, &self)?;

        Ok(())
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
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
    use crate::webgraph::{centrality::harmonic::HarmonicCentrality, WebgraphBuilder};

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

        let mut graph = WebgraphBuilder::new_memory().open();

        graph.insert(Node::from("A"), Node::from("D"), String::new());
        graph.insert(Node::from("B"), Node::from("D"), String::new());
        graph.insert(Node::from("C"), Node::from("D"), String::new());
        graph.insert(Node::from("D"), Node::from("E"), String::new());
        graph.insert(Node::from("E"), Node::from("H"), String::new());
        graph.insert(Node::from("H"), Node::from("C"), String::new());
        graph.insert(Node::from("F"), Node::from("E"), String::new());
        graph.insert(Node::from("F"), Node::from("G"), String::new());
        graph.insert(Node::from("G"), Node::from("E"), String::new());

        graph.commit();

        graph
    }

    #[test]
    fn ordering() {
        let graph = test_graph();
        let harmonic = HarmonicCentrality::calculate(&graph);

        let harmonic_centrality_store = HarmonicCentralityStore::open(crate::gen_temp_path());
        for (node, centrality) in harmonic.host {
            harmonic_centrality_store.host.insert(node.name, centrality);
        }
        harmonic_centrality_store.host.flush();

        let centrality =
            OnlineHarmonicCentrality::new_with_num_proxy(&graph, &harmonic_centrality_store, 5);

        let liked_nodes: Vec<_> = vec![Node::from("B".to_string()), Node::from("E".to_string())]
            .into_iter()
            .filter_map(|node| graph.node2id(&node))
            .collect();

        let scorer = centrality.scorer(&liked_nodes, &[]);

        assert!(
            scorer.score(graph.node2id(&Node::from("E".to_string())).unwrap())
                > scorer.score(graph.node2id(&Node::from("H".to_string())).unwrap())
        );
        assert!(
            scorer.score(graph.node2id(&Node::from("H".to_string())).unwrap())
                > scorer.score(graph.node2id(&Node::from("C".to_string())).unwrap())
        );

        assert!(
            scorer.score(graph.node2id(&Node::from("C".to_string())).unwrap())
                > scorer.score(graph.node2id(&Node::from("A".to_string())).unwrap())
        );
    }

    #[test]
    fn disliked_nodes_centrality() {
        let graph = test_graph();
        let harmonic = HarmonicCentrality::calculate(&graph);

        let harmonic_centrality_store = HarmonicCentralityStore::open(crate::gen_temp_path());
        for (node, centrality) in harmonic.host {
            harmonic_centrality_store.host.insert(node.name, centrality);
        }
        harmonic_centrality_store.host.flush();

        let centrality =
            OnlineHarmonicCentrality::new_with_num_proxy(&graph, &harmonic_centrality_store, 5);

        let disliked_nodes: Vec<_> = vec![Node::from("D".to_string()), Node::from("E".to_string())]
            .into_iter()
            .filter_map(|node| graph.node2id(&node))
            .collect();

        let scorer = centrality.scorer(&[], &disliked_nodes);

        for node in &disliked_nodes {
            assert_eq!(scorer.score(*node), 0.0);
        }
    }

    #[test]
    fn ordering_with_dislikes() {
        let graph = test_graph();
        let harmonic = HarmonicCentrality::calculate(&graph);

        let harmonic_centrality_store = HarmonicCentralityStore::open(crate::gen_temp_path());
        for (node, centrality) in harmonic.host {
            harmonic_centrality_store.host.insert(node.name, centrality);
        }
        harmonic_centrality_store.host.flush();

        let centrality =
            OnlineHarmonicCentrality::new_with_num_proxy(&graph, &harmonic_centrality_store, 5);

        let liked_nodes: Vec<_> = vec![Node::from("B".to_string()), Node::from("E".to_string())]
            .into_iter()
            .filter_map(|node| graph.node2id(&node))
            .collect();

        let disliked_nodes: Vec<_> = vec![Node::from("F".to_string())]
            .into_iter()
            .filter_map(|node| graph.node2id(&node))
            .collect();

        let scorer = centrality.scorer(&liked_nodes, &disliked_nodes);

        assert!(
            scorer.score(graph.node2id(&Node::from("E".to_string())).unwrap())
                > scorer.score(graph.node2id(&Node::from("H".to_string())).unwrap())
        );

        assert!(
            scorer.score(graph.node2id(&Node::from("H".to_string())).unwrap())
                > scorer.score(graph.node2id(&Node::from("C".to_string())).unwrap())
        );

        assert!(
            scorer.score(graph.node2id(&Node::from("C".to_string())).unwrap())
                > scorer.score(graph.node2id(&Node::from("A".to_string())).unwrap())
        );
    }
}
