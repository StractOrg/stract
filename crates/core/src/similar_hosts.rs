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

use std::{cmp::Reverse, collections::BinaryHeap, sync::Arc};

use fnv::{FnvHashMap, FnvHashSet};
use hashbrown::HashSet;
use itertools::Itertools;
use url::Url;

use crate::{
    ranking::inbound_similarity::InboundSimilarity,
    webgraph::{remote::RemoteWebgraph, Node, NodeID},
    webpage::url_ext::UrlExt,
};

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug)]
pub struct ScoredNode {
    pub node: Node,
    pub score: f64,
}

struct ScoredNodeID {
    node_id: NodeID,
    score: f64,
}

impl PartialOrd for ScoredNodeID {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ScoredNodeID {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl Ord for ScoredNodeID {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score.total_cmp(&other.score)
    }
}

impl Eq for ScoredNodeID {}

pub struct SimilarHostsFinder {
    webgraph: Arc<RemoteWebgraph>,
    inbound_similarity: InboundSimilarity,
    max_similar_hosts: usize,
}

impl SimilarHostsFinder {
    pub fn new(
        webgraph: Arc<RemoteWebgraph>,
        inbound_similarity: InboundSimilarity,
        max_similar_hosts: usize,
    ) -> Self {
        Self {
            webgraph,
            inbound_similarity,
            max_similar_hosts,
        }
    }

    pub async fn find_similar_hosts(&self, nodes: &[String], limit: usize) -> Vec<ScoredNode> {
        const DEAD_LINKS_BUFFER: usize = 30;
        let orig_limit = limit.min(self.max_similar_hosts);
        let limit = orig_limit + nodes.len() + DEAD_LINKS_BUFFER;

        let nodes: Vec<_> = nodes
            .iter()
            .map(|url| Node::from(url.to_string()).into_host())
            .collect();

        let domains = nodes
            .iter()
            .filter_map(|node| {
                Url::parse(&format!("http://{}", &node.as_str()))
                    .ok()
                    .and_then(|url| url.root_domain().map(|d| d.to_string()))
            })
            .collect::<HashSet<_>>();

        let nodes = nodes.into_iter().map(|node| node.id()).collect::<Vec<_>>();

        let mut scorer = self.inbound_similarity.scorer(&nodes, &[], true);

        let mut backlinks = FnvHashMap::default();

        let in_edges = self
            .webgraph
            .batch_raw_ingoing_edges(&nodes)
            .await
            .unwrap_or_default();

        for edge in in_edges.into_iter().flatten() {
            backlinks
                .entry(edge.from)
                .or_insert_with(|| scorer.score(&edge.from));
        }

        let mut top_backlink_nodes: Vec<_> = backlinks
            .into_iter()
            .filter(|(_, score)| score.is_finite())
            .collect();

        top_backlink_nodes
            .sort_unstable_by(|(_, score_a), (_, score_b)| score_a.total_cmp(score_b));
        top_backlink_nodes.reverse();

        let mut scored_nodes = BinaryHeap::with_capacity(limit);
        let mut checked_nodes = FnvHashSet::default();

        let backlink_nodes = top_backlink_nodes
            .iter()
            .map(|(node, _)| *node)
            .take(limit)
            .collect::<Vec<_>>();
        let outgoing_edges = self
            .webgraph
            .batch_raw_outgoing_edges(&backlink_nodes)
            .await
            .unwrap_or_default();

        for edge in outgoing_edges.into_iter().flatten() {
            let potential_node = edge.to;

            if checked_nodes.contains(&potential_node) {
                continue;
            }

            checked_nodes.insert(potential_node);

            let score = scorer.score(&potential_node);
            let scored_node_id = ScoredNodeID {
                node_id: potential_node,
                score,
            };

            if scored_nodes.len() < limit {
                scored_nodes.push(Reverse(scored_node_id));
            } else {
                let mut min_scored_node = scored_nodes.peek_mut().unwrap();

                if scored_node_id > min_scored_node.0 {
                    *min_scored_node = Reverse(scored_node_id);
                }
            }
        }

        let mut scored_nodes: Vec<_> = scored_nodes.into_iter().take(limit).map(|n| n.0).collect();
        scored_nodes.sort_unstable();
        scored_nodes.reverse();

        let potential_nodes = scored_nodes
            .iter()
            .map(|ScoredNodeID { node_id, score: _ }| *node_id)
            .collect::<Vec<_>>();

        // remove dead links (nodes without outgoing edges might be dead links)
        let known_nodes = self
            .webgraph
            .batch_raw_ingoing_edges(&potential_nodes)
            .await
            .unwrap_or_default();

        let (potential_nodes, scores): (Vec<_>, Vec<_>) = scored_nodes
            .into_iter()
            .zip_eq(known_nodes)
            .filter_map(|(s, e)| if e.is_empty() { None } else { Some(s) })
            .map(|ScoredNodeID { node_id, score }| (node_id, score))
            .unzip();

        let nodes = self
            .webgraph
            .batch_get_node(&potential_nodes)
            .await
            .unwrap_or_default();

        nodes
            .into_iter()
            .zip_eq(scores)
            .filter_map(|(node, score)| {
                let node = node.unwrap();
                match Url::parse(&format!("http://{}", &node.as_str()))
                    .ok()
                    .and_then(|url| url.root_domain().map(|s| s.to_string()))
                {
                    Some(dom) => {
                        if !domains.contains(&dom) {
                            Some(ScoredNode { node, score })
                        } else {
                            None
                        }
                    }
                    None => None,
                }
            })
            .take(orig_limit)
            .collect()
    }
}
