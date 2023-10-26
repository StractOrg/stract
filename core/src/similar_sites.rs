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

use hashbrown::HashSet;
use url::Url;

use crate::{
    intmap::{IntMap, IntSet},
    ranking::inbound_similarity::InboundSimilarity,
    webgraph::{Node, NodeID, Webgraph},
    webpage::url_ext::UrlExt,
};

#[derive(serde::Serialize, serde::Deserialize, Debug)]
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

pub struct SimilarSitesFinder {
    webgraph: Arc<Webgraph>,
    inbound_similarity: InboundSimilarity,
    max_similar_sites: usize,
}

impl SimilarSitesFinder {
    pub fn new(
        webgraph: Arc<Webgraph>,
        inbound_similarity: InboundSimilarity,
        max_similar_sites: usize,
    ) -> Self {
        Self {
            webgraph,
            inbound_similarity,
            max_similar_sites,
        }
    }

    pub fn find_similar_sites(&self, nodes: &[String], limit: usize) -> Vec<ScoredNode> {
        const SIMILAR_DOMAINS_BUFFER: usize = 30;
        let orig_limit = limit.min(self.max_similar_sites);
        let limit = orig_limit + nodes.len() + SIMILAR_DOMAINS_BUFFER;

        let nodes: Vec<_> = nodes
            .iter()
            .map(|url| Node::from(url.to_string()).into_host())
            .collect();

        let domains = nodes
            .iter()
            .filter_map(|node| {
                Url::parse(&format!("http://{}", &node.name))
                    .ok()
                    .and_then(|url| url.root_domain().map(|d| d.to_string()))
            })
            .collect::<HashSet<_>>();

        let nodes = nodes.into_iter().map(|node| node.id()).collect::<Vec<_>>();

        let scorer = self.inbound_similarity.scorer(&nodes, &[], true);

        let mut backlinks: IntMap<NodeID, f64> = IntMap::new();

        for node in &nodes {
            for edge in self.webgraph.raw_ingoing_edges(node) {
                if !backlinks.contains_key(&edge.from) {
                    backlinks.insert(edge.from, scorer.score(&edge.from));
                }
            }
        }

        let mut top_backlink_nodes: Vec<_> = backlinks
            .into_iter()
            .filter(|(_, score)| score.is_finite())
            .collect();

        top_backlink_nodes
            .sort_unstable_by(|(_, score_a), (_, score_b)| score_a.total_cmp(score_b));
        top_backlink_nodes.reverse();

        let mut scored_nodes = BinaryHeap::with_capacity(limit);
        let mut checked_nodes = IntSet::new();

        for (backlink_node, _) in top_backlink_nodes.into_iter().take(limit) {
            for edge in self.webgraph.raw_outgoing_edges(&backlink_node) {
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
        }

        let mut scored_nodes: Vec<_> = scored_nodes.into_iter().take(limit).map(|n| n.0).collect();
        scored_nodes.sort_unstable();
        scored_nodes.reverse();

        scored_nodes
            .into_iter()
            .filter_map(|ScoredNodeID { node_id, score }| {
                let node = self.webgraph.id2node(&node_id).unwrap();
                match Url::parse(&format!("http://{}", &node.name))
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

    pub fn knows_about(&self, node: &Node) -> bool {
        self.inbound_similarity.knows_about(node.id())
    }
}
