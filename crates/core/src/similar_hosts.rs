// Stract is an open source web search engine.
// Copyright (C) 2024 Stract ApS
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

use std::{cmp::Reverse, sync::Arc};

use fnv::FnvHashMap;
use hashbrown::HashSet;
use itertools::Itertools;
use url::Url;

use crate::{
    ranking::{bitvec_similarity, inbound_similarity},
    webgraph::{
        query::{
            HostBacklinksQuery, HostForwardlinksQuery, Id2NodeQuery, NotFilter, OrFilter,
            RelFlagsFilter, TextFilter,
        },
        remote::RemoteWebgraph,
        EdgeLimit, Node, NodeID, SmallEdge,
    },
    webpage::{html::links::RelFlags, url_ext::UrlExt},
    SortableFloat,
};

const NUM_BACKLINK_APPROXIMATION_THRESHOLD: usize = 32;
const NUM_BACKLINK_APPROXIMATION_FRACTION: f64 = 0.25;
const APPROXIMATION_CANDIDATES: usize = 256;
const CANDIDATES_LIMIT: usize = 1024;

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug)]
pub struct ScoredNode {
    pub node: Node,
    pub score: f64,
}

pub struct SimilarHostsFinder {
    webgraph: Arc<RemoteWebgraph>,
    max_similar_hosts: usize,
}

impl SimilarHostsFinder {
    pub fn new(webgraph: Arc<RemoteWebgraph>, max_similar_hosts: usize) -> Self {
        Self {
            webgraph,
            max_similar_hosts,
        }
    }

    async fn scorer(&self, liked: &[NodeID]) -> inbound_similarity::Scorer {
        inbound_similarity::Scorer::new(&self.webgraph, liked, &[], true).await
    }

    async fn batch_ingoing_edges(&self, nodes: &[NodeID], limit: EdgeLimit) -> Vec<Vec<SmallEdge>> {
        let queries: Vec<_> = nodes
            .iter()
            .map(|n| {
                HostBacklinksQuery::new(*n)
                    .with_limit(limit)
                    .filter(NotFilter::new(RelFlagsFilter::from(RelFlags::NOFOLLOW)))
            })
            .collect();

        self.webgraph
            .batch_search(queries)
            .await
            .unwrap_or_default()
    }

    async fn batch_outgoing_edges(
        &self,
        nodes: &[NodeID],
        limit: EdgeLimit,
        filters: &[String],
    ) -> Vec<Vec<SmallEdge>> {
        let queries: Vec<_> = nodes
            .iter()
            .map(|n| {
                let mut query = HostForwardlinksQuery::new(*n)
                    .with_limit(limit)
                    .filter(NotFilter::new(RelFlagsFilter::from(RelFlags::NOFOLLOW)));

                if !filters.is_empty() {
                    let mut or_filter = OrFilter::new();

                    for filter in filters {
                        or_filter = or_filter.or(TextFilter::new(
                            filter.clone(),
                            crate::webgraph::schema::ToUrl,
                        ));
                    }

                    query = query.filter(or_filter);
                }

                query
            })
            .collect();

        self.webgraph
            .batch_search(queries)
            .await
            .unwrap_or_default()
    }

    async fn potential_nodes(&self, nodes: &[NodeID], filters: &[String]) -> Vec<NodeID> {
        let in_edges = self.batch_ingoing_edges(nodes, EdgeLimit::Limit(128)).await;

        let backlink_nodes = in_edges
            .iter()
            .flatten()
            .filter(|e| !e.rel_flags.contains(RelFlags::NOFOLLOW))
            .map(|e| e.from)
            .unique()
            .collect::<Vec<_>>();

        let num_backlink_nodes = backlink_nodes.len();

        let outgoing_edges = self
            .batch_outgoing_edges(&backlink_nodes, EdgeLimit::Limit(512), filters)
            .await;

        let mut counts = FnvHashMap::default();

        for e in outgoing_edges
            .iter()
            .flatten()
            .filter(|e| !e.rel_flags.contains(RelFlags::NOFOLLOW))
        {
            *counts.entry(e.to).or_insert(0) += 1;
        }

        let apply_filter = num_backlink_nodes > NUM_BACKLINK_APPROXIMATION_THRESHOLD;
        let num_candidates = if apply_filter {
            APPROXIMATION_CANDIDATES
        } else {
            CANDIDATES_LIMIT
        };

        counts
            .into_iter()
            .filter(|(_, c)| {
                !apply_filter
                    || (*c as usize)
                        <= (num_backlink_nodes as f64 * NUM_BACKLINK_APPROXIMATION_FRACTION).ceil()
                            as usize
            })
            .sorted_by_key(|(_, c)| Reverse(*c))
            .map(|(n, _)| n)
            .take(num_candidates)
            .filter(|n| !nodes.contains(n))
            .collect()
    }

    async fn scored_nodes(
        &self,
        nodes: &[NodeID],
        limit: usize,
        filters: &[String],
    ) -> Vec<(NodeID, SortableFloat)> {
        let mut scorer = self.scorer(nodes).await;
        let potential_nodes = self.potential_nodes(nodes, filters).await;

        let inbounds = bitvec_similarity::BitVec::batch_new_for(&potential_nodes, &self.webgraph)
            .await
            .into_iter()
            .zip_eq(potential_nodes.into_iter())
            .map(|(b, n)| (n, b));

        crate::sorted_k(
            inbounds.map(|(n, b)| {
                let score = scorer.score(&n, &b);
                Reverse((SortableFloat(score), n))
            }),
            limit,
        )
        .into_iter()
        .map(|Reverse((score, n))| (n, score))
        .collect()
    }

    pub async fn find_similar_hosts(
        &self,
        nodes: Vec<String>,
        limit: usize,
        filters: Vec<String>,
    ) -> Vec<ScoredNode> {
        const DEAD_LINKS_BUFFER: usize = 30;
        let orig_limit = limit.min(self.max_similar_hosts);
        let limit = orig_limit + nodes.len() + DEAD_LINKS_BUFFER;

        let nodes: Vec<_> = nodes
            .iter()
            .filter_map(|url| Url::robust_parse(url).ok())
            .map(|url| Node::from(url).into_host())
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

        let scored_nodes = self.scored_nodes(&nodes, limit, &filters).await;

        let potential_nodes = scored_nodes
            .iter()
            .map(|(node_id, _)| *node_id)
            .collect::<Vec<_>>();

        // remove dead links (nodes without outgoing edges might be dead links)
        let known_nodes = self
            .batch_ingoing_edges(&potential_nodes, EdgeLimit::Limit(1))
            .await;

        let (potential_nodes, scores): (Vec<_>, Vec<_>) = scored_nodes
            .into_iter()
            .zip_eq(known_nodes)
            .filter_map(|(s, e)| if e.is_empty() { None } else { Some(s) })
            .unzip();

        let nodes = self
            .webgraph
            .batch_search(
                potential_nodes
                    .into_iter()
                    .map(Id2NodeQuery::Host)
                    .collect(),
            )
            .await
            .unwrap_or_default();

        nodes
            .into_iter()
            .zip_eq(scores)
            .filter_map(|(node, SortableFloat(score))| {
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
