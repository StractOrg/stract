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
// along with this program. If not, see <https://www.gnu.org/licenses/>.

use fnv::FnvHashMap as HashMap;
use itertools::Itertools;

use crate::webgraph::NodeID;

use super::bitvec_similarity;

#[derive(Clone)]
struct NodeScorer {
    node: NodeID,
    inbound: bitvec_similarity::BitVec,
    self_score: f64,
}

impl NodeScorer {
    fn new(node: NodeID, inbound: bitvec_similarity::BitVec) -> Self {
        Self {
            node,
            inbound,
            self_score: 1.0,
        }
    }

    fn set_self_score(&mut self, self_score: f64) {
        self.self_score = self_score;
    }

    fn sim(&self, other: &NodeID, other_inbound: &bitvec_similarity::BitVec) -> f64 {
        if self.node == *other {
            self.self_score
        } else {
            self.inbound.sim(other_inbound)
        }
    }
}

#[derive(Clone)]
pub struct Scorer {
    liked: Vec<NodeScorer>,
    disliked: Vec<NodeScorer>,
    cache: HashMap<NodeID, f64>,
    normalized: bool,
}

impl Scorer {
    pub fn empty() -> Self {
        Self {
            liked: Vec::new(),
            disliked: Vec::new(),
            cache: HashMap::default(),
            normalized: false,
        }
    }

    pub async fn new<G: bitvec_similarity::Graph>(
        graph: &G,
        liked_hosts: &[NodeID],
        disliked_hosts: &[NodeID],
        normalized: bool,
    ) -> Scorer {
        let liked = bitvec_similarity::BitVec::batch_new_for(liked_hosts, graph).await;
        let disliked = bitvec_similarity::BitVec::batch_new_for(disliked_hosts, graph).await;

        let liked: Vec<_> = liked_hosts
            .iter()
            .zip_eq(liked)
            .map(|(node, inbound)| NodeScorer::new(*node, inbound))
            .collect();

        let disliked: Vec<_> = disliked_hosts
            .iter()
            .zip_eq(disliked)
            .map(|(node, inbound)| NodeScorer::new(*node, inbound))
            .collect();

        Scorer {
            liked,
            disliked,
            cache: HashMap::default(),
            normalized,
        }
    }
    fn calculate_score(&self, node: &NodeID, inbound: &bitvec_similarity::BitVec) -> f64 {
        let s = (self.disliked.len() as f64)
            + (self
                .liked
                .iter()
                .map(|liked| liked.sim(node, inbound))
                .sum::<f64>()
                - self
                    .disliked
                    .iter()
                    .map(|disliked| disliked.sim(node, inbound))
                    .sum::<f64>());

        if self.normalized {
            s / self.liked.len().max(1) as f64
        } else {
            s
        }
        .max(0.0)
    }
    pub fn score(&mut self, node: &NodeID, inbound: &bitvec_similarity::BitVec) -> f64 {
        if let Some(cached) = self.cache.get(node) {
            return *cached;
        }

        let score = self.calculate_score(node, inbound);
        self.cache.insert(*node, score);
        score
    }

    pub fn set_self_score(&mut self, self_score: f64) {
        for scorer in self.liked.iter_mut() {
            scorer.set_self_score(self_score);
        }

        for scorer in self.disliked.iter_mut() {
            scorer.set_self_score(self_score);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use optics::HostRankings;
    use tokio::sync::RwLock;

    use crate::{
        bangs::Bangs,
        index::Index,
        rand_words,
        searcher::{api::ApiSearcher, LocalSearchClient, LocalSearcher, SearchQuery},
        webgraph::{Edge, EdgeLimit, Node, Webgraph},
        webpage::{html::links::RelFlags, Html, Webpage},
    };

    use super::*;

    fn inbound(graph: &Webgraph, node: &NodeID) -> bitvec_similarity::BitVec {
        bitvec_similarity::BitVec::new(
            graph
                .raw_ingoing_edges(node, EdgeLimit::Unlimited)
                .into_iter()
                .map(|e| e.from.as_u64())
                .collect(),
        )
    }

    #[tokio::test]
    async fn it_favors_liked_hosts() {
        let dir = crate::gen_temp_dir().unwrap();
        let mut graph = Webgraph::open(&dir, 0u64.into()).unwrap();

        graph
            .insert(Edge {
                from: Node::from("a.com").into_host(),
                to: Node::from("b.com").into_host(),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();
        graph
            .insert(Edge {
                from: Node::from("c.com").into_host(),
                to: Node::from("d.com").into_host(),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();
        graph
            .insert(Edge {
                from: Node::from("a.com").into_host(),
                to: Node::from("e.com").into_host(),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();

        graph
            .insert(Edge {
                from: Node::from("z.com").into_host(),
                to: Node::from("a.com").into_host(),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();
        graph
            .insert(Edge {
                from: Node::from("z.com").into_host(),
                to: Node::from("b.com").into_host(),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();
        graph
            .insert(Edge {
                from: Node::from("z.com").into_host(),
                to: Node::from("c.com").into_host(),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();
        graph
            .insert(Edge {
                from: Node::from("z.com").into_host(),
                to: Node::from("d.com").into_host(),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();
        graph
            .insert(Edge {
                from: Node::from("z.com").into_host(),
                to: Node::from("d.com").into_host(),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();
        graph
            .insert(Edge {
                from: Node::from("z.com").into_host(),
                to: Node::from("e.com").into_host(),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();

        graph.commit().unwrap();

        let mut scorer = Scorer::new(&graph, &[Node::from("b.com").id()], &[], false).await;
        let e = Node::from("e.com").id();
        let d = Node::from("d.com").id();

        assert!(scorer.score(&e, &inbound(&graph, &e)) > scorer.score(&d, &inbound(&graph, &d)));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_ranks_search_results() {
        let dir = crate::gen_temp_dir().unwrap();
        let mut graph = Webgraph::open(&dir, 0u64.into()).unwrap();

        graph
            .insert(Edge {
                from: Node::from("b.com").into_host(),
                to: Node::from("a.com").into_host(),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();
        graph
            .insert(Edge {
                from: Node::from("c.com").into_host(),
                to: Node::from("d.com").into_host(),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();
        graph
            .insert(Edge {
                from: Node::from("b.com").into_host(),
                to: Node::from("e.com").into_host(),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();
        graph
            .insert(Edge {
                from: Node::from("c.com").into_host(),
                to: Node::from("b.com").into_host(),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();

        graph.commit().unwrap();

        let (mut index, _dir) = Index::temporary().expect("Unable to open index");

        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                    <html>
                        <head>
                            <title>Title</title>
                        </head>
                        <body>
                            example {}
                        </body>
                    </html>
                "#,
                        rand_words(1000)
                    ),
                    "https://e.com",
                )
                .unwrap(),
                fetch_time_ms: 500,
                node_id: Some(Node::from("e.com").id()),
                ..Default::default()
            })
            .expect("failed to insert webpage");
        index
            .insert(&Webpage {
                html: Html::parse(
                    &format!(
                        r#"
                    <html>
                        <head>
                            <title>Title</title>
                        </head>
                        <body>
                            example {}
                        </body>
                    </html>
                "#,
                        rand_words(1000)
                    ),
                    "https://d.com",
                )
                .unwrap(),
                host_centrality: 0.01,
                fetch_time_ms: 500,
                node_id: Some(Node::from("d.com").id()),
                ..Default::default()
            })
            .expect("failed to insert webpage");

        index.commit().unwrap();

        let searcher: ApiSearcher<_, _> = ApiSearcher::new(
            LocalSearchClient::from(LocalSearcher::builder(Arc::new(RwLock::new(index))).build()),
            None,
            Bangs::empty(),
            crate::searcher::api::Config::default(),
        )
        .await
        .with_webgraph(graph);

        let res = searcher
            .search(&SearchQuery {
                query: "example".to_string(),
                host_rankings: Some(HostRankings {
                    liked: vec!["a.com".to_string()],
                    disliked: vec![],
                    blocked: vec![],
                }),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_websites_result()
            .webpages;

        assert_eq!(res.len(), 2);
        assert_eq!(&res[0].url, "https://e.com/");
        assert_eq!(&res[1].url, "https://d.com/");
    }
}
