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
    use optics::HostRankings;

    use crate::{
        bangs::Bangs,
        gen_temp_path,
        index::Index,
        rand_words,
        searcher::{
            api::ApiSearcher, live::LiveSearcher, LocalSearchClient, LocalSearcher, SearchQuery,
        },
        webgraph::{EdgeLimit, Node, Webgraph, WebgraphWriter},
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
        let mut wrt = WebgraphWriter::new(
            gen_temp_path(),
            crate::executor::Executor::single_thread(),
            crate::webgraph::Compression::default(),
            None,
        );

        wrt.insert(
            Node::from("a.com"),
            Node::from("b.com"),
            String::new(),
            RelFlags::default(),
        );
        wrt.insert(
            Node::from("c.com"),
            Node::from("d.com"),
            String::new(),
            RelFlags::default(),
        );
        wrt.insert(
            Node::from("a.com"),
            Node::from("e.com"),
            String::new(),
            RelFlags::default(),
        );

        wrt.insert(
            Node::from("z.com"),
            Node::from("a.com"),
            String::new(),
            RelFlags::default(),
        );
        wrt.insert(
            Node::from("z.com"),
            Node::from("b.com"),
            String::new(),
            RelFlags::default(),
        );
        wrt.insert(
            Node::from("z.com"),
            Node::from("c.com"),
            String::new(),
            RelFlags::default(),
        );
        wrt.insert(
            Node::from("z.com"),
            Node::from("d.com"),
            String::new(),
            RelFlags::default(),
        );
        wrt.insert(
            Node::from("z.com"),
            Node::from("e.com"),
            String::new(),
            RelFlags::default(),
        );

        let graph = wrt.finalize();

        let mut scorer = Scorer::new(&graph, &[Node::from("b.com").id()], &[], false).await;
        let e = Node::from("e.com").id();
        let d = Node::from("d.com").id();

        assert!(scorer.score(&e, &inbound(&graph, &e)) > scorer.score(&d, &inbound(&graph, &d)));
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn it_ranks_search_results() {
        let mut wrt = WebgraphWriter::new(
            crate::gen_temp_path(),
            crate::executor::Executor::single_thread(),
            crate::webgraph::Compression::default(),
            None,
        );

        wrt.insert(
            Node::from("b.com"),
            Node::from("a.com"),
            String::new(),
            RelFlags::default(),
        );
        wrt.insert(
            Node::from("c.com"),
            Node::from("d.com"),
            String::new(),
            RelFlags::default(),
        );
        wrt.insert(
            Node::from("b.com"),
            Node::from("e.com"),
            String::new(),
            RelFlags::default(),
        );
        wrt.insert(
            Node::from("c.com"),
            Node::from("b.com"),
            String::new(),
            RelFlags::default(),
        );

        let graph = wrt.finalize();

        let mut index = Index::temporary().expect("Unable to open index");

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

        let searcher: ApiSearcher<_, LiveSearcher, _> = ApiSearcher::new(
            LocalSearchClient::from(LocalSearcher::new(index)),
            Bangs::empty(),
            crate::searcher::api::Config::default(),
        )
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
