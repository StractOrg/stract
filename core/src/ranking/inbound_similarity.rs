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

use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    fs::File,
    io::{BufReader, BufWriter, Read},
    path::Path,
    sync::Arc,
};

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::{
    intmap::{IntMap, IntSet},
    webgraph::{NodeID, Webgraph},
    Result,
};

use super::{bitvec_similarity, centrality_store::HarmonicCentralityStore};
const DEFAULT_NUM_TOP_HARMONIC_CENTRALITY_FOR_NODES: usize = 1_000_000;

pub struct Scorer {
    liked: Vec<NodeScorer>,
    disliked: Vec<NodeScorer>,
    vectors: Arc<IntMap<bitvec_similarity::BitVec>>,
    cache: DashMap<NodeID, f64>,
}

#[derive(Debug)]
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

impl Scorer {
    fn calculate_score(&self, node: &NodeID) -> f64 {
        match self.vectors.get(&node.0) {
            Some(vec) => ((self.disliked.len() as f64)
                + (self
                    .liked
                    .iter()
                    .map(|liked| liked.sim(node, vec))
                    .sum::<f64>()
                    - self
                        .disliked
                        .iter()
                        .map(|disliked| disliked.sim(node, vec))
                        .sum::<f64>())
                    / (self.liked.len() + 1) as f64)
                .max(0.0),
            None => 0.0,
        }
    }
    pub fn score(&self, node: &NodeID) -> f64 {
        if let Some(cached) = self.cache.get(node) {
            return *cached;
        }

        let score = self.calculate_score(node);
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

#[derive(Clone, Debug)]
struct ScoredNode {
    node: NodeID,
    score: f64,
}

impl PartialOrd for ScoredNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.score.partial_cmp(&other.score)
    }
}

impl PartialEq for ScoredNode {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

impl Ord for ScoredNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl Eq for ScoredNode {}

#[derive(Serialize, Deserialize, Default)]
pub struct InboundSimilarity {
    vectors: Arc<IntMap<bitvec_similarity::BitVec>>,
}

impl InboundSimilarity {
    pub fn build(graph: &Webgraph, harmonic: &HarmonicCentralityStore) -> Self {
        Self::build_with_threshold(
            graph,
            harmonic,
            DEFAULT_NUM_TOP_HARMONIC_CENTRALITY_FOR_NODES,
        )
    }

    fn build_with_threshold(
        graph: &Webgraph,
        harmonic: &HarmonicCentralityStore,
        num_top_nodes: usize,
    ) -> Self {
        let mut vectors = IntMap::new();

        let mut top_nodes: BinaryHeap<ScoredNode> = BinaryHeap::with_capacity(num_top_nodes);

        for (node, centrality) in harmonic.iter() {
            let scored_node = ScoredNode {
                node,
                score: centrality,
            };

            if top_nodes.len() >= num_top_nodes {
                if let Some(mut worst) = top_nodes.peek_mut() {
                    if worst.score < scored_node.score {
                        *worst = scored_node.clone();
                    }
                }
            } else {
                top_nodes.push(scored_node);
            }
        }

        let top_nodes: IntSet = top_nodes.into_iter().map(|n| n.node.0).collect();

        for node_id in graph.nodes() {
            let mut ranks = Vec::new();

            for edge in graph.raw_ingoing_edges(&node_id) {
                if !top_nodes.contains(&edge.from.0) {
                    continue;
                }

                ranks.push(edge.from.0 as usize);
            }

            vectors.insert(node_id.0, bitvec_similarity::BitVec::new(ranks));
        }

        Self {
            vectors: Arc::new(vectors),
        }
    }

    pub fn scorer(&self, liked_sites: &[NodeID], disliked_sites: &[NodeID]) -> Scorer {
        let liked: Vec<_> = liked_sites
            .iter()
            .filter_map(|id| self.vectors.get(&id.0).cloned().map(|vec| (id, vec)))
            .map(|(node, inbound)| NodeScorer::new(*node, inbound))
            .collect();

        let disliked: Vec<_> = disliked_sites
            .iter()
            .filter_map(|id| self.vectors.get(&id.0).cloned().map(|vec| (id, vec)))
            .map(|(node, inbound)| NodeScorer::new(*node, inbound))
            .collect();

        Scorer {
            liked,
            disliked,
            vectors: self.vectors.clone(),
            cache: DashMap::new(),
        }
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

    pub fn get(&self, node: &NodeID) -> Option<&bitvec_similarity::BitVec> {
        self.vectors.get(&node.0)
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;

        Ok(bincode::deserialize(&buf)?)
    }

    pub fn knows_about(&self, node_id: NodeID) -> bool {
        self.vectors.contains_key(&node_id.0)
    }
}

#[cfg(test)]
mod tests {
    use optics::SiteRankings;

    use crate::{
        gen_temp_path,
        index::Index,
        kv::rocksdb_store::RocksDbStore,
        rand_words,
        ranking::centrality_store::CentralityStore,
        searcher::{LocalSearcher, SearchQuery},
        webgraph::{centrality::harmonic::HarmonicCentrality, Node, WebgraphBuilder},
        webpage::{Html, Webpage},
    };

    use super::*;

    #[test]
    fn it_favors_liked_sites() {
        let mut graph = WebgraphBuilder::new_memory().open();

        graph.insert(Node::from("a.com"), Node::from("b.com"), String::new());
        graph.insert(Node::from("c.com"), Node::from("d.com"), String::new());
        graph.insert(Node::from("a.com"), Node::from("e.com"), String::new());

        graph.insert(Node::from("z.com"), Node::from("a.com"), String::new());
        graph.insert(Node::from("z.com"), Node::from("b.com"), String::new());
        graph.insert(Node::from("z.com"), Node::from("c.com"), String::new());
        graph.insert(Node::from("z.com"), Node::from("d.com"), String::new());
        graph.insert(Node::from("z.com"), Node::from("e.com"), String::new());

        graph.commit();

        let harmonic = HarmonicCentrality::calculate(&graph);

        let harmonic_centrality_store = RocksDbStore::open(crate::gen_temp_path());
        for (node, centrality) in harmonic.iter() {
            harmonic_centrality_store.insert(*node, centrality);
        }
        harmonic_centrality_store.flush();

        let inbound =
            InboundSimilarity::build_with_threshold(&graph, &harmonic_centrality_store, 1000);

        let scorer = inbound.scorer(&[graph.node2id(&Node::from("b.com")).unwrap()], &[]);
        let e = graph.node2id(&Node::from("e.com")).unwrap();
        let d = graph.node2id(&Node::from("d.com")).unwrap();

        assert!(scorer.score(&e) > scorer.score(&d));
    }

    #[test]
    fn it_ranks_search_results() {
        let mut graph = WebgraphBuilder::new_memory().open();

        graph.insert(Node::from("b.com"), Node::from("a.com"), String::new());
        graph.insert(Node::from("c.com"), Node::from("d.com"), String::new());
        graph.insert(Node::from("b.com"), Node::from("e.com"), String::new());
        graph.insert(Node::from("c.com"), Node::from("b.com"), String::new());

        graph.commit();

        let harmonic = HarmonicCentrality::calculate(&graph);

        let harmonic_centrality_store = RocksDbStore::open(crate::gen_temp_path());
        for (node, centrality) in harmonic.iter() {
            harmonic_centrality_store.insert(*node, centrality);
        }
        harmonic_centrality_store.flush();

        let inbound =
            InboundSimilarity::build_with_threshold(&graph, &harmonic_centrality_store, 1000);

        let mut centrality_store = CentralityStore::build(&graph, gen_temp_path());
        centrality_store.inbound_similarity = inbound;

        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(Webpage {
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
                ),
                backlinks: vec![],
                host_centrality: 0.0,
                fetch_time_ms: 500,
                page_centrality: 0.0,
                pre_computed_score: 0.0,

                node_id: Some(graph.node2id(&Node::from("e.com")).unwrap()),
                dmoz_description: None,
            })
            .expect("failed to insert webpage");
        index
            .insert(Webpage {
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
                ),
                backlinks: vec![],
                host_centrality: 0.01,
                fetch_time_ms: 500,
                page_centrality: 0.0,
                pre_computed_score: 0.0,

                node_id: Some(graph.node2id(&Node::from("d.com")).unwrap()),
                dmoz_description: None,
            })
            .expect("failed to insert webpage");

        index.commit().unwrap();

        let mut searcher = LocalSearcher::new(index);
        searcher.set_centrality_store(centrality_store.into());

        let res = searcher
            .search(&SearchQuery {
                query: "example".to_string(),
                site_rankings: Some(SiteRankings {
                    liked: vec!["a.com".to_string()],
                    disliked: vec![],
                    blocked: vec![],
                }),
                ..Default::default()
            })
            .unwrap()
            .webpages;

        assert_eq!(res.len(), 2);
        assert_eq!(&res[0].url, "https://e.com");
        assert_eq!(&res[1].url, "https://d.com");
    }
}
