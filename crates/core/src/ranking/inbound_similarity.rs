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
    fs::File,
    io::{BufReader, BufWriter},
    path::Path,
    sync::Arc,
};

use dashmap::DashMap;
use fnv::FnvHashMap as HashMap;
use fnv::FnvHashSet as HashSet;
use indicatif::ParallelProgressIterator;
use rayon::prelude::*;

use crate::{
    webgraph::{NodeID, Webgraph},
    Result,
};

use super::bitvec_similarity;

const PRECALCULATE_TOP_N: usize = 1_000;
const TOP_CANDIDATES_PER_PRECALCULATION: usize = 1_000;

#[derive(Clone)]
pub struct Scorer {
    liked: Vec<NodeScorer>,
    disliked: Vec<NodeScorer>,
    vectors: Arc<VecMap>,
    cache: HashMap<NodeID, f64>,
    normalized: bool,
}

#[derive(Clone)]
struct NodeScorer {
    node: NodeID,
    inbound: bitvec_similarity::BitVec,
    precalculated: Arc<PreCalculatedSimilarities>,
    self_score: f64,
    default_if_precalculated: bool,
}

impl NodeScorer {
    fn new(
        node: NodeID,
        inbound: bitvec_similarity::BitVec,
        precalculated: Arc<PreCalculatedSimilarities>,
    ) -> Self {
        Self {
            node,
            inbound,
            precalculated,
            self_score: 1.0,
            default_if_precalculated: false,
        }
    }

    fn set_self_score(&mut self, self_score: f64) {
        self.self_score = self_score;
    }

    fn set_default_if_precalculated(&mut self, default_if_precalculated: bool) {
        self.default_if_precalculated = default_if_precalculated;
    }

    fn sim(&self, other: &NodeID, other_inbound: &bitvec_similarity::BitVec) -> f64 {
        if self.node == *other {
            self.self_score
        } else if self.default_if_precalculated {
            self.precalculated
                .get_or_default_if_precalculated(&self.node, other)
                .or_else(|| {
                    self.precalculated
                        .get_or_default_if_precalculated(other, &self.node)
                })
                .unwrap_or_else(|| self.inbound.sim(other_inbound))
        } else {
            self.precalculated
                .get(&self.node, other)
                .or_else(|| self.precalculated.get(other, &self.node))
                .unwrap_or_else(|| self.inbound.sim(other_inbound))
        }
    }
}

impl Scorer {
    fn calculate_score(&mut self, node: &NodeID) -> f64 {
        let s = match self.vectors.get(node) {
            Some(vec) => {
                (self.disliked.len() as f64)
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
            }
            None => 0.0,
        };

        if self.normalized {
            s / self.liked.len().max(1) as f64
        } else {
            s
        }
        .max(0.0)
    }
    pub fn score(&mut self, node: &NodeID) -> f64 {
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

    /// Speedups calculation when we encounter a node for which we have
    /// precalculated similarities. This means, the chosen node is a node
    /// with many inbound links. If we encounter another node for which
    /// we have no precalcalculated similarities with the chosen node, they
    /// are most likely not very similar, so we can just assume a similarity
    /// of 0.0.
    ///
    /// Needless to say, this is less accurate than calculating the similarity.
    pub fn set_default_if_precalculated(&mut self, default_if_precalculated: bool) {
        for scorer in self.liked.iter_mut() {
            scorer.set_default_if_precalculated(default_if_precalculated);
        }

        for scorer in self.disliked.iter_mut() {
            scorer.set_default_if_precalculated(default_if_precalculated);
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Default)]
struct PreCalculatedSimilarities {
    map: HashMap<NodeID, HashMap<NodeID, f64>>,
}

impl PreCalculatedSimilarities {
    fn get_or_default_if_precalculated(&self, node: &NodeID, other: &NodeID) -> Option<f64> {
        Some(self.map.get(node)?.get(other).copied().unwrap_or_default())
    }

    fn get(&self, node: &NodeID, other: &NodeID) -> Option<f64> {
        self.map.get(node)?.get(other).copied()
    }
}

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Default)]
struct VecMap {
    map: HashMap<NodeID, bitvec_similarity::BitVec>,
}

impl VecMap {
    fn build(graph: &Webgraph) -> Self {
        let adjacency: DashMap<NodeID, HashSet<NodeID>> = DashMap::new();

        graph.par_edges().for_each(|edge| {
            adjacency.entry(edge.to).or_default().insert(edge.from);
        });

        let mut map = HashMap::default();
        for (node_id, inbound) in adjacency {
            map.insert(
                node_id,
                bitvec_similarity::BitVec::new(inbound.into_iter().map(|n| n.as_u64()).collect()),
            );
        }

        Self { map }
    }

    fn get(&self, id: &NodeID) -> Option<&bitvec_similarity::BitVec> {
        self.map.get(id)
    }

    fn contains(&self, id: &NodeID) -> bool {
        self.map.contains_key(id)
    }
}

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Default)]
pub struct InboundSimilarity {
    vectors: Arc<VecMap>,
    precalculated: Arc<PreCalculatedSimilarities>,
}

impl InboundSimilarity {
    pub fn build(graph: &Webgraph) -> Self {
        let vectors = VecMap::build(graph);

        tracing::info!("precalculating similarities...");

        let mut nodes = vectors
            .map
            .iter()
            .map(|(node, ranks)| (*node, ranks.len()))
            .collect::<Vec<_>>();
        nodes.sort_by(|(_, a), (_, b)| b.cmp(a));
        nodes.truncate(PRECALCULATE_TOP_N);

        let precalculated: HashMap<NodeID, HashMap<NodeID, f64>> = nodes
            .into_par_iter()
            .progress()
            .map(|(node, _)| {
                let node_vec = vectors.map.get(&node).unwrap();

                let candidates = graph
                    .raw_ingoing_edges(&node)
                    .into_iter()
                    .map(|edge| edge.from)
                    .flat_map(|c| graph.raw_outgoing_edges(&c).into_iter().map(|edge| edge.to))
                    .filter(|c| *c != node)
                    .collect::<HashSet<_>>();

                let mut candidates = candidates
                    .into_iter()
                    .filter_map(|c| vectors.map.get(&c).map(|vec| (c, vec)))
                    .map(|(c, vec)| (c, node_vec.sim(vec)))
                    .collect::<Vec<_>>();
                candidates.sort_by(|(_, a), (_, b)| b.total_cmp(a));
                candidates.truncate(TOP_CANDIDATES_PER_PRECALCULATION);

                let mut map = HashMap::default();

                for (candidate, _) in candidates {
                    if let Some(candidate_vec) = vectors.map.get(&candidate) {
                        let score = node_vec.sim(candidate_vec);
                        map.insert(candidate, score);
                    }
                }

                (node, map)
            })
            .collect();

        let precalculated = PreCalculatedSimilarities { map: precalculated };

        Self {
            vectors: Arc::new(vectors),
            precalculated: Arc::new(precalculated),
        }
    }

    pub fn scorer(
        &self,
        liked_hosts: &[NodeID],
        disliked_hosts: &[NodeID],
        normalized: bool,
    ) -> Scorer {
        let liked: Vec<_> = liked_hosts
            .iter()
            .filter_map(|id| self.vectors.get(id).cloned().map(|vec| (id, vec)))
            .map(|(node, inbound)| NodeScorer::new(*node, inbound, self.precalculated.clone()))
            .collect();

        let disliked: Vec<_> = disliked_hosts
            .iter()
            .filter_map(|id| self.vectors.get(id).cloned().map(|vec| (id, vec)))
            .map(|(node, inbound)| NodeScorer::new(*node, inbound, self.precalculated.clone()))
            .collect();

        Scorer {
            liked,
            disliked,
            vectors: self.vectors.clone(),
            cache: HashMap::default(),
            normalized,
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

        bincode::encode_into_std_write(self, &mut file, bincode::config::standard())?;

        Ok(())
    }

    pub fn get(&self, node: &NodeID) -> Option<&bitvec_similarity::BitVec> {
        self.vectors.get(node)
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        Ok(bincode::decode_from_std_read(
            &mut reader,
            bincode::config::standard(),
        )?)
    }
}

#[cfg(test)]
mod tests {
    use optics::HostRankings;

    use crate::{
        gen_temp_path,
        index::Index,
        rand_words,
        searcher::{LocalSearcher, SearchQuery},
        webgraph::{Node, WebgraphWriter},
        webpage::{Html, Webpage},
    };

    use super::*;

    #[test]
    fn it_favors_liked_hosts() {
        let mut wrt = WebgraphWriter::new(
            gen_temp_path(),
            crate::executor::Executor::single_thread(),
            crate::webgraph::Compression::default(),
        );

        wrt.insert(Node::from("a.com"), Node::from("b.com"), String::new());
        wrt.insert(Node::from("c.com"), Node::from("d.com"), String::new());
        wrt.insert(Node::from("a.com"), Node::from("e.com"), String::new());

        wrt.insert(Node::from("z.com"), Node::from("a.com"), String::new());
        wrt.insert(Node::from("z.com"), Node::from("b.com"), String::new());
        wrt.insert(Node::from("z.com"), Node::from("c.com"), String::new());
        wrt.insert(Node::from("z.com"), Node::from("d.com"), String::new());
        wrt.insert(Node::from("z.com"), Node::from("e.com"), String::new());

        let graph = wrt.finalize();

        let inbound = InboundSimilarity::build(&graph);

        let mut scorer = inbound.scorer(&[Node::from("b.com").id()], &[], false);
        let e = Node::from("e.com").id();
        let d = Node::from("d.com").id();

        assert!(scorer.score(&e) > scorer.score(&d));
    }

    #[test]
    fn it_ranks_search_results() {
        let mut wrt = WebgraphWriter::new(
            crate::gen_temp_path(),
            crate::executor::Executor::single_thread(),
            crate::webgraph::Compression::default(),
        );

        wrt.insert(Node::from("b.com"), Node::from("a.com"), String::new());
        wrt.insert(Node::from("c.com"), Node::from("d.com"), String::new());
        wrt.insert(Node::from("b.com"), Node::from("e.com"), String::new());
        wrt.insert(Node::from("c.com"), Node::from("b.com"), String::new());

        let graph = wrt.finalize();

        let inbound = InboundSimilarity::build(&graph);

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

        let mut searcher = LocalSearcher::new(index);
        searcher.set_inbound_similarity(inbound);

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
            .unwrap()
            .webpages;

        assert_eq!(res.len(), 2);
        assert_eq!(&res[0].url, "https://e.com/");
        assert_eq!(&res[1].url, "https://d.com/");
    }
}
