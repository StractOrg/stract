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
    io::{BufReader, BufWriter, Read},
    path::Path,
    sync::Arc,
};

use dashmap::DashMap;
use fnv::FnvHashMap as HashMap;
use fnv::FnvHashSet as HashSet;
use rayon::prelude::*;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    webgraph::{NodeID, Webgraph},
    Result,
};

use super::bitvec_similarity;

#[derive(Clone)]
pub struct Scorer {
    liked: Vec<NodeScorer>,
    disliked: Vec<NodeScorer>,

    similarities: Arc<CommutativeMap<NodeID, f64>>,
    normalized: bool,
}

#[derive(Debug, Clone)]
struct NodeScorer {
    node: NodeID,
    self_score: f64,
}

impl NodeScorer {
    fn new(node: NodeID) -> Self {
        Self {
            node,
            self_score: 1.0,
        }
    }

    fn set_self_score(&mut self, self_score: f64) {
        self.self_score = self_score;
    }

    fn sim(&self, other: &NodeID, similarities: &CommutativeMap<NodeID, f64>) -> f64 {
        if self.node == *other {
            self.self_score
        } else {
            similarities
                .get(&(self.node, *other))
                .copied()
                .unwrap_or_default()
        }
    }
}

impl Scorer {
    pub fn score(&mut self, node: &NodeID) -> f64 {
        let s = (self.disliked.len() as f64)
            + (self
                .liked
                .iter()
                .map(|liked| liked.sim(node, &self.similarities))
                .sum::<f64>()
                - self
                    .disliked
                    .iter()
                    .map(|disliked| disliked.sim(node, &self.similarities))
                    .sum::<f64>());

        if self.normalized {
            s / self.liked.len().max(1) as f64
        } else {
            s
        }
        .max(0.0)
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
struct OrderedTuple<T> {
    elements: (T, T),
}

impl<T> OrderedTuple<T>
where
    T: PartialEq + Eq + PartialOrd + Ord + std::hash::Hash + Copy,
{
    fn new(elements: (T, T)) -> Self {
        if elements.0 > elements.1 {
            Self {
                elements: (elements.1, elements.0),
            }
        } else {
            Self { elements }
        }
    }
}

#[derive(Serialize, Deserialize)]
struct CommutativeMap<K, V>
where
    K: Eq + std::hash::Hash,
{
    map: fnv::FnvHashMap<OrderedTuple<K>, V>,
}

impl<K, V> Default for CommutativeMap<K, V>
where
    K: Eq + std::hash::Hash,
{
    fn default() -> Self {
        Self {
            map: fnv::FnvHashMap::default(),
        }
    }
}

impl<K, V> CommutativeMap<K, V>
where
    K: PartialEq + Eq + PartialOrd + Ord + std::hash::Hash + Copy + Serialize + DeserializeOwned,
    V: Copy + Serialize + DeserializeOwned,
{
    fn new() -> Self {
        Self::default()
    }

    fn insert(&mut self, key: (K, K), value: V) {
        self.map.insert(OrderedTuple::new(key), value);
    }

    fn get(&self, key: &(K, K)) -> Option<&V> {
        self.map.get(&OrderedTuple::new(*key))
    }
}

struct VecMap {
    vectors: HashMap<NodeID, bitvec_similarity::BitVec>,
}

impl VecMap {
    fn build(graph: &Webgraph) -> Self {
        let mut vectors = HashMap::default();

        let adjacency: DashMap<NodeID, HashSet<NodeID>> = DashMap::new();

        graph.par_edges().for_each(|edge| {
            adjacency.entry(edge.to).or_default().insert(edge.from);
        });

        for (node_id, inbound) in adjacency {
            vectors.insert(
                node_id,
                bitvec_similarity::BitVec::new(inbound.into_iter().map(|n| n.bit_64()).collect()),
            );
        }

        Self { vectors }
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct InboundSimilarity {
    similarities: Arc<CommutativeMap<NodeID, f64>>,
    known: HashSet<NodeID>,
}

impl InboundSimilarity {
    pub fn build(graph: &Webgraph) -> Self {
        let inbound = VecMap::build(graph);

        tracing::info!("Pre-calculating inbound similarities");
        let pb = indicatif::ProgressBar::new(graph.nodes().count() as u64);
        let sims: DashMap<OrderedTuple<NodeID>, f64> = graph
            .par_nodes()
            .flat_map(|node_id| {
                pb.tick();
                match inbound.vectors.get(&node_id) {
                    Some(node_inb) => graph
                        .raw_ingoing_edges(&node_id)
                        .into_iter()
                        .flat_map(|edge| {
                            graph
                                .raw_outgoing_edges(&edge.from)
                                .into_iter()
                                .map(|edge| edge.to)
                        })
                        .filter(|candidate| *candidate != node_id)
                        .map(|candidate| {
                            let candidate_inb = inbound.vectors.get(&candidate).unwrap();

                            let sim = node_inb.sim(candidate_inb);

                            (OrderedTuple::new((node_id, candidate)), sim)
                        })
                        .collect(),
                    None => vec![],
                }
            })
            .collect();

        pb.finish_and_clear();

        let mut similarities = CommutativeMap::new();
        let mut known = HashSet::default();

        for (key, value) in sims {
            similarities.insert(key.elements, value);

            known.insert(key.elements.0);
            known.insert(key.elements.1);
        }

        Self {
            similarities: Arc::new(similarities),
            known,
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
            .map(|node| NodeScorer::new(*node))
            .collect();

        let disliked: Vec<_> = disliked_hosts
            .iter()
            .map(|node| NodeScorer::new(*node))
            .collect();

        Scorer {
            liked,
            disliked,
            similarities: self.similarities.clone(),
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

    pub fn knows_about(&self, node_id: NodeID) -> bool {
        self.known.contains(&node_id)
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
                )
                .unwrap(),
                fetch_time_ms: 500,
                node_id: Some(Node::from("e.com").id()),
                ..Default::default()
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
