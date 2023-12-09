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
use rayon::prelude::ParallelIterator;
use serde::{Deserialize, Serialize};

use crate::{
    webgraph::{NodeID, Webgraph},
    Result,
};

use super::bitvec_similarity;

#[derive(Clone)]
pub struct Scorer {
    similarity: bitvec_similarity::BitVecSimilarity,
    liked: Vec<NodeScorer>,
    disliked: Vec<NodeScorer>,
    vectors: Arc<HashMap<NodeID, bitvec_similarity::BitVec>>,
    cache: HashMap<NodeID, f64>,
    normalized: bool,
}

#[derive(Debug, Clone)]
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

    fn sim(
        &self,
        other: &NodeID,
        other_inbound: &bitvec_similarity::BitVec,
        similarity: &mut bitvec_similarity::BitVecSimilarity,
    ) -> f64 {
        if self.node == *other {
            self.self_score
        } else {
            similarity.sim(&self.inbound, other_inbound)
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
                        .map(|liked| liked.sim(node, vec, &mut self.similarity))
                        .sum::<f64>()
                        - self
                            .disliked
                            .iter()
                            .map(|disliked| disliked.sim(node, vec, &mut self.similarity))
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
}

#[derive(Serialize, Deserialize, Default)]
pub struct InboundSimilarity {
    vectors: Arc<HashMap<NodeID, bitvec_similarity::BitVec>>,
}

impl InboundSimilarity {
    pub fn build(graph: &Webgraph) -> Self {
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

        Self {
            vectors: Arc::new(vectors),
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
            .map(|(node, inbound)| NodeScorer::new(*node, inbound))
            .collect();

        let disliked: Vec<_> = disliked_hosts
            .iter()
            .filter_map(|id| self.vectors.get(id).cloned().map(|vec| (id, vec)))
            .map(|(node, inbound)| NodeScorer::new(*node, inbound))
            .collect();

        Scorer {
            similarity: bitvec_similarity::BitVecSimilarity::default(),
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

        bincode::serialize_into(&mut file, &self)?;

        Ok(())
    }

    pub fn get(&self, node: &NodeID) -> Option<&bitvec_similarity::BitVec> {
        self.vectors.get(node)
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;

        Ok(bincode::deserialize(&buf)?)
    }

    pub fn knows_about(&self, node_id: NodeID) -> bool {
        self.vectors.contains_key(&node_id)
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
