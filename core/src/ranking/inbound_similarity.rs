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

use std::{
    fs::File,
    io::{BufReader, BufWriter, Read},
    path::Path,
    sync::Arc,
};

use serde::{Deserialize, Serialize};

use crate::{
    intmap::IntMap,
    webgraph::{centrality::harmonic::HarmonicCentrality, NodeID, Webgraph},
    Result,
};

use super::bitvec_similarity;
const DEFAULT_HARMONIC_CENTRALITY_THRESHOLD_FOR_INBOUND: f64 = 0.038;
const DEFAULT_HARMONIC_CENTRALITY_THRESHOLD_FOR_NODES: f64 = 0.03;
const SCORE_SCALE: f64 = 5.0;

pub struct Scorer {
    liked: Vec<bitvec_similarity::BitVec>,
    disliked: Vec<bitvec_similarity::BitVec>,
    vectors: Arc<IntMap<bitvec_similarity::BitVec>>,
}

impl Scorer {
    pub fn score(&self, node: &NodeID) -> f64 {
        match self.vectors.get(&node.0) {
            Some(vec) => (SCORE_SCALE
                + (self.liked.iter().map(|liked| liked.sim(vec)).sum::<f64>()
                    - self
                        .disliked
                        .iter()
                        .map(|disliked| disliked.sim(vec))
                        .sum::<f64>()))
            .max(0.0),
            None => 0.0,
        }
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct InboundSimilarity {
    vectors: Arc<IntMap<bitvec_similarity::BitVec>>,
}

impl InboundSimilarity {
    pub fn build(graph: &Webgraph, harmonic: &HarmonicCentrality) -> Self {
        Self::build_with_threshold(
            graph,
            harmonic,
            DEFAULT_HARMONIC_CENTRALITY_THRESHOLD_FOR_NODES,
            DEFAULT_HARMONIC_CENTRALITY_THRESHOLD_FOR_INBOUND,
        )
    }
    fn build_with_threshold(
        graph: &Webgraph,
        harmonic: &HarmonicCentrality,
        harmonic_centrality_threshold_nodes: f64,
        harmonic_centrality_threshold_inbound: f64,
    ) -> Self {
        let mut vectors = IntMap::new();
        let nodes: Vec<_> = graph.nodes().collect();

        if let Some(max_node) = nodes.iter().max().copied() {
            for node_id in nodes
                .into_iter()
                .filter(|node_id| match graph.id2node(&node_id) {
                    Some(node) => {
                        let score = *harmonic.host.get(&node.into_host()).unwrap_or(&0.0);
                        score >= harmonic_centrality_threshold_nodes
                    }
                    None => false,
                })
            {
                let mut buf = vec![false; max_node.0 as usize];

                for edge in graph
                    .raw_ingoing_edges(&node_id)
                    .into_iter()
                    .filter(|edge| match graph.id2node(&edge.from) {
                        Some(node) => {
                            let score = *harmonic.host.get(&node.into_host()).unwrap_or(&0.0);
                            score >= harmonic_centrality_threshold_inbound
                        }
                        None => false,
                    })
                {
                    buf[edge.from.0 as usize] = true;
                }

                vectors.insert(node_id.0, bitvec_similarity::BitVec::new(buf));
            }
        }

        Self {
            vectors: Arc::new(vectors),
        }
    }

    pub fn scorer(&self, liked_sites: &[NodeID], disliked_sites: &[NodeID]) -> Scorer {
        let liked: Vec<_> = liked_sites
            .iter()
            .filter_map(|id| self.vectors.get(&id.0).cloned())
            .collect();

        let disliked: Vec<_> = disliked_sites
            .iter()
            .filter_map(|id| self.vectors.get(&id.0).cloned())
            .collect();

        Scorer {
            liked,
            disliked,
            vectors: self.vectors.clone(),
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
}

#[cfg(test)]
mod tests {
    use optics::SiteRankings;

    use crate::{
        gen_temp_path,
        index::Index,
        rand_words,
        ranking::centrality_store::CentralityStore,
        searcher::{LocalSearcher, SearchQuery},
        webgraph::{Node, WebgraphBuilder},
        webpage::{Html, Webpage},
    };

    use super::*;

    #[test]
    fn it_favors_liked_sites() {
        let mut graph = WebgraphBuilder::new_memory().open();

        graph.insert(Node::from("a.com"), Node::from("b.com"), String::new());
        graph.insert(Node::from("c.com"), Node::from("d.com"), String::new());
        graph.insert(Node::from("a.com"), Node::from("e.com"), String::new());

        graph.commit();

        let harmonic = HarmonicCentrality::calculate(&graph);
        let inbound = InboundSimilarity::build_with_threshold(&graph, &harmonic, -1.0, -1.0);

        let scorer = inbound.scorer(&[graph.node2id(&Node::from("b.com")).unwrap()], &[]);
        let e = graph.node2id(&Node::from("e.com")).unwrap();
        let d = graph.node2id(&Node::from("d.com")).unwrap();

        assert!(scorer.score(&e) > scorer.score(&d));
    }

    #[test]
    fn it_ranks_search_results() {
        let mut graph = WebgraphBuilder::new_memory().open();

        graph.insert(Node::from("a.com"), Node::from("b.com"), String::new());
        graph.insert(Node::from("c.com"), Node::from("d.com"), String::new());
        graph.insert(Node::from("a.com"), Node::from("e.com"), String::new());

        graph.commit();

        let harmonic = HarmonicCentrality::calculate(&graph);
        let inbound = InboundSimilarity::build_with_threshold(&graph, &harmonic, -1.0, -1.0);

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
                    "e.com",
                ),
                backlinks: vec![],
                host_centrality: 0.0,
                fetch_time_ms: 500,
                page_centrality: 0.0,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                primary_image: None,
                host_topic: None,
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
                    "d.com",
                ),
                backlinks: vec![],
                host_centrality: 0.01,
                fetch_time_ms: 500,
                page_centrality: 0.0,
                pre_computed_score: 0.0,
                crawl_stability: 0.0,
                primary_image: None,
                host_topic: None,
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
