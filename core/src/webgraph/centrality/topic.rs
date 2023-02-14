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
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufReader, Read, Write},
    path::Path,
    sync::Arc,
};

use indicatif::ProgressIterator;
use itertools::Itertools;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use tantivy::{
    query::{BooleanQuery, Occur, QueryClone, TermQuery},
    schema::IndexRecordOption,
};

use crate::{
    collector::TopDocs,
    human_website_annotations::Mapper,
    index::Index,
    ranking::{initial::InitialScoreTweaker, SignalAggregator},
    schema::{FastField, Field, TextField},
    webgraph::{NodeID, Webgraph},
};
use crate::{query::Query, Result};

use super::online_harmonic::OnlineHarmonicCentrality;

const TOP_TERMS: usize = 1_000_000;
pub const NUM_TOPICS: usize = 50;

#[derive(Clone)]
pub struct Scorer {
    term_weights: Vec<f64>,
    host_centrality: Arc<Vec<Vec<f64>>>,
}

impl Scorer {
    pub fn score(&self, host: NodeID) -> f64 {
        match self.host_centrality.get(host.0 as usize) {
            Some(host_score) => host_score
                .iter()
                .zip_eq(self.term_weights.iter())
                .map(|(h, t)| h * t)
                .sum(),
            None => 0.0,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct TopicCentrality {
    terms: HashMap<String, Vec<f64>>,
    host_centrality: Arc<Vec<Vec<f64>>>, // host_idx => [f64; N]
}

impl TopicCentrality {
    pub fn build(
        index: &Index,
        topics: Mapper,
        webgraph: Webgraph,
        harmonic: OnlineHarmonicCentrality,
    ) -> Self {
        let body_field = index
            .schema()
            .get_field(Field::Text(TextField::CleanBody).name())
            .unwrap();

        let searcher = index.inverted_index.reader.searcher();
        let mut term_count: HashMap<String, u64> = HashMap::new();

        for segment in searcher.segment_readers() {
            let reader = segment.inverted_index(body_field).unwrap();

            let mut stream = reader.terms().stream().unwrap();

            while let Some((term, info)) = stream.next() {
                let term = std::str::from_utf8(term)
                    .expect("Bytes are not unicode")
                    .to_string();
                *term_count.entry(term).or_default() += info.doc_freq as u64;
            }
        }

        let top_topics = topics.top_topics(NUM_TOPICS);
        let topic_field = searcher
            .schema()
            .get_field(Field::Text(TextField::HostTopic).name())
            .unwrap();
        let collector = tantivy::collector::Count;

        let term_scores: HashMap<String, Vec<f64>> = term_count
            .into_iter()
            .sorted_by_key(|(_, count)| *count)
            .rev()
            .take(TOP_TERMS)
            .progress()
            .par_bridge()
            .map(|(term, total_doc_freq)| {
                let mut scores = vec![0.0; NUM_TOPICS];
                for (i, topic) in top_topics.iter().enumerate() {
                    let facet = topic.as_facet();
                    let topic = tantivy::Term::from_facet(topic_field, &facet);
                    let topic = TermQuery::new(topic, IndexRecordOption::Basic).box_clone();

                    let term = tantivy::Term::from_field_text(body_field, &term);
                    let term = TermQuery::new(term, IndexRecordOption::Basic).box_clone();

                    let query = BooleanQuery::new(vec![(Occur::Must, topic), (Occur::Must, term)]);

                    let topic_freq = searcher.search(&query, &collector).unwrap();
                    scores[i] = topic_freq as f64 / total_doc_freq as f64;
                }

                (term, scores)
            })
            .collect();

        let score_tweaker = InitialScoreTweaker::new(
            Arc::new(index.region_count.clone()),
            None,
            SignalAggregator::default(),
            Arc::clone(&index.inverted_index.fastfield_cache),
        );

        let graph_node_id = index
            .schema()
            .get_field(Field::Fast(FastField::HostNodeID).name())
            .unwrap();

        let collector = TopDocs::with_limit(1000, index.inverted_index.fastfield_cache())
            .tweak_score(score_tweaker);

        let mut nodes: Vec<_> = webgraph.nodes().collect();
        nodes.sort();

        let mut node_scores = vec![vec![0.0; NUM_TOPICS]; nodes.len()];

        for (i, topic) in top_topics.iter().enumerate() {
            let facet = topic.as_facet();
            let topic = tantivy::Term::from_facet(topic_field, &facet);
            let query = TermQuery::new(topic, IndexRecordOption::Basic).box_clone();

            let top_sites: HashSet<_> = searcher
                .search(&query, &collector)
                .unwrap()
                .into_iter()
                .filter_map(|pointer| {
                    let doc = searcher.doc(pointer.address.into()).unwrap();
                    let id = doc.get_first(graph_node_id).unwrap().as_u64().unwrap();
                    if id == u64::MAX {
                        None
                    } else {
                        let id = id.into();
                        Some(webgraph.id2node(&id).unwrap())
                    }
                })
                .collect();

            let top_sites: Vec<_> = top_sites
                .into_iter()
                .filter_map(|node| webgraph.node2id(&node))
                .collect();

            assert!(!top_sites.is_empty());

            let scorer = harmonic.scorer(&top_sites, &[]);

            for node in &nodes {
                node_scores[node.0 as usize][i] = scorer.score(*node);
            }
        }

        Self {
            terms: term_scores,
            host_centrality: Arc::new(node_scores),
        }
    }

    pub fn save<P: AsRef<Path>>(self, path: P) -> Result<()> {
        let mut file = File::options()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;

        let bytes = bincode::serialize(&self)?;
        file.write_all(&bytes)?;

        Ok(())
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut reader = BufReader::new(File::open(path)?);

        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;

        Ok(bincode::deserialize(&bytes)?)
    }

    pub fn scorer(&self, query: &Query) -> Scorer {
        let terms = query.simple_terms();
        let mut term_weights = [0.0; NUM_TOPICS];

        for term in terms {
            if let Some(score) = self.terms.get(&term) {
                term_weights
                    .iter_mut()
                    .zip_eq(score.iter())
                    .for_each(|(acc, score)| *acc += score)
            }
        }

        Scorer {
            term_weights: term_weights.to_vec(),
            host_centrality: Arc::clone(&self.host_centrality),
        }
    }
}
