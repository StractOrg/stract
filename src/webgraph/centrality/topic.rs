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

use crate::Result;
use crate::{
    human_website_annotations::Mapper,
    index::Index,
    ranking::{initial::InitialScoreTweaker, SignalAggregator},
    schema::{FastField, Field, TextField},
    webgraph::{centrality::approximate_harmonic::SHIFT, Webgraph},
};

use super::approximate_harmonic::ApproximatedHarmonicCentrality;

const TOP_TERMS: usize = 1_000_000;

#[derive(Serialize, Deserialize)]
struct SerializableTopicCentrality {
    terms: HashMap<String, Vec<f64>>,
    host_centrality: Vec<Vec<f64>>, // host_idx => [f64; N]
}

impl<const N: usize> From<TopicCentrality<N>> for SerializableTopicCentrality {
    fn from(topic_centrality: TopicCentrality<N>) -> Self {
        Self {
            terms: topic_centrality
                .terms
                .into_iter()
                .map(|(s, v)| (s, v.to_vec()))
                .collect(),
            host_centrality: topic_centrality
                .host_centrality
                .into_iter()
                .map(|v| v.to_vec())
                .collect(),
        }
    }
}

impl<const N: usize> From<SerializableTopicCentrality> for TopicCentrality<N> {
    fn from(serialized: SerializableTopicCentrality) -> Self {
        Self {
            terms: serialized
                .terms
                .into_iter()
                .map(|(s, v)| (s, v.try_into().unwrap()))
                .collect(),
            host_centrality: serialized
                .host_centrality
                .into_iter()
                .map(|v| v.try_into().unwrap())
                .collect(),
        }
    }
}

pub struct TopicCentrality<const N: usize> {
    terms: HashMap<String, [f64; N]>,
    host_centrality: Vec<[f64; N]>, // host_idx => [f64; N]
}

impl<const N: usize> TopicCentrality<N> {
    pub fn build(
        index: Index,
        topics: Mapper,
        webgraph: Webgraph,
        approx: ApproximatedHarmonicCentrality,
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

        let top_topics = topics.top_topics(N);
        let topic_field = searcher
            .schema()
            .get_field(Field::Text(TextField::HostTopic).name())
            .unwrap();
        let collector = tantivy::collector::Count;

        let term_scores: HashMap<String, [f64; N]> = term_count
            .into_iter()
            .sorted_by_key(|(_, count)| *count)
            .rev()
            .take(TOP_TERMS)
            .progress()
            .par_bridge()
            .map(|(term, total_doc_freq)| {
                let mut scores = Vec::with_capacity(top_topics.len());
                for topic in &top_topics {
                    let facet = topic.as_facet();
                    let topic = tantivy::Term::from_facet(topic_field, &facet);
                    let topic = TermQuery::new(topic, IndexRecordOption::Basic).box_clone();

                    let term = tantivy::Term::from_field_text(body_field, &term);
                    let term = TermQuery::new(term, IndexRecordOption::Basic).box_clone();

                    let query = BooleanQuery::new(vec![(Occur::Must, topic), (Occur::Must, term)]);

                    let topic_freq = searcher.search(&query, &collector).unwrap();
                    scores.push(topic_freq as f64 / total_doc_freq as f64);
                }

                (term, scores.try_into().unwrap())
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

        let collector = tantivy::collector::TopDocs::with_limit(1000).tweak_score(score_tweaker);

        let mut nodes: Vec<_> = webgraph.host.as_ref().unwrap().nodes().collect();
        nodes.sort();

        let mut node_scores = vec![Vec::new(); nodes.len()];

        for topic in &top_topics {
            let facet = topic.as_facet();
            let topic = tantivy::Term::from_facet(topic_field, &facet);
            let query = TermQuery::new(topic, IndexRecordOption::Basic).box_clone();

            let top_sites: HashSet<_> = searcher
                .search(&query, &collector)
                .unwrap()
                .into_iter()
                .filter_map(|(_, doc_address)| {
                    let doc = searcher.doc(doc_address).unwrap();
                    let id = doc.get_first(graph_node_id).unwrap().as_u64().unwrap();
                    webgraph.host.as_ref().unwrap().id2node(&id)
                })
                .collect();

            let top_sites: Vec<_> = top_sites.into_iter().collect();

            assert!(!top_sites.is_empty());

            let scorer = approx.scorer_without_fixed(&top_sites, &[]);

            for node in &nodes {
                node_scores[*node as usize].push(scorer.score(*node) - SHIFT);
            }
        }

        let node_scores: Vec<_> = node_scores
            .into_iter()
            .map(|scores| scores.try_into().unwrap())
            .collect();

        Self {
            terms: term_scores,
            host_centrality: node_scores,
        }
    }

    pub fn save<P: AsRef<Path>>(self, path: P) -> Result<()> {
        let mut file = File::options()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;

        let serializable = SerializableTopicCentrality::from(self);

        let bytes = bincode::serialize(&serializable)?;
        file.write_all(&bytes)?;

        Ok(())
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut reader = BufReader::new(File::open(path)?);

        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;

        let serializable: SerializableTopicCentrality = bincode::deserialize(&bytes)?;
        Ok(Self::from(serializable))
    }
}
