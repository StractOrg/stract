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
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
use anyhow::anyhow;
use chrono::Utc;
use rayon::prelude::*;
use std::path::Path;
use std::thread;

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::pin;
use tracing::{debug, info, trace, warn};

use crate::config::{self, WarcSource};
use crate::entrypoint::download_all_warc_files;
use crate::index::Index;
use crate::kv::rocksdb_store::RocksDbStore;
use crate::kv::Kv;
use crate::mapreduce::{Map, Reduce, Worker};
use crate::ranking::SignalAggregator;
use crate::warc::PayloadType;
use crate::webgraph::{Node, NodeID, Webgraph, WebgraphBuilder};
use crate::webpage::{safety_classifier, Html, Webpage};
use crate::{human_website_annotations, Result};

#[derive(Debug, Serialize, Deserialize)]
pub struct Job {
    pub source_config: config::WarcSource,
    pub warc_paths: Vec<String>,
    pub base_path: String,
    pub settings: JobSettings,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct JobSettings {
    pub host_centrality_threshold: Option<f64>,
    pub minimum_clean_words: Option<usize>,
}

pub struct IndexingWorker {
    host_centrality_store: RocksDbStore<NodeID, f64>,
    host_centrality_rank_store: RocksDbStore<NodeID, f64>,
    page_centrality_store: Option<RocksDbStore<NodeID, f64>>,
    page_centrality_rank_store: Option<RocksDbStore<NodeID, f64>>,
    page_webgraph: Option<Webgraph>,
    topics: Option<human_website_annotations::Mapper>,
    safety_classifier: Option<safety_classifier::Model>,
    job_settings: Option<JobSettings>,
}

impl IndexingWorker {
    pub fn new(
        host_centrality_store_path: String,
        page_centrality_store_path: Option<String>,
        page_webgraph_path: Option<String>,
        topics_path: Option<String>,
        safety_classifier_path: Option<String>,
    ) -> Self {
        Self {
            host_centrality_store: RocksDbStore::open(
                Path::new(&host_centrality_store_path).join("harmonic"),
            ),
            host_centrality_rank_store: RocksDbStore::open(
                Path::new(&host_centrality_store_path).join("harmonic_rank"),
            ),
            page_centrality_store: page_centrality_store_path
                .as_ref()
                .map(|p| RocksDbStore::open(Path::new(&p).join("approx_harmonic"))),
            page_centrality_rank_store: page_centrality_store_path
                .as_ref()
                .map(|p| RocksDbStore::open(Path::new(&p).join("approx_harmonic_rank"))),
            page_webgraph: page_webgraph_path
                .map(|path| WebgraphBuilder::new(path).single_threaded().open()),
            topics: topics_path.map(|path| human_website_annotations::Mapper::open(path).unwrap()),
            safety_classifier: safety_classifier_path
                .map(|path| safety_classifier::Model::open(path).unwrap()),
            job_settings: None,
        }
    }

    pub fn set_job_settings(&mut self, job_settings: JobSettings) {
        self.job_settings = Some(job_settings);
    }

    pub fn prepare_webpage(&self, body: &str, url: &str, fetch_time_ms: u64) -> Result<Webpage> {
        let mut html = match Html::parse_without_text(body, url) {
            Ok(html) => html,
            Err(err) => {
                debug!("error parsing html: {:?}", err);
                return Err(anyhow!("error parsing html: {:?}", err));
            }
        };

        if html.is_no_index() {
            return Err(anyhow!("noindex"));
        }

        let title = html.title().unwrap_or_default();
        if title.is_empty() || title.chars().all(|c| c.is_whitespace()) {
            return Err(anyhow!("empty title"));
        }

        let node = Node::from(html.url());
        let host_node_id = node.clone().into_host().id();

        let mut host_centrality = self
            .host_centrality_store
            .get(&host_node_id)
            .unwrap_or_default();

        let mut host_centrality_rank = self
            .host_centrality_rank_store
            .get(&host_node_id)
            .unwrap_or(u64::MAX as f64);

        if let Some(host_centrality_threshold) =
            self.job_settings.and_then(|s| s.host_centrality_threshold)
        {
            if host_centrality < host_centrality_threshold {
                debug!("skipping due to low host_centrality value");
                return Err(anyhow!("low host_centrality value"));
            }
        }

        html.parse_text();

        if html.empty_all_text() {
            return Err(anyhow!("empty all text"));
        }

        if let Some(minimum_clean_words) = self.job_settings.and_then(|s| s.minimum_clean_words) {
            match html.clean_text() {
                Some(clean_text) => {
                    if clean_text.split_whitespace().count() < minimum_clean_words {
                        return Err(anyhow!("too few clean words"));
                    }
                }
                None => return Err(anyhow!("no clean text")),
            }
        }

        let backlink_labels: Vec<String> = self
            .page_webgraph
            .as_ref()
            .map(|webgraph| {
                webgraph
                    .raw_ingoing_edges_with_labels(&Node::from(html.url()).id())
                    .into_iter()
                    .map(|edge| edge.label)
                    .filter(|label| !label.is_empty())
                    .filter(|label| {
                        let label = label.to_lowercase();
                        let stopwords = [
                            "click",
                            "click here",
                            "here",
                            "link",
                            "website",
                            "webpage",
                            "page",
                            "site",
                            "url",
                            "web",
                            "visit",
                            "more",
                            "info",
                            "information",
                            "read",
                            "read more",
                        ];

                        !stopwords.contains(&label.as_str())
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut page_centrality = 0.0;

        if let Some(store) = self.page_centrality_store.as_ref() {
            let node_id = node.id();

            page_centrality = store.get(&node_id).unwrap_or_default();
        }

        let mut page_centrality_rank = u64::MAX as f64;

        if let Some(store) = self.page_centrality_rank_store.as_ref() {
            let node_id = node.id();

            page_centrality_rank = store.get(&node_id).unwrap_or(u64::MAX as f64);
        }

        if !page_centrality.is_finite() {
            page_centrality = 0.0;
        }

        if !host_centrality.is_finite() {
            host_centrality = 0.0;
        }

        if !page_centrality_rank.is_finite() {
            page_centrality_rank = u64::MAX as f64;
        }

        if !host_centrality_rank.is_finite() {
            host_centrality_rank = u64::MAX as f64;
        }

        let mut dmoz_description = None;

        if let Some(mapper) = self.topics.as_ref() {
            if let Some(info) = mapper.get(&html.url().host_str().unwrap_or_default().to_string()) {
                dmoz_description = Some(info.description.clone())
            }
        }

        let mut webpage = Webpage {
            html,
            backlink_labels,
            page_centrality,
            page_centrality_rank,
            host_centrality,
            host_centrality_rank,
            fetch_time_ms,
            pre_computed_score: 0.0,
            node_id: Some(host_node_id),
            dmoz_description,
            safety_classification: None,
            inserted_at: Utc::now(),
        };

        if let Some(model) = self.safety_classifier.as_ref() {
            webpage.safety_classification = Some(model.predict(&webpage).label);
        }

        let mut signal_aggregator = SignalAggregator::new(None);
        signal_aggregator.set_current_timestamp(Utc::now().timestamp().max(0) as usize);

        webpage.pre_computed_score = signal_aggregator.precompute_score(&webpage);

        Ok(webpage)
    }
}

pub fn process_job(job: &Job, worker: &IndexingWorker) -> Index {
    let name = job.warc_paths.first().unwrap().split('/').last().unwrap();

    let mut has_host_centrality = false;
    let mut has_page_centrality = false;
    let mut has_backlinks = false;

    info!("processing {}", name);

    let mut index = Index::open(Path::new(&job.base_path).join(name)).unwrap();
    index.prepare_writer().unwrap();

    let warc_files = download_all_warc_files(&job.warc_paths, &job.source_config);
    pin!(warc_files);

    for file in warc_files.by_ref() {
        for record in
            file.records()
                .flatten()
                .filter(|record| match &record.response.payload_type {
                    Some(payload_type) => matches!(payload_type, PayloadType::Html),
                    None => false,
                })
        {
            if let Ok(webpage) = worker.prepare_webpage(
                &record.response.body,
                &record.request.url,
                record.metadata.fetch_time_ms,
            ) {
                if webpage.host_centrality > 0.0 {
                    has_host_centrality = true;
                }

                if webpage.page_centrality > 0.0 {
                    has_page_centrality = true;
                }

                if !webpage.backlink_labels.is_empty() {
                    has_backlinks = true;
                }
                trace!("inserting webpage: {:?}", webpage.html.url());
                trace!("title = {:?}", webpage.html.title());
                trace!("text = {:?}", webpage.html.clean_text());

                if let Err(err) = index.insert(webpage) {
                    warn!("{:?}", err);
                    panic!();
                }
            }
        }

        index.commit().unwrap();
    }

    if !has_host_centrality {
        warn!("no host centrality values found in {}", name);
    }

    if !has_page_centrality && worker.page_centrality_store.is_some() {
        warn!("no page centrality values found in {}", name);
    }

    if !has_backlinks && worker.page_webgraph.is_some() {
        warn!("no backlinks found in {}", name);
    }

    index.inverted_index.merge_into_max_segments(1).unwrap();

    info!("{} done", name);

    index
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexPointer(String);

impl From<String> for IndexPointer {
    fn from(path: String) -> Self {
        IndexPointer(path)
    }
}

impl Worker for IndexingWorker {}

impl Map<IndexingWorker, IndexPointer> for Job {
    fn map(&self, worker: &IndexingWorker) -> IndexPointer {
        let index = process_job(self, worker);
        IndexPointer(index.path)
    }
}

impl Reduce<Index> for Index {
    fn reduce(self, element: Index) -> Self {
        let other = element;
        let other_path = other.path.clone();

        let res = self.merge(other);

        std::fs::remove_dir_all(other_path).unwrap();

        res
    }
}

pub struct Indexer {}
impl Indexer {
    pub fn run(config: &config::IndexingLocalConfig) -> Result<()> {
        let warc_paths = config.warc_source.paths()?;

        let job_config: WarcSource = config.warc_source.clone();

        let worker = IndexingWorker::new(
            config.host_centrality_store_path.clone(),
            config.page_centrality_store_path.clone(),
            config.page_webgraph_path.clone(),
            config.topics_path.clone(),
            config.safety_classifier_path.clone(),
        );

        let indexes = warc_paths
            .into_iter()
            .skip(config.skip_warc_files.unwrap_or(0))
            .take(config.limit_warc_files.unwrap_or(usize::MAX))
            .chunks(config.batch_size.unwrap_or(1))
            .into_iter()
            .map(|paths| paths.collect_vec())
            .collect_vec()
            .into_par_iter()
            .map(|warc_paths| Job {
                source_config: job_config.clone(),
                warc_paths,
                base_path: config
                    .output_path
                    .clone()
                    .unwrap_or_else(|| "data/index".to_string()),
                settings: JobSettings {
                    host_centrality_threshold: config.host_centrality_threshold,
                    minimum_clean_words: config.minimum_clean_words,
                },
            })
            .map(|job| {
                let pointer: IndexPointer = job.map(&worker);
                pointer
            })
            .collect();

        Self::merge(indexes)?;
        Ok(())
    }

    pub fn merge(indexes: Vec<IndexPointer>) -> Result<()> {
        let num_indexes = indexes.len();
        let mut it = indexes.into_iter();
        let num_cores = num_cpus::get();

        let mut threads = Vec::new();

        for _ in 0..(num_cores + 1) {
            let indexes = it
                .by_ref()
                .take(((num_indexes as f64) / (num_cores as f64)).ceil() as usize)
                .collect_vec();

            if indexes.is_empty() {
                break;
            }

            threads.push(thread::spawn(move || {
                let mut it = indexes.into_iter();
                let mut index = Index::open(it.next().unwrap().0).unwrap();
                index.prepare_writer().unwrap();

                for other in it {
                    let other_path = other.0;
                    let mut other = Index::open(&other_path).unwrap();
                    other.prepare_writer().unwrap();

                    index = index.merge(other);

                    std::fs::remove_dir_all(other_path).unwrap();
                }
                index.inverted_index.merge_into_max_segments(1).unwrap();

                index
            }));
        }

        let mut indexes = Vec::new();
        for thread in threads {
            indexes.push(thread.join().unwrap());
        }

        let mut it = indexes.into_iter();
        let mut index = it.next().unwrap();

        for other in it {
            let other_path = other.path.clone();
            index = index.merge(other);
            std::fs::remove_dir_all(other_path).unwrap();
        }

        Ok(())
    }
}
