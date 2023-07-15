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
use chrono::Utc;
use std::path::Path;
use std::thread;

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::pin;
use tracing::{debug, info, trace};

use crate::entrypoint::download_all_warc_files;
use crate::executor::Executor;
use crate::index::{FrozenIndex, Index};
use crate::mapreduce::{Map, Reduce, Worker};
use crate::ranking::centrality_store::IndexerCentralityStore;
use crate::ranking::SignalAggregator;
use crate::warc::WarcFile;
use crate::webgraph::{Node, Webgraph, WebgraphBuilder};
use crate::webpage::{Html, Link, Webpage};
use crate::{
    human_website_annotations, HttpConfig, IndexingLocalConfig, LocalConfig, Result, S3Config,
    WarcSource,
};

pub struct Indexer {}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum JobConfig {
    Http(HttpConfig),
    Local(LocalConfig),
    S3(S3Config),
}

impl From<WarcSource> for JobConfig {
    fn from(value: WarcSource) -> Self {
        match value {
            WarcSource::HTTP(config) => JobConfig::Http(config),
            WarcSource::Local(config) => JobConfig::Local(config),
            WarcSource::S3(config) => JobConfig::S3(config),
        }
    }
}

impl From<JobConfig> for WarcSource {
    fn from(value: JobConfig) -> Self {
        match value {
            JobConfig::Http(config) => WarcSource::HTTP(config),
            JobConfig::Local(config) => WarcSource::Local(config),
            JobConfig::S3(config) => WarcSource::S3(config),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Job {
    pub source_config: JobConfig,
    pub warc_paths: Vec<String>,
    pub base_path: String,
    pub host_centrality_threshold: Option<f64>,
    pub minimum_clean_words: Option<usize>,
}

pub struct IndexingWorker {
    host_centrality_store: IndexerCentralityStore,
    page_centrality_store: Option<IndexerCentralityStore>,
    webgraph: Option<Webgraph>,
    topics: Option<human_website_annotations::Mapper>,
}

impl IndexingWorker {
    pub fn new(
        host_centrality_store_path: String,
        page_centrality_store_path: Option<String>,
        webgraph_path: Option<String>,
        topics_path: Option<String>,
    ) -> Self {
        Self {
            host_centrality_store: IndexerCentralityStore::open(host_centrality_store_path),
            page_centrality_store: page_centrality_store_path.map(IndexerCentralityStore::open),
            webgraph: webgraph_path.map(|path| WebgraphBuilder::new(path).open()),
            topics: topics_path.map(|path| human_website_annotations::Mapper::open(path).unwrap()),
        }
    }
}

pub fn process_job(job: &Job, worker: &IndexingWorker) -> Index {
    let name = job.warc_paths.first().unwrap().split('/').last().unwrap();

    info!("processing {}", name);

    let mut index = Index::open(Path::new(&job.base_path).join(name)).unwrap();

    let source: WarcSource = job.source_config.clone().into();

    let warc_files = download_all_warc_files(&job.warc_paths, &source, &job.base_path);
    pin!(warc_files);

    let current_timestamp = Utc::now().timestamp().max(0) as usize;

    for file in warc_files.by_ref() {
        let name = file.split('/').last().unwrap();
        let path = Path::new(&job.base_path).join("warc_files").join(name);

        if let Ok(file) = WarcFile::open(&path) {
            for record in
                file.records()
                    .flatten()
                    .filter(|record| match &record.response.payload_type {
                        Some(payload_type) => !matches!(payload_type.as_str(), "application/pdf"),
                        None => true,
                    })
            {
                let mut html = Html::parse_without_text(&record.response.body, &record.request.url);
                let node = Node::from(html.url());
                let node_id = worker
                    .host_centrality_store
                    .node2id
                    .get(&node.clone().into_host());

                let host_centrality = node_id
                    .and_then(|node_id| worker.host_centrality_store.harmonic.get(&node_id))
                    .unwrap_or_default();

                if let Some(host_centrality_threshold) = job.host_centrality_threshold {
                    if host_centrality < host_centrality_threshold {
                        debug!("skipping due to low host_centrality value");
                        continue;
                    }
                }

                html.parse_text();
                if let Some(minimum_clean_words) = job.minimum_clean_words {
                    match html.clean_text() {
                        Some(clean_text) => {
                            if clean_text.split_whitespace().count() < minimum_clean_words {
                                continue;
                            }
                        }
                        None => continue,
                    }
                }

                let backlinks: Vec<Link> = worker
                    .webgraph
                    .as_ref()
                    .map(|webgraph| {
                        webgraph
                            .ingoing_edges(Node::from(html.url()))
                            .into_iter()
                            .map(|edge| Link {
                                source: edge.from.name.into(),
                                destination: edge.to.name.into(),
                                text: edge.label,
                            })
                            .collect()
                    })
                    .unwrap_or_else(Vec::new);

                let mut page_centrality = 0.0;

                if let Some(store) = worker.page_centrality_store.as_ref() {
                    let node_id = store.node2id.get(&node);

                    page_centrality = node_id
                        .and_then(|node_id| store.harmonic.get(&node_id))
                        .unwrap_or_default();
                }

                let fetch_time_ms = record.metadata.fetch_time_ms as u64;

                trace!("inserting webpage: {:?}", html.url());

                trace!("title = {:?}", html.title());
                trace!("text = {:?}", html.clean_text());

                let node_id = worker
                    .host_centrality_store
                    .node2id
                    .get(&Node::from(html.url()).into_host());

                let mut dmoz_description = None;

                if let Some(mapper) = worker.topics.as_ref() {
                    if let Some(info) = mapper.get(&html.url().site().to_string()) {
                        dmoz_description = Some(info.description.clone())
                    }
                }

                let mut webpage = Webpage {
                    html,
                    backlinks,
                    page_centrality,
                    host_centrality,
                    fetch_time_ms,
                    pre_computed_score: 0.0,
                    node_id,
                    dmoz_description,
                };

                let mut signal_aggregator = SignalAggregator::new(None);
                signal_aggregator.set_current_timestamp(current_timestamp);

                webpage.pre_computed_score = signal_aggregator.precompute_score(&webpage);

                if let Err(err) = index.insert(webpage) {
                    debug!("{:?}", err);
                }
            }
        }

        index.commit().unwrap();

        std::fs::remove_file(path).unwrap();
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

impl Map<IndexingWorker, FrozenIndex> for Job {
    fn map(&self, worker: &IndexingWorker) -> FrozenIndex {
        let index = process_job(self, worker);
        index.into()
    }
}

impl Map<IndexingWorker, IndexPointer> for Job {
    fn map(&self, worker: &IndexingWorker) -> IndexPointer {
        let index = process_job(self, worker);
        IndexPointer(index.path)
    }
}

impl Reduce<FrozenIndex> for Index {
    fn reduce(self, element: FrozenIndex) -> Self {
        let other: Index = element.into();

        let other_path = other.path.clone();

        let res = self.merge(other);

        std::fs::remove_dir_all(other_path).unwrap();

        res
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

impl Indexer {
    pub fn run(config: &IndexingLocalConfig) -> Result<()> {
        let warc_paths = config.warc_source.paths()?;

        let job_config: JobConfig = config.warc_source.clone().into();

        let worker = IndexingWorker::new(
            config.host_centrality_store_path.clone(),
            config.page_centrality_store_path.clone(),
            config.page_webgraph_path.clone(),
            config.topics_path.clone(),
        );

        let executor = Executor::multi_thread("indexer").unwrap();

        let indexes = executor
            .map(
                |job| -> IndexPointer { job.map(&worker) },
                warc_paths
                    .into_iter()
                    .skip(config.skip_warc_files.unwrap_or(0))
                    .take(config.limit_warc_files.unwrap_or(usize::MAX))
                    .chunks(config.batch_size.unwrap_or(1))
                    .into_iter()
                    .map(|warc_paths| Job {
                        source_config: job_config.clone(),
                        warc_paths: warc_paths.collect_vec(),
                        host_centrality_threshold: config.host_centrality_threshold,
                        base_path: config
                            .output_path
                            .clone()
                            .unwrap_or_else(|| "data/index".to_string()),
                        minimum_clean_words: config.minimum_clean_words,
                    }),
            )
            .unwrap_or_default();

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

                for other in it {
                    let other_path = other.0;
                    let other = Index::open(&other_path).unwrap();
                    index = index.merge(other);

                    std::fs::remove_dir_all(other_path).unwrap();
                    index.inverted_index.merge_into_max_segments(1).unwrap();
                }

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
