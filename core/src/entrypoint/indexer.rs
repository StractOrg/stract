use chrono::Utc;
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
use futures::StreamExt;
use std::net::SocketAddr;
use std::path::Path;

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::pin;
use tracing::{debug, info, trace};

use crate::entrypoint::async_download_all_warc_files;
use crate::executor::Executor;
use crate::index::{FrozenIndex, Index};
use crate::mapreduce::{Manager, Map, Reduce, Worker};
use crate::ranking::centrality_store::IndexerCentralityStore;
use crate::ranking::SignalAggregator;
use crate::warc::WarcFile;
use crate::webgraph::{Node, Webgraph, WebgraphBuilder};
use crate::webpage::{Html, Link, Webpage};
use crate::{
    human_website_annotations, HttpConfig, IndexingLocalConfig, IndexingMasterConfig, LocalConfig,
    Result, WarcSource,
};

use super::crawl_stability::CrawlStability;

pub struct Indexer {}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum JobConfig {
    Http(HttpConfig),
    Local(LocalConfig),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Job {
    pub source_config: JobConfig,
    pub download_images: bool,
    pub warc_paths: Vec<String>,
    pub base_path: String,
    pub host_centrality_threshold: Option<f64>,
    pub minimum_clean_words: Option<usize>,
    pub max_num_segments: u32,
}

pub struct IndexingWorker {
    centrality_store: IndexerCentralityStore,
    webgraph: Option<Webgraph>,
    crawl_stabilty: Option<CrawlStability>,
    topics: Option<human_website_annotations::Mapper>,
}

impl IndexingWorker {
    pub fn new(
        centrality_store_path: String,
        webgraph_path: Option<String>,
        crawl_stability_path: Option<String>,
        topics_path: Option<String>,
    ) -> Self {
        Self {
            centrality_store: IndexerCentralityStore::open(centrality_store_path),
            webgraph: webgraph_path.map(|path| WebgraphBuilder::new(path).open()),
            crawl_stabilty: crawl_stability_path.map(CrawlStability::open),
            topics: topics_path.map(|path| human_website_annotations::Mapper::open(path).unwrap()),
        }
    }
}

async fn async_process_job(job: &Job, worker: &IndexingWorker) -> Index {
    let name = job.warc_paths.first().unwrap().split('/').last().unwrap();

    info!("processing {}", name);

    let mut index = Index::open(Path::new(&job.base_path).join(name)).unwrap();

    let source = match job.source_config.clone() {
        JobConfig::Http(config) => WarcSource::HTTP(config),
        JobConfig::Local(config) => WarcSource::Local(config),
    };

    let warc_files = async_download_all_warc_files(&job.warc_paths, &source, &job.base_path).await;
    pin!(warc_files);

    let current_timestamp = Utc::now().timestamp().max(0) as usize;

    while let Some(file) = warc_files.next().await {
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
                let node_id = worker.centrality_store.node2id.get(
                    &html
                        .url()
                        .host_without_specific_subdomains_and_query()
                        .to_string()
                        .into(),
                );

                let host_centrality = node_id
                    .and_then(|node_id| worker.centrality_store.harmonic.host.get(node_id))
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

                let crawl_stability = worker
                    .crawl_stabilty
                    .as_ref()
                    .and_then(|stability| stability.get(&html.url().site().to_string()))
                    .unwrap_or_default();

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

                let node_id = worker
                    .centrality_store
                    .node2id
                    .get(&html.url().without_protocol().to_lowercase().into());

                let page_centrality = node_id
                    .and_then(|node_id| worker.centrality_store.harmonic.full.get(node_id))
                    .unwrap_or_default();

                let fetch_time_ms = record.metadata.fetch_time_ms as u64;

                trace!("inserting webpage: {:?}", html.url());

                trace!("title = {:?}", html.title());
                trace!("text = {:?}", html.clean_text());

                let node_id = worker
                    .centrality_store
                    .node2id
                    .get(&Node::from_url(html.url()).into_host())
                    .cloned();

                let mut host_topic = None;
                let mut dmoz_description = None;

                if let Some(mapper) = worker.topics.as_ref() {
                    if let Some(info) = mapper.get(&html.url().site().to_string()) {
                        host_topic = Some(info.topic.clone());
                        dmoz_description = Some(info.description.clone())
                    }
                }

                let mut webpage = Webpage {
                    html,
                    backlinks,
                    page_centrality,
                    host_centrality,
                    fetch_time_ms,
                    primary_image: None,
                    pre_computed_score: 0.0,
                    node_id,
                    crawl_stability,
                    host_topic,
                    dmoz_description,
                };

                let mut signal_aggregator = SignalAggregator::new(None);
                signal_aggregator.set_current_timestamp(current_timestamp);

                webpage.pre_computed_score = signal_aggregator.precompute_score(&webpage);

                if let Err(err) = index.insert(webpage) {
                    debug!("{:?}", err);
                }
            }
            if job.download_images {
                info!("downloading images");
                index.download_pending_images();
            }
        }

        index.commit().unwrap();

        std::fs::remove_file(path).unwrap();
    }

    index
        .inverted_index
        .merge_into_max_segments(job.max_num_segments)
        .unwrap();

    info!("{} done", name);

    index
}

pub fn process_job(job: &Job, worker: &IndexingWorker) -> Index {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { async_process_job(job, worker).await })
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
    pub fn run_master(config: &IndexingMasterConfig) -> Result<()> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                info!("Running master for index construction");

                let warc_paths = config.warc_source.paths().unwrap();

                let workers: Vec<SocketAddr> = config
                    .workers
                    .iter()
                    .map(|worker| worker.parse().unwrap())
                    .collect();

                let job_config = match config.warc_source.clone() {
                    WarcSource::HTTP(config) => JobConfig::Http(config),
                    WarcSource::Local(config) => JobConfig::Local(config),
                };

                let warc_paths: Box<dyn Iterator<Item = Job> + Send> = Box::new(
                    warc_paths
                        .into_iter()
                        .skip(config.skip_num_warc_files.unwrap_or(0))
                        .take(config.limit_warc_files.unwrap_or(usize::MAX))
                        .chunks(config.batch_size.unwrap_or(1))
                        .into_iter()
                        .map(|warc_paths| Job {
                            source_config: job_config.clone(),
                            warc_paths: warc_paths.collect_vec(),
                            download_images: config.download_images.unwrap_or(true),
                            host_centrality_threshold: config.host_centrality_threshold,
                            base_path: config
                                .index_base_path
                                .clone()
                                .unwrap_or_else(|| "data/index".to_string()),
                            max_num_segments: config.final_num_segments.unwrap_or(20),
                            minimum_clean_words: config.minimum_clean_words,
                        })
                        .collect_vec()
                        .into_iter(),
                );

                let manager = Manager::new(&workers);
                let mut index: Index = manager
                    .run::<IndexingWorker, Job, FrozenIndex, Index>(warc_paths)
                    .await
                    .unwrap();

                index
                    .inverted_index
                    .merge_into_max_segments(config.final_num_segments.unwrap_or(20))
                    .unwrap();
            });

        Ok(())
    }

    pub fn run_worker(
        worker_addr: String,
        centrality_store_path: String,
        webgraph_path: Option<String>,
        crawl_stability_path: Option<String>,
        topics_path: Option<String>,
    ) -> Result<()> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                IndexingWorker::new(
                    centrality_store_path,
                    webgraph_path,
                    crawl_stability_path,
                    topics_path,
                )
                .run::<Job, FrozenIndex>(
                    worker_addr
                        .parse::<SocketAddr>()
                        .expect("Could not parse worker address"),
                )
                .await
                .unwrap();
            });
        Ok(())
    }

    pub fn run_locally(config: &IndexingLocalConfig) -> Result<()> {
        let warc_paths = config.warc_source.paths()?;

        let job_config = match config.warc_source.clone() {
            WarcSource::HTTP(config) => JobConfig::Http(config),
            WarcSource::Local(config) => JobConfig::Local(config),
        };

        let worker = IndexingWorker::new(
            config.centrality_store_path.clone(),
            config.webgraph_path.clone(),
            config.crawl_stability_path.clone(),
            config.topics_path.clone(),
        );

        let executor = Executor::multi_thread("indexer").unwrap();

        let indexes = executor
            .map(
                |job| -> IndexPointer { job.map(&worker) },
                warc_paths
                    .into_iter()
                    .skip(config.skip_num_warc_files.unwrap_or(0))
                    .take(config.limit_warc_files.unwrap_or(usize::MAX))
                    .chunks(config.batch_size.unwrap_or(1))
                    .into_iter()
                    .map(|warc_paths| Job {
                        source_config: job_config.clone(),
                        warc_paths: warc_paths.collect_vec(),
                        download_images: config.download_images.unwrap_or(true),
                        host_centrality_threshold: config.host_centrality_threshold,
                        base_path: config
                            .output_path
                            .clone()
                            .unwrap_or_else(|| "data/index".to_string()),
                        max_num_segments: config.final_num_segments.unwrap_or(20),
                        minimum_clean_words: config.minimum_clean_words,
                    }),
            )
            .unwrap_or_default();

        Self::merge(indexes, config.final_num_segments.unwrap_or(20))?;
        Ok(())
    }

    pub fn merge(indexes: Vec<IndexPointer>, num_segments: u32) -> Result<()> {
        let mut it = indexes.into_iter();
        let mut index = Index::open(it.next().unwrap().0)?;

        for other in it {
            let other_path = other.0;
            let other = Index::open(&other_path)?;
            index = index.merge(other);

            std::fs::remove_dir_all(other_path)?;
            index.inverted_index.merge_into_max_segments(num_segments)?;
        }

        Ok(())
    }
}
