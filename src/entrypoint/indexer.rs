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
use std::net::SocketAddr;
use std::path::Path;

use itertools::Itertools;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, trace};

use crate::entrypoint::download_all_warc_files;
use crate::index::{FrozenIndex, Index};
use crate::mapreduce::{Map, MapReduce, Reduce, Worker};
use crate::ranking::centrality_store::CentralityStore;
use crate::warc::WarcFile;
use crate::webgraph::{Node, Webgraph, WebgraphBuilder};
use crate::webpage::{Html, Link, Webpage};
use crate::{
    HttpConfig, IndexingLocalConfig, IndexingMasterConfig, LocalConfig, Result, WarcSource,
};

pub struct Indexer {}

#[derive(Debug, Serialize, Deserialize, Clone)]
enum JobConfig {
    Http(HttpConfig),
    Local(LocalConfig),
}

#[derive(Debug, Serialize, Deserialize)]
struct Job {
    config: JobConfig,
    warc_paths: Vec<String>,
    base_path: String,
}

struct IndexingWorker {
    centrality_store: CentralityStore,
    webgraph: Option<Webgraph>,
}

impl IndexingWorker {
    fn new(centrality_store_path: String, webgraph_path: Option<String>) -> Self {
        Self {
            centrality_store: CentralityStore::new(centrality_store_path),
            webgraph: webgraph_path.map(|path| {
                WebgraphBuilder::new(path)
                    .with_full_graph()
                    .with_host_graph()
                    .read_only(true)
                    .open()
            }),
        }
    }
}

impl Worker for IndexingWorker {}

impl Map<IndexingWorker, FrozenIndex> for Job {
    fn map(self, worker: &IndexingWorker) -> FrozenIndex {
        let name = self.warc_paths.first().unwrap().split('/').last().unwrap();

        info!("processing {}", name);

        let mut index = Index::open(Path::new(&self.base_path).join(name)).unwrap();

        let source = match self.config {
            JobConfig::Http(config) => WarcSource::HTTP(config),
            JobConfig::Local(config) => WarcSource::Local(config),
        };

        let warc_files = download_all_warc_files(&self.warc_paths, &source, &self.base_path);

        for file in warc_files {
            if let Ok(file) = WarcFile::open(&file) {
                for record in
                    file.records()
                        .flatten()
                        .filter(|record| match &record.response.payload_type {
                            Some(payload_type) => {
                                !matches!(payload_type.as_str(), "application/pdf")
                            }
                            None => true,
                        })
                {
                    let html = Html::parse(&record.response.body, &record.request.url);
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
                    let centrality = worker
                        .centrality_store
                        .get(html.url().host_without_specific_subdomains())
                        .unwrap_or_default();
                    let fetch_time_ms = record.metadata.fetch_time_ms as u64;

                    trace!("inserting webpage: {:?}", html.url());

                    trace!("title = {:?}", html.title());
                    trace!("text = {:?}", html.clean_text());

                    let webpage = Webpage {
                        html,
                        backlinks,
                        centrality,
                        fetch_time_ms,
                        primary_image_uuid: None,
                    };
                    if let Err(err) = index.insert(webpage) {
                        debug!("{:?}", err);
                    }
                }
                index.commit().unwrap();
                info!("downloading images");
                index.download_pending_images();
            }

            std::fs::remove_file(file).ok();
        }

        info!("{} done", name);

        index.into()
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
        info!("Running master for index construction");

        let warc_paths = config.warc_source.paths()?;

        let workers: Vec<SocketAddr> = config
            .workers
            .iter()
            .map(|worker| worker.parse().unwrap())
            .collect();

        let job_config = match config.warc_source.clone() {
            WarcSource::HTTP(config) => JobConfig::Http(config),
            WarcSource::Local(config) => JobConfig::Local(config),
        };

        let mut warc_paths: Box<dyn Iterator<Item = Job> + Send> = Box::new(
            warc_paths
                .into_iter()
                .chunks(config.batch_size.unwrap_or(1))
                .into_iter()
                .map(|warc_paths| Job {
                    config: job_config.clone(),
                    warc_paths: warc_paths.collect_vec(),
                    base_path: config
                        .index_base_path
                        .clone()
                        .unwrap_or_else(|| "data/index".to_string()),
                })
                .collect_vec()
                .into_iter(),
        );

        if let Some(limit) = config.limit_warc_files {
            warc_paths = Box::new(warc_paths.take(limit));
        }

        let _index: Index = warc_paths
            .map_reduce(&workers)
            .expect("failed to build index");

        Ok(())
    }

    pub fn run_worker(
        worker_addr: String,
        centrality_store_path: String,
        webgraph_path: Option<String>,
    ) -> Result<()> {
        IndexingWorker::new(centrality_store_path, webgraph_path).run::<Job, FrozenIndex>(
            worker_addr
                .parse::<SocketAddr>()
                .expect("Could not parse worker address"),
        )?;
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
        );

        warc_paths
            .into_iter()
            .take(config.limit_warc_files.unwrap_or(usize::MAX))
            .chunks(config.batch_size.unwrap_or(1))
            .into_iter()
            .map(|warc_paths| Job {
                config: job_config.clone(),
                warc_paths: warc_paths.collect_vec(),
                base_path: "data/index".to_string(),
            })
            .collect_vec()
            .into_par_iter()
            .panic_fuse()
            .map(|job| job.map(&worker))
            .map(Index::from)
            .map(Some)
            .reduce(
                || None,
                |a, b| match (a, b) {
                    (Some(a), Some(b)) => Some(a.reduce(b)),
                    (Some(graph), None) | (None, Some(graph)) => Some(graph),
                    (None, None) => None,
                },
            );

        Ok(())
    }
}
