// Stract is an open source web search engine.
// Copyright (C) 2024 Stract ApS
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

use crate::{
    canon_index::CanonicalIndex,
    config::{self, WarcSource, WebgraphConstructConfig},
    entrypoint::download_all_warc_files,
    webgraph::{self, Edge, Node, NodeID},
    webpage::Html,
    Result,
};
use itertools::Itertools;
use url::Url;

use std::{path::Path, sync::Arc};
use tokio::pin;
use tracing::{info, trace};

#[derive(Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Clone)]
pub enum JobConfig {
    Http(config::HttpConfig),
    Local(config::LocalConfig),
    S3(config::S3Config),
}

impl From<config::WarcSource> for JobConfig {
    fn from(value: config::WarcSource) -> Self {
        match value {
            config::WarcSource::HTTP(config) => JobConfig::Http(config),
            config::WarcSource::Local(config) => JobConfig::Local(config),
            config::WarcSource::S3(config) => JobConfig::S3(config),
        }
    }
}

impl From<JobConfig> for config::WarcSource {
    fn from(value: JobConfig) -> Self {
        match value {
            JobConfig::Http(config) => config::WarcSource::HTTP(config),
            JobConfig::Local(config) => config::WarcSource::Local(config),
            JobConfig::S3(config) => WarcSource::S3(config),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Clone)]
pub struct Job {
    pub config: JobConfig,
    pub warc_paths: Vec<String>,
}

fn canonical_or_self(index: &CanonicalIndex, url: Url) -> Url {
    if let Some(url) = index.get(&url).unwrap() {
        url
    } else {
        url
    }
}

pub struct WebgraphWorker {
    pub host_centrality_store: Option<Arc<speedy_kv::Db<NodeID, f64>>>,
    pub host_rank_store: Option<Arc<speedy_kv::Db<NodeID, u64>>>,
    pub graph: webgraph::Webgraph,
    pub canonical_index: Option<Arc<CanonicalIndex>>,
}

impl WebgraphWorker {
    pub fn process_job(&mut self, job: &Job) {
        let name = job.warc_paths.first().unwrap().split('/').last().unwrap();

        info!("processing {}", name);

        let source = WarcSource::from(job.config.clone());

        let warc_files = download_all_warc_files(&job.warc_paths, &source);
        pin!(warc_files);

        for file in warc_files.by_ref() {
            for record in file.records().flatten() {
                let webpage =
                    match Html::parse_without_text(&record.response.body, &record.request.url) {
                        Ok(webpage) => webpage,
                        Err(err) => {
                            tracing::error!("error parsing webpage: {}", err);
                            continue;
                        }
                    };

                let mut source = webpage.url().clone();
                if let Some(index) = &self.canonical_index {
                    source = canonical_or_self(index, source);
                }

                let source = Node::from(source);
                let source_centrality = self
                    .host_centrality_store
                    .as_ref()
                    .and_then(|store| store.get(&source.clone().into_host().id()).unwrap())
                    .unwrap_or(0.0);
                let source_rank = self
                    .host_rank_store
                    .as_ref()
                    .and_then(|store| store.get(&source.clone().into_host().id()).unwrap())
                    .unwrap_or(u64::MAX);

                let num_outgoing_hosts_from_page = webpage
                    .anchor_links()
                    .into_iter()
                    .filter_map(|l| l.destination.host_str().map(|h| h.to_string()))
                    .unique()
                    .count() as u64;

                for mut link in webpage.anchor_links().into_iter() {
                    let mut destination = link.destination.clone();

                    if let Some(index) = &self.canonical_index {
                        destination = canonical_or_self(index, destination);
                    }

                    link.text = link.text.chars().take(128).collect();

                    let destination = Node::from(destination);

                    let destination_centrality = self
                        .host_centrality_store
                        .as_ref()
                        .and_then(|store| store.get(&destination.clone().into_host().id()).unwrap())
                        .unwrap_or(0.0);
                    let destination_rank = self
                        .host_rank_store
                        .as_ref()
                        .and_then(|store| store.get(&destination.clone().into_host().id()).unwrap())
                        .unwrap_or(u64::MAX);

                    trace!("inserting link {:?}", link);
                    self.graph
                        .insert(Edge {
                            from: source.clone(),
                            to: destination,
                            rel_flags: link.rel,
                            label: link.text,
                            sort_score: source_centrality + destination_centrality,
                            from_centrality: source_centrality,
                            to_centrality: destination_centrality,
                            from_rank: source_rank,
                            to_rank: destination_rank,
                            num_outgoing_hosts_from_page,
                        })
                        .unwrap();
                }
            }

            self.graph.commit().unwrap();
        }

        info!("{} done", name);
    }
}

pub struct Webgraph {}

impl Webgraph {
    pub fn run(config: &WebgraphConstructConfig) -> Result<()> {
        let warc_paths = config.warc_source.paths()?;

        let job_config = JobConfig::from(config.warc_source.clone());

        let jobs: Vec<_> = warc_paths
            .into_iter()
            .skip(config.skip_warc_files.unwrap_or(0))
            .take(config.limit_warc_files.unwrap_or(usize::MAX))
            .chunks(config.batch_size.unwrap_or(1))
            .into_iter()
            .map(|warc_paths| Job {
                config: job_config.clone(),
                warc_paths: warc_paths.collect_vec(),
            })
            .collect_vec();

        let canonical_index = if let Some(index_path) = &config.canonical_index_path {
            Some(Arc::new(CanonicalIndex::open(index_path)?))
        } else {
            None
        };

        let host_centrality_store = Arc::new(speedy_kv::Db::open_or_create(
            &config.host_centrality_store_path,
        )?);

        let host_rank_store =
            Arc::new(speedy_kv::Db::open_or_create(&config.host_rank_store_path)?);

        let num_workers = usize::from(std::thread::available_parallelism()?);

        let mut handlers = Vec::new();
        let graph_path = &config.graph_base_path;

        const MAX_FINALIZE_CONCURRENT: usize = 4;
        let (s, r) = crossbeam_channel::bounded(MAX_FINALIZE_CONCURRENT);

        for _ in 0..MAX_FINALIZE_CONCURRENT {
            s.send(())?;
        }

        for i in 0..num_workers {
            let graph_path = graph_path.clone();
            let graph_path = Path::new(&graph_path).join(format!("worker_{i}"));

            let mut worker = WebgraphWorker {
                graph: webgraph::Webgraph::open(graph_path, config.shard)?,
                host_centrality_store: Some(host_centrality_store.clone()),
                host_rank_store: Some(host_rank_store.clone()),
                canonical_index: canonical_index.clone(),
            };

            let jobs = jobs.clone();
            let (s, r) = (s.clone(), r.clone());
            handlers.push(std::thread::spawn(move || {
                for job in jobs.iter().skip(i).step_by(num_workers) {
                    worker.process_job(job);
                }

                r.recv().unwrap();

                worker.graph.commit().unwrap();
                worker.graph.optimize_read().unwrap();

                s.send(()).unwrap();
                worker.graph
            }));
        }

        let mut graphs = Vec::new();
        for handler in handlers {
            graphs.push(handler.join().unwrap());
        }

        let mut graph = graphs.pop().unwrap();

        for other in graphs {
            graph.merge(other)?;
        }

        if config.merge_all_segments {
            graph.optimize_read().unwrap();
        }
        crate::mv(graph.path(), &config.graph_base_path)?;

        Ok(())
    }
}
