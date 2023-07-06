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
use crate::{
    crawler::crawl_db::RedirectDb,
    entrypoint::download_all_warc_files,
    mapreduce::{Map, Reduce, Worker},
    warc::WarcFile,
    webgraph::{self, FrozenWebgraph, Node, WebgraphBuilder},
    webpage::Html,
    HttpConfig, LocalConfig, Result, S3Config, WarcSource, WebgraphConstructConfig, WebgraphLevel,
};
use itertools::Itertools;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{path::Path, thread};
use tokio::pin;
use tracing::{info, trace};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct GraphPointer {
    path: String,
}

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
    pub level: WebgraphLevel,
    pub config: JobConfig,
    pub warc_paths: Vec<String>,
    pub graph_base_path: String,
}

fn open_graph<P: AsRef<Path>>(path: P) -> webgraph::Webgraph {
    WebgraphBuilder::new(path).open()
}

pub struct WebgraphWorker {
    pub redirect: Option<RedirectDb>,
}

impl WebgraphWorker {
    pub fn process_job(&self, job: &Job) -> webgraph::Webgraph {
        let name = job.warc_paths.first().unwrap().split('/').last().unwrap();

        info!("processing {}", name);

        let mut graph = open_graph(Path::new(&job.graph_base_path).join(name));

        let source = WarcSource::from(job.config.clone());

        let warc_files = download_all_warc_files(&job.warc_paths, &source, &job.graph_base_path);
        pin!(warc_files);

        for warc_path in warc_files.by_ref() {
            let name = warc_path.split('/').last().unwrap();
            let path = Path::new(&job.graph_base_path)
                .join("warc_files")
                .join(name);

            if let Ok(file) = WarcFile::open(&path) {
                for record in file.records().flatten() {
                    let webpage =
                        Html::parse_without_text(&record.response.body, &record.request.url);
                    for link in webpage
                        .anchor_links()
                        .into_iter()
                        .filter(|link| matches!(link.destination.protocol(), "http" | "https"))
                        .filter(|link| link.source.domain() != link.destination.domain())
                        .filter(|link| link.matches_url_regex())
                    {
                        let source = link.source.clone();
                        let mut destination = link.destination.clone();

                        if let Some(redirect) = &self.redirect {
                            if let Some(new_destination) = redirect.get(&destination).unwrap() {
                                trace!("redirecting {:?} to {:?}", destination, new_destination);
                                destination = new_destination;
                            }
                        }

                        if source.domain() == destination.domain() {
                            continue;
                        }

                        trace!("inserting link {:?}", link);
                        let mut source = Node::from(source);

                        let mut destination = Node::from(destination);

                        if let WebgraphLevel::Host = job.level {
                            source = source.into_host();
                            destination = destination.into_host();
                        }

                        graph.insert(source, destination, link.text);
                    }
                }
            }

            graph.commit();

            std::fs::remove_file(path).unwrap();
        }
        graph.merge_segments(1);

        info!("{} done", name);

        graph
    }
}

impl Worker for WebgraphWorker {}

impl Map<WebgraphWorker, FrozenWebgraph> for Job {
    fn map(&self, worker: &WebgraphWorker) -> FrozenWebgraph {
        let graph = worker.process_job(self);
        graph.into()
    }
}

impl Map<WebgraphWorker, GraphPointer> for Job {
    fn map(&self, worker: &WebgraphWorker) -> GraphPointer {
        let graph = worker.process_job(self);
        GraphPointer { path: graph.path }
    }
}

impl Reduce<FrozenWebgraph> for webgraph::Webgraph {
    fn reduce(self, other: FrozenWebgraph) -> webgraph::Webgraph {
        let other: webgraph::Webgraph = other.into();
        self.reduce(other)
    }
}

impl Reduce<webgraph::Webgraph> for webgraph::Webgraph {
    fn reduce(mut self, element: webgraph::Webgraph) -> Self {
        let other_path = element.path.clone();

        self.merge(element);

        std::fs::remove_dir_all(other_path).unwrap();
        self
    }
}

impl Reduce<GraphPointer> for GraphPointer {
    fn reduce(self, other: GraphPointer) -> Self {
        let self_clone = self.clone();

        {
            let graph = open_graph(self.path);
            let other_graph = open_graph(other.path);

            let _ = graph.reduce(other_graph);
        }

        self_clone
    }
}

impl Reduce<GraphPointer> for webgraph::Webgraph {
    fn reduce(self, other: GraphPointer) -> Self {
        let other = open_graph(other.path);
        self.reduce(other)
    }
}

pub struct Webgraph {}

impl Webgraph {
    pub fn run(config: &WebgraphConstructConfig) -> Result<()> {
        let warc_paths = config.warc_source.paths()?;

        let job_config = JobConfig::from(config.warc_source.clone());

        let redirect = match &config.redirect_db_path {
            Some(path) => Some(RedirectDb::open(path)?),
            None => None,
        };

        let worker = WebgraphWorker { redirect };

        let graphs: Vec<_> = warc_paths
            .into_iter()
            .take(config.limit_warc_files.unwrap_or(usize::MAX))
            .chunks(config.batch_size.unwrap_or(1))
            .into_iter()
            .map(|warc_paths| Job {
                config: job_config.clone(),
                level: config.level.clone(),
                warc_paths: warc_paths.collect_vec(),
                graph_base_path: config
                    .graph_base_path
                    .clone()
                    .unwrap_or_else(|| "data/webgraph".to_string()),
            })
            .collect_vec()
            .into_par_iter()
            .map(|job| -> GraphPointer { job.map(&worker) })
            .collect();

        if graphs.len() > 1 {
            Self::merge(graphs);
        }

        Ok(())
    }

    fn merge(graphs: Vec<GraphPointer>) {
        let num_graphs = graphs.len();
        let mut it = graphs.into_iter();
        let num_cores = num_cpus::get();

        let mut threads = Vec::new();

        for _ in 0..(num_cores + 1) {
            let graphs = it
                .by_ref()
                .take(((num_graphs as f64) / (num_cores as f64)).ceil() as usize)
                .collect_vec();

            if graphs.is_empty() {
                break;
            }

            threads.push(thread::spawn(move || {
                let mut it = graphs.into_iter();
                let mut graph = open_graph(it.next().unwrap().path);

                for other in it {
                    graph = graph.reduce(other);
                }
                graph.merge_segments(1);

                graph
            }));
        }

        let mut graphs = Vec::new();
        for thread in threads {
            graphs.push(thread.join().unwrap());
        }

        let mut graph = graphs.pop().unwrap();

        for other in graphs {
            graph = graph.reduce(other);
        }
    }
}
