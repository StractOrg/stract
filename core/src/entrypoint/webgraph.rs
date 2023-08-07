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
    config::WarcSource,
    config::WebgraphLevel,
    config::{self, WebgraphConstructConfig},
    crawler::crawl_db::RedirectDb,
    entrypoint::download_all_warc_files,
    mapreduce::Worker,
    webgraph::{self, Node, WebgraphBuilder},
    webpage::Html,
    Result,
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc};
use tokio::pin;
use tracing::{info, trace};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct GraphPointer {
    path: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Job {
    pub level: WebgraphLevel,
    pub config: JobConfig,
    pub warc_paths: Vec<String>,
    pub graph_base_path: String,
}

pub fn open_graph<P: AsRef<Path>>(path: P) -> webgraph::Webgraph {
    WebgraphBuilder::new(path)
        .commit_mode(webgraph::CommitMode::SingleSegment)
        .open()
}

pub struct WebgraphWorker {
    pub graph: webgraph::Webgraph,
    pub redirect: Option<Arc<RedirectDb>>,
}

impl WebgraphWorker {
    pub fn process_job(&mut self, job: &Job) {
        let name = job.warc_paths.first().unwrap().split('/').last().unwrap();

        info!("processing {}", name);

        let source = WarcSource::from(job.config.clone());

        let warc_files = download_all_warc_files(&job.warc_paths, &source, &job.graph_base_path);
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

                for link in webpage
                    .anchor_links()
                    .into_iter()
                    .filter(|link| matches!(link.destination.scheme(), "http" | "https"))
                    .filter(|link| link.source.domain() != link.destination.domain())
                    .filter(|link| {
                        link.source.domain().is_some() && link.destination.domain().is_some()
                    })
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

                    self.graph.insert(source, destination, link.text);
                }
            }

            self.graph.commit();
        }
        self.graph.merge_segments(1);

        info!("{} done", name);
    }
}

impl Worker for WebgraphWorker {}

pub struct Webgraph {}

impl Webgraph {
    pub fn run(config: &WebgraphConstructConfig) -> Result<()> {
        let warc_paths = config.warc_source.paths()?;

        let job_config = JobConfig::from(config.warc_source.clone());

        let redirect = match &config.redirect_db_path {
            Some(path) => Some(Arc::new(RedirectDb::open(path)?)),
            None => None,
        };

        let jobs: Vec<_> = warc_paths
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
            .collect_vec();

        let num_workers = num_cpus::get();

        let mut handlers = Vec::new();
        let parent_path = config
            .graph_base_path
            .clone()
            .unwrap_or_else(|| "data/webgraph".to_string());

        for i in 0..num_workers {
            let path = parent_path.clone();
            let path = Path::new(&path);
            let path = path.join(format!("worker_{i}"));

            let mut worker = WebgraphWorker {
                redirect: redirect.clone(),
                graph: open_graph(path),
            };

            let jobs = jobs.clone();
            handlers.push(std::thread::spawn(move || {
                for job in jobs.iter().skip(i).step_by(num_workers) {
                    worker.process_job(job);
                }

                worker.graph
            }));
        }

        let mut graphs = Vec::new();
        for handler in handlers {
            graphs.push(handler.join().unwrap());
        }

        let mut graph = graphs.pop().unwrap();
        for other_graph in graphs {
            graph.merge(other_graph);
        }

        graph.move_to(&parent_path);

        Ok(())
    }
}
