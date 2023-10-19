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
    config::{self, WebgraphConstructConfig},
    entrypoint::download_all_warc_files,
    Result,
};
use itertools::Itertools;
use mapreduce::Worker;
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};
use tokio::pin;
use tracing::{info, trace};
use webgraph::{Node, WebgraphWriter};
use webpage::Html;

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
    pub config: JobConfig,
    pub warc_paths: Vec<String>,
}

pub fn open_host_graph_writer<P: AsRef<Path>>(path: P) -> webgraph::WebgraphWriter {
    WebgraphWriter::new(
        path,
        executor::Executor::single_thread(),
        webgraph::Compression::Lz4,
    )
}

pub fn open_page_graph_writer<P: AsRef<Path>>(path: P) -> webgraph::WebgraphWriter {
    WebgraphWriter::new(
        path,
        executor::Executor::single_thread(),
        webgraph::Compression::Lz4,
    )
}

pub struct WebgraphWorker {
    pub host_graph: webgraph::WebgraphWriter,
    pub page_graph: webgraph::WebgraphWriter,
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

                for link in webpage
                    .anchor_links()
                    .into_iter()
                    .filter(|link| matches!(link.destination.scheme(), "http" | "https"))
                {
                    let source = link.source.clone();
                    let destination = link.destination.clone();

                    trace!("inserting link {:?}", link);
                    let mut source = Node::from(source);

                    let mut destination = Node::from(destination);

                    self.page_graph
                        .insert(source.clone(), destination.clone(), link.text.clone());

                    source = source.into_host();
                    destination = destination.into_host();

                    self.host_graph.insert(source, destination, link.text);
                }
            }

            self.host_graph.commit();
            self.page_graph.commit();
        }

        info!("{} done", name);
    }
}

impl Worker for WebgraphWorker {}

pub struct Webgraph {}

impl Webgraph {
    pub fn run(config: &WebgraphConstructConfig) -> Result<()> {
        let warc_paths = config.warc_source.paths()?;

        let job_config = JobConfig::from(config.warc_source.clone());

        let jobs: Vec<_> = warc_paths
            .into_iter()
            .take(config.limit_warc_files.unwrap_or(usize::MAX))
            .chunks(config.batch_size.unwrap_or(1))
            .into_iter()
            .map(|warc_paths| Job {
                config: job_config.clone(),
                warc_paths: warc_paths.collect_vec(),
            })
            .collect_vec();

        let num_workers = num_cpus::get();

        let mut handlers = Vec::new();
        let host_path = &config.host_graph_base_path;
        let page_path = &config.page_graph_base_path;

        for i in 0..num_workers {
            let host_path = host_path.clone();
            let host_path = Path::new(&host_path);
            let host_path = host_path.join(format!("worker_{i}"));

            let page_path = page_path.clone();
            let page_path = Path::new(&page_path);
            let page_path = page_path.join(format!("worker_{i}"));

            let mut worker = WebgraphWorker {
                host_graph: open_host_graph_writer(host_path),
                page_graph: open_page_graph_writer(page_path),
            };

            let jobs = jobs.clone();
            handlers.push(std::thread::spawn(move || {
                for job in jobs.iter().skip(i).step_by(num_workers) {
                    worker.process_job(job);
                }

                (worker.host_graph.finalize(), worker.page_graph.finalize())
            }));
        }

        let mut graphs = Vec::new();
        for handler in handlers {
            graphs.push(handler.join().unwrap());
        }

        let (mut host_graph, mut page_graph) = graphs.pop().unwrap();

        for (other_host, other_page) in graphs {
            let other_host_path = other_host.path.clone();
            let other_page_path = other_page.path.clone();

            host_graph.merge(other_host);
            page_graph.merge(other_page);

            fs::remove_dir_all(other_host_path)?;
            fs::remove_dir_all(other_page_path)?;
        }

        Ok(())
    }
}
