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
use crate::{
    directory::DirEntry,
    mapreduce::{Map, MapReduce, Reduce, StatelessWorker, Worker},
    warc::WarcFile,
    webgraph::{self, FrozenWebgraph, Node, WebgraphBuilder},
    webpage::Html,
    HttpConfig, LocalConfig, Result, WarcSource, WebgraphLocalConfig, WebgraphMasterConfig,
};
use rayon::prelude::ParallelBridge;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::Path};
use tracing::{debug, info, trace};

#[derive(Debug, Serialize, Deserialize, Clone)]
enum JobConfig {
    Http(HttpConfig),
    Local(LocalConfig),
}

#[derive(Debug, Serialize, Deserialize)]
struct Job {
    config: JobConfig,
    warc_path: String,
    graph_base_path: String,
}

impl Map<StatelessWorker, FrozenWebgraph> for Job {
    fn map(self, _worker: &StatelessWorker) -> FrozenWebgraph {
        let name = self.warc_path.split('/').last().unwrap();

        info!("processing {}", name);

        let mut graph = WebgraphBuilder::new(Path::new(&self.graph_base_path).join(name))
            .with_host_graph()
            .with_full_graph()
            .open();

        let source = match self.config {
            JobConfig::Http(config) => WarcSource::HTTP(config),
            JobConfig::Local(config) => WarcSource::Local(config),
        };

        debug!("downlooading warc file");
        let file = WarcFile::download(source, &self.warc_path).unwrap();
        debug!("finished downloading");

        for record in file.records().flatten() {
            let webpage = Html::parse(&record.response.body, &record.request.url);
            for link in webpage
                .links()
                .into_iter()
                .filter(|link| matches!(link.destination.protocol(), "http" | "https"))
                .filter(|link| link.source.domain() != link.destination.domain())
            {
                trace!("inserting link {:?}", link);
                graph.insert(
                    Node::from(link.source),
                    Node::from(link.destination),
                    link.text,
                );
            }
        }

        info!("{} done", name);

        graph.into()
    }
}

impl Reduce<FrozenWebgraph> for webgraph::Webgraph {
    fn reduce(mut self, other: FrozenWebgraph) -> webgraph::Webgraph {
        let other_path = match &other.root {
            DirEntry::Folder { name, entries: _ } | DirEntry::File { name, content: _ } => {
                name.clone()
            }
        };

        let other = other.into();

        self.merge(other);

        std::fs::remove_dir_all(other_path).unwrap();

        self
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

pub struct Webgraph {}

impl Webgraph {
    pub fn run_master(config: &WebgraphMasterConfig) -> Result<()> {
        info!("Running master for webgraph construction");

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

        let mut warc_paths: Box<dyn Iterator<Item = Job> + Send> =
            Box::new(warc_paths.into_iter().map(|warc_path| {
                Job {
                    config: job_config.clone(),
                    warc_path,
                    graph_base_path: config
                        .graph_base_path
                        .clone()
                        .unwrap_or_else(|| "data/webgraph".to_string()),
                }
            }));

        if let Some(limit) = config.limit_warc_files {
            warc_paths = Box::new(warc_paths.take(limit));
        }

        let _graph: webgraph::Webgraph = warc_paths
            .map_reduce(&workers)
            .expect("failed to build webgraph");

        Ok(())
    }

    pub fn run_worker(worker_addr: String) -> Result<()> {
        StatelessWorker::default().run::<Job, FrozenWebgraph>(
            worker_addr
                .parse::<SocketAddr>()
                .expect("Could not parse worker address"),
        )?;
        Ok(())
    }

    pub fn run_locally(config: &WebgraphLocalConfig) -> Result<()> {
        let warc_paths = config.warc_source.paths()?;

        let job_config = match config.warc_source.clone() {
            WarcSource::HTTP(config) => JobConfig::Http(config),
            WarcSource::Local(config) => JobConfig::Local(config),
        };
        let worker = StatelessWorker::default();

        warc_paths
            .into_iter()
            .take(config.limit_warc_files.unwrap_or(usize::MAX))
            .map(|path| Job {
                config: job_config.clone(),
                warc_path: path,
                graph_base_path: "data/webgraph".to_string(),
            })
            .par_bridge()
            .map(|job| job.map(&worker))
            .map(webgraph::Webgraph::from)
            .reduce_with(|a, b| a.reduce(b));

        Ok(())
    }
}
