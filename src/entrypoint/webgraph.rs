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
    mapreduce::{Map, MapReduce, Reduce, Worker},
    warc::WarcFile,
    webgraph::{Node, SledStore, Webgraph},
    webpage::{self, Html},
    HttpConfig, LocalConfig, Result, WarcSource, WebgraphConfig, WebgraphLocalConfig,
    WebgraphMasterConfig, WebgraphWorkerConfig,
};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::Path};
use tracing::{debug, info};

pub struct WebgraphBuilder {
    config: WebgraphConfig,
}

impl From<WebgraphConfig> for WebgraphBuilder {
    fn from(config: WebgraphConfig) -> Self {
        Self { config }
    }
}

#[derive(Serialize, Deserialize, Clone)]
enum JobConfig {
    HTTP(HttpConfig),
    Local(LocalConfig),
}

#[derive(Serialize, Deserialize)]
struct Job {
    config: JobConfig,
    warc_path: String,
}

impl Map<Webgraph> for Job {
    fn map(self) -> Webgraph {
        let name = self.warc_path.split('/').last().unwrap();

        info!("processing {}", name);

        let mut graph = Webgraph::<SledStore>::open(Path::new("webgraph").join(name));

        let source = match self.config {
            JobConfig::HTTP(config) => WarcSource::HTTP(config),
            JobConfig::Local(config) => WarcSource::Local(config),
        };

        let file =
            futures::executor::block_on(WarcFile::download(source, &self.warc_path)).unwrap();

        for record in file.records().flatten() {
            let webpage = Html::parse(&record.response.body, &record.request.url);
            for link in webpage
                .links()
                .into_iter()
                .filter(|link| {
                    link.destination.starts_with("http://")
                        || link.destination.starts_with("https://")
                })
                .filter(|link| webpage::domain(&link.source) != webpage::domain(&link.destination))
            {
                debug!("inserting link {:?}", link);
                graph.insert(
                    Node::from(link.source),
                    Node::from(link.destination),
                    link.text,
                );
            }
        }

        info!("{} done", name);

        graph
    }
}

impl Reduce<Webgraph> for Webgraph {
    fn reduce(mut self, other: Webgraph) -> Webgraph {
        self.merge(other);
        self
    }
}

impl WebgraphBuilder {
    async fn run_master(config: &WebgraphMasterConfig) -> Result<()> {
        info!("Running master for webgraph construction");

        let warc_paths = config.warc_source.paths()?;

        let workers: Vec<SocketAddr> = config
            .workers
            .iter()
            .map(|worker| worker.parse().unwrap())
            .collect();

        let job_config = match config.warc_source.clone() {
            WarcSource::S3(_) => todo!("s3 not supported yet"),
            WarcSource::HTTP(config) => JobConfig::HTTP(config),
            WarcSource::Local(config) => JobConfig::Local(config),
        };

        warc_paths
            .into_iter()
            .map(|warc_path| Job {
                config: job_config.clone(),
                warc_path,
            })
            .map_reduce(&workers)
            .await
            .expect("failed to build webgraph");

        Ok(())
    }

    async fn run_worker(config: &WebgraphWorkerConfig) -> Result<()> {
        Worker::run::<Job, Webgraph>(config.addr.parse::<SocketAddr>().unwrap()).await?;
        Ok(())
    }

    fn run_locally(config: &WebgraphLocalConfig) -> Result<()> {
        let warc_paths = config.warc_source.paths()?;

        let job_config = match config.warc_source.clone() {
            WarcSource::S3(_) => todo!("s3 not supported yet"),
            WarcSource::HTTP(config) => JobConfig::HTTP(config),
            WarcSource::Local(config) => JobConfig::Local(config),
        };

        warc_paths
            .into_iter()
            .map(|path| Job {
                config: job_config.clone(),
                warc_path: path,
            })
            .map(|job| job.map())
            .fold(None, |acc: Option<Webgraph>, elem| match acc {
                Some(acc) => Some(acc.reduce(elem)),
                None => Some(elem),
            });

        Ok(())
    }

    pub async fn run(&self) -> Result<()> {
        match &self.config {
            WebgraphConfig::Master(config) => WebgraphBuilder::run_master(config).await,
            WebgraphConfig::Worker(config) => WebgraphBuilder::run_worker(config).await,
            WebgraphConfig::Local(config) => WebgraphBuilder::run_locally(config),
        }
    }
}
