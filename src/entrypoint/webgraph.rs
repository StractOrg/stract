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
    HttpConfig, Result, WarcSource, WebgraphConfig, WebgraphMasterConfig, WebgraphWorkerConfig,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{self, BufRead},
    net::SocketAddr,
    path::Path,
};
use tracing::{debug, info};

pub struct WebgraphBuilder {
    config: WebgraphConfig,
}

impl From<WebgraphConfig> for WebgraphBuilder {
    fn from(config: WebgraphConfig) -> Self {
        Self { config }
    }
}

#[derive(Serialize, Deserialize)]
struct Job {
    http_config: HttpConfig,
    warc_path: String,
}

impl Map<Webgraph<SledStore>> for Job {
    fn map(self) -> Webgraph<SledStore> {
        let name = self.warc_path.split("/").last().unwrap();

        info!("processing {}", name);

        let mut graph = Webgraph::<SledStore>::open(Path::new("webgraph").join(name));

        let file = futures::executor::block_on(WarcFile::download(
            WarcSource::HTTP(self.http_config),
            &self.warc_path,
        ))
        .unwrap();

        for record in file.records() {
            if let Ok(record) = record {
                let webpage = Html::parse(&record.response.body, &record.request.url);
                for link in webpage
                    .links()
                    .into_iter()
                    .filter(|link| {
                        link.destination.starts_with("http://")
                            || link.destination.starts_with("https://")
                    })
                    .filter(|link| {
                        webpage::domain(&link.source) != webpage::domain(&link.destination)
                    })
                {
                    debug!("inserting link {:?}", link);
                    graph.insert(
                        Node::from(link.source),
                        Node::from(link.destination),
                        link.text,
                    );
                }
            }
        }

        info!("{} done", name);

        graph
    }
}

impl Reduce<Webgraph<SledStore>> for Webgraph<SledStore> {
    fn reduce(mut self, other: Webgraph<SledStore>) -> Webgraph<SledStore> {
        self.merge(other);
        self
    }
}

impl WebgraphBuilder {
    async fn run_master(config: &WebgraphMasterConfig) -> Result<()> {
        info!("Running master for webgraph construction");

        let file = File::open(&config.warc_paths_file)?;
        let mut warc_paths = Vec::new();

        for line in io::BufReader::new(file).lines() {
            warc_paths.push(line?);
        }

        let workers: Vec<SocketAddr> = config
            .workers
            .iter()
            .map(|worker| worker.parse().unwrap())
            .collect();

        let http_config = if let WarcSource::HTTP(http_config) = config.warc_source.clone() {
            Some(http_config)
        } else {
            None
        };

        let http_config = http_config.unwrap();

        warc_paths
            .into_iter()
            .map(|warc_path| Job {
                http_config: http_config.clone(),
                warc_path,
            })
            .map_reduce(&workers)
            .await
            .expect("failed to build webgraph");

        Ok(())
    }

    async fn run_worker(config: &WebgraphWorkerConfig) -> Result<()> {
        Worker::run::<Job, Webgraph<SledStore>>(config.addr.parse::<SocketAddr>().unwrap()).await?;
        Ok(())
    }

    pub async fn run(&self) -> Result<()> {
        match &self.config {
            WebgraphConfig::Master(config) => WebgraphBuilder::run_master(config).await,
            WebgraphConfig::Worker(config) => WebgraphBuilder::run_worker(config).await,
        }
    }
}
