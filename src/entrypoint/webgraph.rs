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
    mapreduce::{Map, Reduce},
    webgraph::{SledStore, Webgraph},
    Result, WarcSource, WebgraphConfig, WebgraphMasterConfig, WebgraphWorkerConfig,
};
use serde::{Deserialize, Serialize};

pub struct WebgraphBuilder {
    config: WebgraphConfig,
}

impl From<WebgraphConfig> for WebgraphBuilder {
    fn from(config: WebgraphConfig) -> Self {
        Self { config }
    }
}

#[derive(Serialize, Deserialize)]
struct SingleJob {
    warc_source: WarcSource,
    warc_path: String,
}

#[derive(Serialize, Deserialize)]
struct Job(Vec<SingleJob>);

impl Map<Webgraph<SledStore>> for Job {
    fn map(self) -> Webgraph<SledStore> {
        todo!()
    }
}

impl Reduce<Webgraph<SledStore>> for Webgraph<SledStore> {
    fn reduce(mut self, other: Webgraph<SledStore>) -> Webgraph<SledStore> {
        self.merge(other);
        self
    }
}

impl WebgraphBuilder {
    fn run_master(config: &WebgraphMasterConfig) -> Result<()> {
        todo!();
    }

    fn run_worker(config: &WebgraphWorkerConfig) -> Result<()> {
        todo!();
    }

    pub fn run(&self) -> Result<()> {
        match &self.config {
            WebgraphConfig::Master(config) => WebgraphBuilder::run_master(config),
            WebgraphConfig::Worker(config) => WebgraphBuilder::run_worker(config),
        }
    }
}
