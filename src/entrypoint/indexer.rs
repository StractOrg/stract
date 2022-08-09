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

use serde::{Deserialize, Serialize};
use tracing::{debug, info, trace};

use crate::index::{FrozenIndex, Index};
use crate::mapreduce::{Map, MapReduce, Reduce, Worker};
use crate::ranking::centrality_store::CentralityStore;
use crate::warc::WarcFile;
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
    warc_path: String,
    base_path: String,
}

struct IndexingWorker {
    centrality_store: CentralityStore,
}

impl IndexingWorker {
    fn new(centrality_store_path: String) -> Self {
        Self {
            centrality_store: CentralityStore::new(centrality_store_path),
        }
    }
}

impl Worker for IndexingWorker {}

impl Map<IndexingWorker, FrozenIndex> for Job {
    fn map(self, worker: &IndexingWorker) -> FrozenIndex {
        let name = self.warc_path.split('/').last().unwrap();

        info!("processing {}", name);

        let mut index = Index::open(Path::new(&self.base_path).join(name)).unwrap();

        let source = match self.config {
            JobConfig::Http(config) => WarcSource::HTTP(config),
            JobConfig::Local(config) => WarcSource::Local(config),
        };

        debug!("downlooading warc file");
        let file = WarcFile::download(source, &self.warc_path).unwrap();
        debug!("finished downloading");

        for record in file.records().flatten().take(2000) {
            let html = Html::parse(&record.response.body, &record.request.url);
            let backlinks: Vec<Link> = Vec::new(); // TODO: lookup backlinks in full webgraph
            let centrality = worker.centrality_store.get(html.host()).unwrap_or_default();
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
                if record.response.body.is_empty() {
                    continue;
                }
                if let Some(payload) = &record.response.payload_type {
                    if payload == "application/pdf" {
                        continue;
                    }
                }

                debug!("{:?}", err);
                // these url's actually have errors (no title etc)
                // if matches!(
                //     record.request.url.as_str(),
                //     "http://ain.liberzic.com/agenda-date-26-5-2008.html"
                // ) {
                //     continue;
                // }
                // dbg!(&record);
                // println!();
                // println!();
                // println!("{}", record.response.body);
                // panic!();
            }
        }
        index.commit().unwrap();
        index.merge_all_segments().unwrap();

        info!("{} done", name);

        index.into()
    }
}

impl Reduce<FrozenIndex> for Index {
    fn reduce(self, element: FrozenIndex) -> Self {
        let other = element.into();

        self.merge(other)
    }
}

impl Reduce<Index> for Index {
    fn reduce(self, element: Index) -> Self {
        self.merge(element)
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
            WarcSource::S3(_) => todo!("s3 not supported yet"),
            WarcSource::HTTP(config) => JobConfig::Http(config),
            WarcSource::Local(config) => JobConfig::Local(config),
        };

        let mut warc_paths: Box<dyn Iterator<Item = Job> + Send> =
            Box::new(warc_paths.into_iter().map(|warc_path| {
                Job {
                    config: job_config.clone(),
                    warc_path,
                    base_path: config
                        .index_base_path
                        .clone()
                        .unwrap_or_else(|| "data/index".to_string()),
                }
            }));

        if let Some(limit) = config.limit_warc_files {
            warc_paths = Box::new(warc_paths.take(limit));
        }

        let _index: Index = warc_paths
            .map_reduce(&workers)
            .expect("failed to build index");

        Ok(())
    }

    pub fn run_worker(worker_addr: String, centrality_store_path: String) -> Result<()> {
        IndexingWorker::new(centrality_store_path).run::<Job, FrozenIndex>(
            worker_addr
                .parse::<SocketAddr>()
                .expect("Could not parse worker address"),
        )?;
        Ok(())
    }

    pub fn run_locally(config: &IndexingLocalConfig) -> Result<()> {
        let warc_paths = config.warc_source.paths()?;

        let job_config = match config.warc_source.clone() {
            WarcSource::S3(_) => todo!("s3 not supported yet"),
            WarcSource::HTTP(config) => JobConfig::Http(config),
            WarcSource::Local(config) => JobConfig::Local(config),
        };

        let worker = IndexingWorker::new(config.centrality_store_path.clone());

        warc_paths
            .into_iter()
            .map(|path| Job {
                config: job_config.clone(),
                warc_path: path,
                base_path: "data/index".to_string(),
            })
            .map(|job| job.map(&worker))
            .fold(None, |acc: Option<Index>, elem: FrozenIndex| match acc {
                Some(acc) => Some(acc.reduce(elem)),
                None => Some(elem.into()),
            });

        Ok(())
    }
}
