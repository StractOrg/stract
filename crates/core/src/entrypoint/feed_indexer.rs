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

use std::{
    path::{Path, PathBuf},
    thread,
};

use anyhow::Result;
use itertools::Itertools;
use rayon::prelude::*;

use tracing::info;
use url::Url;

use crate::{
    config::{FeedIndexingConfig, WarcSource},
    feed::{index::FeedIndex, Feed, FeedKind},
    warc::{PayloadType, WarcFile},
};

#[derive(Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct Job {
    pub source_config: WarcSource,
    pub warc_path: String,
    pub base_path: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct IndexPointer(PathBuf);

impl From<String> for IndexPointer {
    fn from(path: String) -> Self {
        IndexPointer(Path::new(&path).to_path_buf())
    }
}

pub struct IndexingWorker {}
impl IndexingWorker {
    fn process_job(&self, job: &Job) -> FeedIndex {
        let name = job.warc_path.split('/').last().unwrap().to_string();
        info!("processing {}", name);

        let mut index = FeedIndex::open(Path::new(&job.base_path).join(&name)).unwrap();

        if let Ok(file) = WarcFile::download(&job.source_config, &job.warc_path) {
            for record in
                file.records()
                    .flatten()
                    .filter(|record| match &record.response.payload_type {
                        Some(payload_type) => {
                            matches!(payload_type, PayloadType::Rss | PayloadType::Atom)
                        }
                        None => false,
                    })
            {
                let kind = match &record.response.payload_type {
                    Some(payload_type) => match payload_type {
                        PayloadType::Rss => FeedKind::Rss,
                        PayloadType::Atom => FeedKind::Atom,
                        _ => unreachable!(),
                    },
                    None => unreachable!(),
                };

                let url = Url::parse(&record.request.url);

                if url.is_err() {
                    continue;
                }

                let feed = Feed {
                    url: url.unwrap(),
                    kind,
                };

                index.insert(&feed).unwrap();
            }

            index.commit().unwrap();
            index.merge_into_max_segments(1).unwrap();
        }

        info!("{} done", name);

        index
    }
}

pub fn build(config: FeedIndexingConfig) -> Result<()> {
    let warc_paths = config.warc_source.paths()?;

    let job_config: WarcSource = config.warc_source.clone();

    let worker = IndexingWorker {};

    let indexes = warc_paths
        .into_par_iter()
        .skip(config.skip_warc_files.unwrap_or(0))
        .take(config.limit_warc_files.unwrap_or(usize::MAX))
        .map(|warc_path| Job {
            source_config: job_config.clone(),
            warc_path,
            base_path: config.output_path.clone(),
        })
        .map(|job| IndexPointer(worker.process_job(&job).path))
        .collect();

    merge(indexes)?;
    Ok(())
}

pub fn merge(indexes: Vec<IndexPointer>) -> Result<()> {
    let num_indexes = indexes.len();
    let mut it = indexes.into_iter();
    let num_cores = num_cpus::get();

    let mut threads = Vec::new();

    for _ in 0..(num_cores + 1) {
        let indexes = it
            .by_ref()
            .take(((num_indexes as f64) / (num_cores as f64)).ceil() as usize)
            .collect_vec();

        if indexes.is_empty() {
            break;
        }

        threads.push(thread::spawn(move || {
            let mut it = indexes.into_iter();
            let mut index = FeedIndex::open(it.next().unwrap().0).unwrap();

            for other in it {
                let other_path = other.0;
                let other = FeedIndex::open(&other_path).unwrap();
                index = index.merge(other);

                std::fs::remove_dir_all(other_path).unwrap();
            }

            index.merge_into_max_segments(1).unwrap();

            index
        }));
    }

    let mut indexes = Vec::new();
    for thread in threads {
        indexes.push(thread.join().unwrap());
    }

    let mut it = indexes.into_iter();
    let mut index = it.next().unwrap();

    for other in it {
        let other_path = other.path.clone();
        index = index.merge(other);
        std::fs::remove_dir_all(other_path).unwrap();
    }

    Ok(())
}
