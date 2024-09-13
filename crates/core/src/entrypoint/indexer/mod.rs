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

pub mod indexable_webpage;
pub mod job;
pub mod worker;

use rayon::prelude::*;
use std::thread;

use itertools::Itertools;

pub use crate::entrypoint::indexer::indexable_webpage::IndexableWebpage;
pub use crate::entrypoint::indexer::job::{Job, JobSettings};
pub use crate::entrypoint::indexer::worker::IndexingWorker;

use crate::config::{self, WarcSource};
use crate::index::Index;
use crate::Result;

#[derive(Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct IndexPointer(String);

impl From<String> for IndexPointer {
    fn from(path: String) -> Self {
        IndexPointer(path)
    }
}

pub fn run(config: &config::IndexerConfig) -> Result<()> {
    let warc_paths = config.warc_source.paths()?;

    let job_config: WarcSource = config.warc_source.clone();

    let worker = IndexingWorker::new(config.clone().into());

    let indexes = warc_paths
        .into_par_iter()
        .skip(config.skip_warc_files.unwrap_or(0))
        .take(config.limit_warc_files.unwrap_or(usize::MAX))
        .map(|warc_path| Job {
            source_config: job_config.clone(),
            warc_path,
            base_path: config.output_path.clone(),
            settings: JobSettings {
                host_centrality_threshold: config.host_centrality_threshold,
                minimum_clean_words: config.minimum_clean_words,
                batch_size: config.batch_size,
                autocommit_after_num_inserts: config.autocommit_after_num_inserts,
            },
        })
        .map(|job| {
            IndexPointer(
                job.process(&worker)
                    .path()
                    .as_os_str()
                    .to_str()
                    .unwrap()
                    .to_string(),
            )
        })
        .collect();

    let index = merge(indexes)?;
    crate::mv(index.path(), &config.output_path)?;

    Ok(())
}

pub fn merge(indexes: Vec<IndexPointer>) -> Result<Index> {
    let num_indexes = indexes.len();
    let mut it = indexes.into_iter();
    let num_cores = usize::from(std::thread::available_parallelism()?);

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
            let mut index = Index::open(it.next().unwrap().0).unwrap();

            for other in it {
                let other_path = other.0;
                let other = Index::open(&other_path).unwrap();

                index = index.merge(other);

                std::fs::remove_dir_all(other_path).unwrap();
            }

            index.inverted_index.merge_into_max_segments(1).unwrap();

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
        let other_path = other.path();
        index = index.merge(other);
        std::fs::remove_dir_all(other_path).unwrap();
    }

    index.inverted_index.merge_into_max_segments(1).unwrap();

    Ok(index)
}
