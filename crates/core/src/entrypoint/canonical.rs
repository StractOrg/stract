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
// along with this program.  If not, see <https://www.gnu.org/licenses/>

use std::path::Path;

use tracing::info;
use url::Url;

use crate::webpage::Html;
use crate::Result;

use crate::{
    canon_index::CanonicalIndex,
    config::{CanonicalIndexConfig, WarcSource},
    warc::WarcFile,
};

#[derive(Debug, bincode::Encode, bincode::Decode, Clone)]
pub struct Job {
    pub warc_source: WarcSource,
    pub warc_path: String,
}

pub struct Worker {
    pub index: CanonicalIndex,
}
impl Worker {
    pub fn process_job(&mut self, job: &Job) -> Result<()> {
        let name = job.warc_path.split('/').last().unwrap();
        info!("processing {}", name);

        let warc_file = WarcFile::download(&job.warc_source, &job.warc_path)?;

        for record in warc_file.records().flatten() {
            let webpage = match Html::parse_without_text(&record.response.body, &record.request.url)
            {
                Ok(webpage) => webpage,
                Err(err) => {
                    tracing::error!("error parsing webpage: {}", err);
                    continue;
                }
            };

            if let Some(canonical_url) = webpage.canonical_url() {
                let url = Url::parse(&record.request.url)?;
                self.index.insert(url, canonical_url)?;
            }
        }

        self.index.commit()?;

        info!("{} done", name);

        Ok(())
    }
}

pub fn create(config: CanonicalIndexConfig) -> Result<()> {
    let jobs: Vec<_> = config
        .warc_source
        .paths()?
        .into_iter()
        .skip(config.skip_warc_files.unwrap_or(0))
        .take(config.limit_warc_files.unwrap_or(usize::MAX))
        .map(|warc_path| Job {
            warc_source: config.warc_source.clone(),
            warc_path,
        })
        .collect();

    let num_workers = num_cpus::get();
    let mut handles = Vec::new();

    for i in 0..num_workers {
        let path = Path::new(&config.output_path).join(format!("worker_{}", i));
        let index = CanonicalIndex::open(path)?;

        let mut worker = Worker { index };
        let jobs = jobs.clone();

        let handle = std::thread::spawn(move || {
            for job in jobs.into_iter().skip(i).step_by(num_workers) {
                worker.process_job(&job).unwrap();
            }

            worker.index.optimize_read().unwrap();
            worker.index
        });

        handles.push(handle);
    }

    let mut indexes = Vec::new();
    for handler in handles {
        let index = handler.join().unwrap();
        indexes.push(index);
    }

    if !indexes.is_empty() {
        let mut first = indexes.remove(0);
        for index in indexes {
            first.merge(index)?;
        }
        first.optimize_read()?;
    }

    Ok(())
}
