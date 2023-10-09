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

use hashbrown::HashMap;

use super::{crawl_db::CrawlDb, Domain, DomainCrawled, Job, Result, UrlToInsert};
use std::{path::Path, sync::Mutex, time::Instant};

const DEFAULT_JOB_URLS: usize = 200;

pub struct CrawlCoordinator {
    db: Mutex<CrawlDb>,
}

impl CrawlCoordinator {
    pub fn new<P: AsRef<Path>>(crawldb_folder: P) -> Result<Self> {
        let db = CrawlDb::open(crawldb_folder)?;

        Ok(Self { db: Mutex::new(db) })
    }

    pub fn insert_urls(&self, urls: HashMap<Domain, Vec<UrlToInsert>>) -> Result<()> {
        let mut db = self.db.lock().unwrap();
        let start = Instant::now();

        db.insert_urls(urls)?;

        tracing::info!("inserted responses in {:?}", start.elapsed());

        Ok(())
    }

    pub fn mark_jobs_complete(&self, domains: &[DomainCrawled]) -> Result<()> {
        let mut db = self.db.lock().unwrap();
        let start = Instant::now();

        db.mark_jobs_complete(domains)?;

        tracing::info!("marked jobs complete in {:?}", start.elapsed());

        Ok(())
    }

    pub fn sample_jobs(&self, num_jobs: usize) -> Result<Vec<Job>> {
        let mut db = self.db.lock().unwrap();
        let start = Instant::now();

        let domains = db.sample_domains(num_jobs)?;
        tracing::debug!("sampled domains: {:?}", domains);
        let jobs = db.prepare_jobs(&domains, DEFAULT_JOB_URLS)?;
        tracing::debug!("sampled jobs: {:?}", jobs);
        tracing::info!("sampled {} jobs in {:?}", jobs.len(), start.elapsed());

        Ok(jobs)
    }
}
