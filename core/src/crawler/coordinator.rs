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

use crate::{call_counter::CallCounter, webpage::Url};

use super::{
    crawl_db::{CrawlDb, DomainStatus},
    Domain, Job, JobResponse, Result,
};
use std::{
    collections::HashSet,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex,
    },
    time::{Duration, Instant},
};

const DEFAULT_JOB_URLS: usize = 1000;

pub struct CrawlCoordinator {
    db: CrawlDb,
    num_crawled_urls: AtomicU64,
    num_urls_to_crawl: u64,
    call_counter: Mutex<CallCounter>,
}

impl CrawlCoordinator {
    pub fn new<P: AsRef<Path>>(
        crawldb_folder: P,
        num_urls_to_crawl: u64,
        seed_urls: Vec<String>,
    ) -> Result<Self> {
        let db = CrawlDb::open(crawldb_folder)?;

        let seed_urls = seed_urls.into_iter().map(Url::from).collect::<Vec<_>>();

        let domain_seeds = seed_urls
            .iter()
            .map(|url| Domain(url.domain().to_string()))
            .collect::<HashSet<_>>();

        db.transaction()?.insert_seed_urls(&seed_urls)?;
        db.transaction()?
            .update_max_inlinks_domains(domain_seeds.iter())?;

        Ok(Self {
            db,
            num_urls_to_crawl,
            num_crawled_urls: AtomicU64::new(0),
            call_counter: Mutex::new(CallCounter::new(Duration::from_secs(10))),
        })
    }

    fn log_crawls_per_second(&self, num_urls: usize) {
        if self.is_done() {
            return;
        }

        let mut call_counter = self.call_counter.lock().unwrap();

        call_counter.count_with_weight(num_urls);
        tracing::info!("avg crawls per second: {}", call_counter.avg_per_second());
    }

    pub fn add_response(&self, response: &JobResponse) -> Result<()> {
        self.log_crawls_per_second(response.url_responses.len());
        self.num_crawled_urls.fetch_add(1, Ordering::SeqCst);
        let tx = self.db.transaction()?;

        tx.insert_urls(&response.domain, &response.discovered_urls)?;
        tx.update_url_status(&response.url_responses)?;

        tx.set_domain_status(&response.domain, DomainStatus::Pending)?;

        let mut domains = HashSet::new();
        domains.insert(response.domain.clone());
        domains.extend(
            response
                .discovered_urls
                .iter()
                .map(|url| Domain(url.domain().to_string())),
        );

        tx.update_max_inlinks_domains(domains.iter())?;

        Ok(())
    }

    pub fn is_done(&self) -> bool {
        self.num_crawled_urls.load(Ordering::SeqCst) >= self.num_urls_to_crawl
    }

    pub fn sample_jobs(&self, num_jobs: usize) -> Result<Vec<Job>> {
        let start = Instant::now();
        let tx = self.db.transaction()?;

        let domains = tx.sample_domains(num_jobs)?;
        tracing::debug!("sampled domains: {:?}", domains);
        let jobs = tx.prepare_jobs(&domains, DEFAULT_JOB_URLS)?;
        tracing::debug!("sampled jobs: {:?}", jobs);
        tracing::info!("sampled {} jobs in {:?}", jobs.len(), start.elapsed());

        Ok(jobs)
    }
}
