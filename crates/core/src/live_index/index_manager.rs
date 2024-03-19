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
// along with this program.  If not, see <https://www.gnu.org/licenses/>

use std::sync::Arc;

use chrono::Utc;

use crate::{
    config::{CrawlerConfig, LiveIndexConfig},
    feed::scheduler::Split,
};

use super::{
    crawler::{CrawlResults, Crawler},
    downloaded_db::DownloadedDb,
    indexer::Indexer,
    Index, AUTO_COMMIT_INTERVAL, EVENT_LOOP_INTERVAL, PRUNE_INTERVAL,
};
use crate::Result;

pub struct IndexManager {
    index: Arc<Index>,
    crawler: Crawler,
}

impl IndexManager {
    pub fn new(config: LiveIndexConfig) -> Result<Self> {
        let index = Index::new(&config.index_path)?;
        let indexer = Arc::new(Indexer::new(index.clone_inner_index(), config.clone()));

        let crawler_config = CrawlerConfig::from(&config);

        let crawler = Crawler::new(
            Split::open(&config.split_path)?,
            indexer,
            DownloadedDb::open(&config.downloaded_db_path)?,
            Arc::new(crawler_config),
        )?;

        Ok(Self {
            index: Arc::new(index),
            crawler,
        })
    }

    pub async fn run(mut self) {
        let mut has_inserts = false;
        let mut last_commit = Utc::now();
        let mut last_prune = Utc::now();

        loop {
            if let CrawlResults::HasInserts = self.crawler.check_feeds().await {
                has_inserts = true;
            }

            if last_prune + PRUNE_INTERVAL < Utc::now() {
                self.index.prune();

                last_prune = Utc::now();
            }

            if last_commit + AUTO_COMMIT_INTERVAL < Utc::now() && has_inserts {
                self.index.commit();

                last_commit = Utc::now();
                has_inserts = false;
            }

            tokio::time::sleep(EVENT_LOOP_INTERVAL).await;
        }
    }

    pub fn index(&self) -> Arc<Index> {
        self.index.clone()
    }
}
