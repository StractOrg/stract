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
// along with this program.  If not, see <https://www.gnu.org/licenses/

use crate::Result;
use std::sync::Arc;

use chrono::Utc;
use url::Url;

use crate::{
    config::CrawlerConfig,
    crawler::{reqwest_client, JobExecutor, RetrieableUrl, WeightedUrl, WorkerJob},
    feed::{
        self,
        scheduler::{Domain, DomainFeeds, Split},
    },
};

use super::{downloaded_db::DownloadedDb, indexer::Indexer, Feeds, FEED_CHECK_INTERVAL};

pub enum CrawlResults {
    None,
    HasInserts,
}

pub struct Crawler {
    feeds: Vec<Feeds>,
    indexer: Arc<Indexer>,
    downloaded_db: DownloadedDb,
    config: Arc<CrawlerConfig>,
    client: reqwest::Client,
}

impl Crawler {
    pub fn new(
        split: Split,
        indexer: Arc<Indexer>,
        downloaded_db: DownloadedDb,
        config: Arc<CrawlerConfig>,
    ) -> Result<Self> {
        let client = reqwest_client(&config)?;

        Ok(Self {
            feeds: split.into(),
            indexer,
            downloaded_db,
            config,
            client,
        })
    }

    async fn process_urls(&self, urls: Vec<Url>) -> Result<bool> {
        if urls.is_empty() {
            return Ok(false);
        }

        let domain = urls.first().unwrap().into();
        let job = WorkerJob {
            domain,
            urls: urls
                .clone()
                .into_iter()
                .map(|url| RetrieableUrl::from(WeightedUrl { url, weight: 1.0 }))
                .collect(),
            wandering_urls: 0,
        };

        let executor = JobExecutor::new(
            job,
            self.client.clone(),
            self.config.clone(),
            self.indexer.clone(),
        );
        executor.run().await;

        for url in &urls {
            self.downloaded_db.insert(url)?;
        }

        Ok(true)
    }

    async fn process_feed(&self, domain_feeds: &DomainFeeds) -> Result<bool> {
        let mut urls = Vec::new();

        for feed in domain_feeds.feeds.iter() {
            let content = self
                .client
                .get(feed.url.as_str())
                .send()
                .await?
                .text()
                .await?;

            let feed = feed::parse(&content, feed.kind)?;

            for url in feed.links {
                if !self.downloaded_db.has_downloaded(&url)?
                    && Domain::from(&url) == domain_feeds.domain
                {
                    urls.push(url);
                }
            }
        }

        self.process_urls(urls).await
    }

    pub async fn check_feeds(&mut self) -> CrawlResults {
        let mut feeds = self.feeds.clone();

        let mut futures = Vec::new();
        for feeds in feeds.iter_mut() {
            if feeds.last_checked + FEED_CHECK_INTERVAL < Utc::now() {
                futures.push(self.process_feed(&feeds.feed));
                feeds.last_checked = Utc::now();
            }
        }

        let res = futures::future::join_all(futures).await;

        self.feeds = feeds;

        if res.iter().filter_map(|r| r.as_ref().ok()).any(|r| *r) {
            CrawlResults::HasInserts
        } else {
            CrawlResults::None
        }
    }
}
