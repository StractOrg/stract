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

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use itertools::Itertools;
use tokio::sync::Mutex;
use url::Url;

use crate::{
    config::{CheckIntervals, CrawlerConfig},
    crawler,
    entrypoint::{
        indexer::IndexableWebpage,
        site_stats::{self, FinalSiteStats},
    },
    live_index::crawler::checker::{Feeds, Frontpage, Sitemap},
    webpage::url_ext::UrlExt,
    Result,
};

use super::{
    checker::{Checker, CrawlableUrl},
    crawled_db::ShardedCrawledDb,
    Client,
};

pub const MAX_PENDING_BUDGET: u64 = 32;

struct InnerCrawlableSite {
    site: site_stats::Site,
    feeds: Feeds,
    sitemap: Sitemap,
    frontpage: Frontpage,
    last_drip: Instant,
    drip_rate: Duration,
    budget: u64,
}

impl InnerCrawlableSite {
    pub fn new(site: FinalSiteStats, client: &Client, drip_rate: Duration) -> Result<Self> {
        Ok(Self {
            site: site.site().clone(),
            feeds: Feeds::new(
                site.stats()
                    .feeds
                    .clone()
                    .into_iter()
                    .map(|feed| feed.into())
                    .collect(),
                client.reqwest().clone(),
            ),
            sitemap: Sitemap::new(site.site(), client.reqwest().clone())?,
            frontpage: Frontpage::new(site.site(), client.reqwest().clone())?,
            last_drip: Instant::now(),
            drip_rate,
            budget: 0,
        })
    }

    pub fn drip(&mut self) {
        let pages_to_drip =
            (self.last_drip.elapsed().as_secs_f32() / self.drip_rate.as_secs_f32()) as u64;

        if pages_to_drip > 0 {
            self.last_drip = Instant::now();
            self.budget += pages_to_drip;

            if self.budget > MAX_PENDING_BUDGET {
                self.budget = MAX_PENDING_BUDGET;
            }
        }
    }
    pub fn should_crawl(&self, interval: &CheckIntervals) -> bool {
        (self.frontpage.should_check(interval)
            || self.sitemap.should_check(interval)
            || self.feeds.should_check(interval))
            && self.budget > 0
    }
}

pub struct CrawlableSite {
    inner: Arc<Mutex<InnerCrawlableSite>>,
    currently_crawling: AtomicBool,
    site: site_stats::Site,
}

impl CrawlableSite {
    pub fn new(site: FinalSiteStats, client: &Client, drip_rate: Duration) -> Result<Self> {
        Ok(Self {
            site: site.site().clone(),
            inner: Arc::new(Mutex::new(InnerCrawlableSite::new(
                site, client, drip_rate,
            )?)),
            currently_crawling: AtomicBool::new(false),
        })
    }

    pub fn currently_crawling(&self) -> bool {
        self.currently_crawling.load(Ordering::Relaxed)
    }

    pub fn site(&self) -> &site_stats::Site {
        &self.site
    }

    pub async fn should_crawl(&self, interval: &CheckIntervals) -> bool {
        !self.currently_crawling() && self.inner.lock().await.should_crawl(interval)
    }

    pub async fn drip(&self) {
        if self.currently_crawling() {
            return;
        }

        self.inner.lock().await.drip();
    }
}

impl crawler::DatumStream for tokio::sync::Mutex<Vec<crawler::CrawlDatum>> {
    async fn write(&self, crawl_datum: crawler::CrawlDatum) -> Result<(), crawler::Error> {
        self.lock().await.push(crawl_datum);
        Ok(())
    }

    async fn finish(&self) -> Result<(), crawler::Error> {
        Ok(())
    }
}

pub struct CrawlableSiteGuard {
    site: Arc<CrawlableSite>,
    crawled_db: Arc<ShardedCrawledDb>,
    config: Arc<CrawlerConfig>,
}

impl CrawlableSiteGuard {
    pub async fn new(
        site: Arc<CrawlableSite>,
        crawled_db: Arc<ShardedCrawledDb>,
        config: Arc<CrawlerConfig>,
    ) -> Self {
        {
            let currently_crawling = site.currently_crawling.swap(true, Ordering::Relaxed);
            if currently_crawling {
                panic!("site is already being crawled");
            }
        }

        Self {
            site,
            crawled_db,
            config,
        }
    }

    pub fn url(&self) -> Result<Url> {
        self.site.site().url()
    }
}

impl Drop for CrawlableSiteGuard {
    fn drop(&mut self) {
        self.site.currently_crawling.store(false, Ordering::Relaxed);

        tracing::debug!(
            "Dropping crawlable site guard for site {}",
            self.site.site().as_str()
        );
    }
}

impl CrawlableSiteGuard {
    pub async fn crawl(self, client: &Client, interval: &CheckIntervals) -> Result<()> {
        let mut site = self.site.inner.lock().await;
        let mut urls = Vec::new();

        if site.feeds.should_check(interval) {
            urls.extend(site.feeds.get_urls().await.unwrap_or_default());
        }

        if site.sitemap.should_check(interval) {
            urls.extend(site.sitemap.get_urls().await.unwrap_or_default());
        }

        if site.frontpage.should_check(interval) {
            urls.extend(site.frontpage.get_urls().await.unwrap_or_default());
        }

        let url = site.site.url()?;
        let icann_domain = url.icann_domain();
        urls = urls
            .into_iter()
            .filter(|u| u.url.icann_domain() == icann_domain)
            .unique_by(|u| u.url.clone())
            .map(|mut u| {
                u.url.normalize_in_place();
                u
            })
            .collect();

        urls.retain(|url| !self.crawled_db.has_crawled(&url.url).unwrap_or(false));

        order_urls(&mut urls);

        let budget = site.budget.min(urls.len() as u64);
        site.budget = site.budget.saturating_sub(budget);

        urls.truncate(budget as usize);

        if urls.is_empty() {
            tracing::debug!("No new urls to crawl for site {}", site.site.as_str());
            return Ok(());
        }

        tracing::debug!(
            "Crawling {} urls for site {}",
            urls.len(),
            site.site.as_str()
        );

        for crawlable_url in &urls {
            self.crawled_db
                .insert(&crawlable_url.url.clone().normalize())?;
        }

        let crawl_data = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let executor = crawler::JobExecutor::new(
            crawler::WorkerJob {
                domain: crawler::Domain::from(site.site.url()?),
                urls: urls
                    .into_iter()
                    .take(budget as usize)
                    .map(|url| {
                        let weighted_url = crawler::WeightedUrl {
                            url: url.url,
                            weight: 1.0,
                        };

                        crawler::RetrieableUrl::from(weighted_url)
                    })
                    .collect(),
                wandering_urls: 0,
            },
            client.reqwest().clone(),
            self.config.clone(),
            Arc::clone(&crawl_data),
        );

        executor.run().await;

        let crawl_data = crawl_data.lock().await.clone();

        tracing::debug!(
            "Indexing {} urls for site {}",
            crawl_data.len(),
            site.site.as_str()
        );
        client
            .index(crawl_data.into_iter().map(IndexableWebpage::from).collect())
            .await?;

        tracing::debug!("Finished crawling site {}", site.site.as_str());

        Ok(())
    }
}

fn order_urls(urls: &mut [CrawlableUrl]) {
    urls.sort_by(|a, b| match (a.last_modified, b.last_modified) {
        (Some(a), Some(b)) => a.cmp(&b).reverse(),
        (Some(_), None) => std::cmp::Ordering::Greater,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (None, None) => std::cmp::Ordering::Equal,
    });
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use url::Url;

    use super::*;

    #[test]
    fn test_latest_urls_crawled_first() {
        let mut urls = vec![
            CrawlableUrl {
                url: Url::parse("https://example.com/page1").unwrap(),
                last_modified: Some(
                    DateTime::parse_from_rfc2822("Mon, 01 Jan 2024 00:00:00 GMT")
                        .unwrap()
                        .into(),
                ),
            },
            CrawlableUrl {
                url: Url::parse("https://example.com/page2").unwrap(),
                last_modified: Some(
                    DateTime::parse_from_rfc2822("Tue, 02 Jan 2024 00:00:01 GMT")
                        .unwrap()
                        .into(),
                ),
            },
            CrawlableUrl {
                url: Url::parse("https://example.com/page3").unwrap(),
                last_modified: None,
            },
        ];

        order_urls(&mut urls);

        assert_eq!(
            urls[0].url,
            Url::parse("https://example.com/page3").unwrap()
        );
        assert_eq!(
            urls[1].url,
            Url::parse("https://example.com/page2").unwrap()
        );
        assert_eq!(
            urls[2].url,
            Url::parse("https://example.com/page1").unwrap()
        );
    }
}
