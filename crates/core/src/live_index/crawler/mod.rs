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

mod budgets;
mod checker;
mod crawlable_site;
mod downloaded_db;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::ampc::dht::ShardId;
use crate::config::{CheckIntervals, CrawlerConfig, LiveCrawlerConfig};
use crate::distributed::cluster::Cluster;
use crate::distributed::sonic::replication::{
    AllShardsSelector, RandomReplicaSelector, RandomShardSelector, ShardedClient,
};
use crate::entrypoint::search_server;
use crate::entrypoint::site_stats::FinalSiteStats;
use crate::{crawler, Result};
use crate::{
    distributed::sonic::replication::ReusableShardedClient,
    entrypoint::{indexer::IndexableWebpage, live_index},
};
use crawlable_site::{CrawlableSite, CrawlableSiteGuard};
use downloaded_db::ShardedDownloadedDb;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use url::Url;

const SITE_URL_BATCH_SIZE: usize = 100;
const DEFAULT_CONSISTENCY_FRACTION: f64 = 0.5;
const BLOG_FRACTION_THRESHOLD: f64 = 0.5;
const NEWS_FRACTION_THRESHOLD: f64 = 0.5;
const MIN_CRAWL_DELAY: Duration = Duration::from_secs(60);
const MAX_CRAWL_DELAY: Duration = Duration::from_secs(300);
const TICK_INTERVAL: Duration = Duration::from_secs(5);

struct Client {
    live_index: Mutex<ReusableShardedClient<live_index::LiveIndexService>>,
    search: Mutex<ReusableShardedClient<search_server::SearchService>>,
    reqwest: reqwest::Client,
}

impl Client {
    pub async fn new(cluster: Arc<Cluster>, crawler_config: &CrawlerConfig) -> Result<Self> {
        let live_index = Mutex::new(ReusableShardedClient::new(cluster.clone()).await);
        let search = Mutex::new(ReusableShardedClient::new(cluster.clone()).await);

        Ok(Self {
            live_index,
            search,
            reqwest: crawler::reqwest_client(crawler_config)?,
        })
    }

    pub fn reqwest(&self) -> &reqwest::Client {
        &self.reqwest
    }

    async fn live_conn(&self) -> Arc<ShardedClient<live_index::LiveIndexService, ShardId>> {
        self.live_index.lock().await.conn().await
    }

    async fn search_conn(&self) -> Arc<ShardedClient<search_server::SearchService, ShardId>> {
        self.search.lock().await.conn().await
    }

    pub async fn index(&self, pages: Vec<IndexableWebpage>) -> Result<()> {
        let conn = self.live_conn().await;
        let req = live_index::IndexWebpages {
            pages,
            consistency_fraction: Some(DEFAULT_CONSISTENCY_FRACTION),
        };

        while let Err(e) = conn
            .send(req.clone(), &RandomShardSelector, &RandomReplicaSelector)
            .await
        {
            tracing::error!("Failed to index webpages: {e}");
            tokio::time::sleep(Duration::from_millis(1_000)).await;
        }

        Ok(())
    }

    pub async fn get_site_urls(&self, site: &str) -> Result<Vec<Url>> {
        let mut res = Vec::new();
        let conn = self.search_conn().await;
        let mut offset = 0;

        while let Ok(resp) = conn
            .send(
                search_server::GetSiteUrls {
                    site: site.to_string(),
                    offset,
                    limit: SITE_URL_BATCH_SIZE,
                },
                &AllShardsSelector,
                &RandomReplicaSelector,
            )
            .await
        {
            let urls: Vec<_> = resp
                .into_iter()
                .flat_map(|(_, v)| v.into_iter().flat_map(|(_, v)| v.urls))
                .collect();

            if urls.is_empty() {
                break;
            }

            res.extend(urls);
            offset += SITE_URL_BATCH_SIZE;
        }

        Ok(res)
    }
}

impl From<LiveCrawlerConfig> for CrawlerConfig {
    fn from(config: LiveCrawlerConfig) -> Self {
        Self {
            num_worker_threads: 1,
            user_agent: config.user_agent,
            robots_txt_cache_sec: crate::config::defaults::Crawler::robots_txt_cache_sec(),
            min_politeness_factor: crate::config::defaults::Crawler::min_politeness_factor(),
            start_politeness_factor: crate::config::defaults::Crawler::start_politeness_factor(),
            min_crawl_delay_ms: MIN_CRAWL_DELAY.as_millis() as u64,
            max_crawl_delay_ms: MAX_CRAWL_DELAY.as_millis() as u64,
            max_politeness_factor: crate::config::defaults::Crawler::max_politeness_factor(),
            max_url_slowdown_retry: crate::config::defaults::Crawler::max_url_slowdown_retry(),
            timeout_seconds: 60,
            s3: crate::config::S3Config {
                bucket: String::new(),
                folder: String::new(),
                access_key: String::new(),
                secret_key: String::new(),
                endpoint: String::new(),
            },
            router_hosts: vec![],
        }
    }
}

struct SiteStats {
    sites: Vec<FinalSiteStats>,
}

impl SiteStats {
    pub fn open(path: PathBuf) -> Result<Self> {
        let file = File::open(path)?;
        let sites = serde_json::from_reader(file)?;
        Ok(Self { sites })
    }

    pub fn blogs(&self) -> impl Iterator<Item = &FinalSiteStats> {
        self.sites.iter().filter(|site| {
            site.stats().blogposts as f64 / site.stats().pages as f64 > BLOG_FRACTION_THRESHOLD
        })
    }

    pub fn news(&self) -> impl Iterator<Item = &FinalSiteStats> {
        self.sites.iter().filter(|site| {
            site.stats().news_articles as f64 / site.stats().pages as f64 > NEWS_FRACTION_THRESHOLD
        })
    }

    pub fn all(&self) -> impl Iterator<Item = &FinalSiteStats> {
        self.sites.iter()
    }
}

pub struct Crawler {
    client: Arc<Client>,
    db: Arc<ShardedDownloadedDb>,
    sites: Vec<Arc<Mutex<CrawlableSite>>>,
    num_worker_threads: usize,
    check_intervals: CheckIntervals,
    crawler_config: Arc<CrawlerConfig>,
}

impl Crawler {
    pub async fn new(config: LiveCrawlerConfig) -> Result<Self> {
        let crawler_config = Arc::new(CrawlerConfig::from(config.clone()));

        let cluster = Arc::new(
            Cluster::join_as_spectator(
                config.gossip.addr,
                config.gossip.seed_nodes.unwrap_or_default(),
            )
            .await?,
        );

        let client = Arc::new(Client::new(cluster, &crawler_config).await?);
        let db = Arc::new(ShardedDownloadedDb::open(config.downloaded_db_path)?);

        let site_stats = SiteStats::open(config.site_stats_path)?;
        let sites: Vec<_> = site_stats
            .news()
            .chain(site_stats.blogs())
            .cloned()
            .collect();

        let budgets = budgets::SiteBudgets::new(
            &config.host_centrality_path,
            &site_stats,
            config.daily_budget.clone(),
        )?;

        let mut crawlable_sites = Vec::new();
        for site in sites {
            for url in client.get_site_urls(site.site().as_str()).await? {
                if !db.has_downloaded(&url)? {
                    db.insert(&url)?;
                }
            }

            if let Some(drip_rate) = budgets.drip_rate(site.site()) {
                crawlable_sites.push(Arc::new(Mutex::new(CrawlableSite::new(
                    site, &client, drip_rate,
                )?)));
            }
        }

        Ok(Self {
            client,
            db,
            sites: crawlable_sites,
            num_worker_threads: config.num_worker_threads,
            check_intervals: config.check_intervals,
            crawler_config,
        })
    }

    pub async fn run(mut self) -> Result<()> {
        let mut interval = tokio::time::interval(TICK_INTERVAL);
        let semaphore = Arc::new(Semaphore::new(self.num_worker_threads));

        loop {
            interval.tick().await;

            for site in &mut self.sites {
                let mut site_lock = site.lock().await;
                if site_lock.currently_crawling() {
                    continue;
                }

                site_lock.drip();

                if site_lock.should_crawl(&self.check_intervals) {
                    let client = self.client.clone();
                    let intervals = self.check_intervals.clone();
                    let guard = CrawlableSiteGuard::new(
                        site.clone(),
                        self.db.clone(),
                        self.crawler_config.clone(),
                    );
                    let semaphore = semaphore.clone();

                    tokio::spawn(async move {
                        let _permit = semaphore.acquire().await.unwrap();
                        let url = guard.url().await;

                        if let Err(e) = guard.crawl(&client, &intervals).await {
                            if let Ok(url) = url {
                                tracing::error!("Failed to crawl site {}: {:?}", url, e);
                            } else {
                                tracing::error!("Failed to crawl site: {:?}", e);
                            }
                        }
                    });
                }
            }
        }
    }
}
