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

use anyhow::Result;
use std::{
    path::Path,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime},
};

use chrono::{DateTime, Utc};
use url::Url;

use crate::{
    config::{CrawlerConfig, LiveIndexConfig},
    crawler::{reqwest_client, CrawlDatum, DatumStream, JobExecutor, RetrieableUrl, WorkerJob},
    entrypoint::indexer::IndexingWorker,
    feed::{
        self,
        scheduler::{Domain, DomainFeeds, Split},
    },
};

const TTL: Duration = Duration::from_secs(60 * 60 * 24 * 60); // 60 days
const PRUNE_INTERVAL: Duration = Duration::from_secs(60 * 60); // 1 hour
const FEED_CHECK_INTERVAL: Duration = Duration::from_secs(60 * 10); // 10 minutes
const AUTO_COMMIT_INTERVAL: Duration = Duration::from_secs(60 * 5); // 5 minutes
const EVENT_LOOP_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
struct Feeds {
    last_checked: DateTime<Utc>,
    feed: DomainFeeds,
}

impl From<Split> for Vec<Feeds> {
    fn from(split: Split) -> Self {
        split
            .feeds
            .into_iter()
            .map(|feed| Feeds {
                last_checked: Utc::now(),
                feed,
            })
            .collect()
    }
}

struct DownloadedDb {
    db: rocksdb::DB,
}

impl DownloadedDb {
    fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);

        // some recommended settings (https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning)
        options.set_level_compaction_dynamic_level_bytes(true);
        options.set_bytes_per_sync(1048576);
        let mut block_options = rocksdb::BlockBasedOptions::default();
        block_options.set_block_size(16 * 1024);
        block_options.set_format_version(5);
        block_options.set_cache_index_and_filter_blocks(true);
        block_options.set_pin_l0_filter_and_index_blocks_in_cache(true);

        block_options.set_ribbon_filter(10.0);

        options.set_block_based_table_factory(&block_options);
        options.optimize_for_point_lookup(512); // 512 mb

        let db = rocksdb::DB::open_with_ttl(&options, path, TTL)?;
        Ok(Self { db })
    }

    fn has_downloaded(&self, url: &Url) -> Result<bool> {
        let key = url.as_str().as_bytes();

        if self.db.key_may_exist(key) {
            self.db.get(key).map(|v| v.is_some()).map_err(Into::into)
        } else {
            Ok(false)
        }
    }

    fn insert(&self, url: &Url) -> Result<()> {
        let key = url.as_str().as_bytes();
        self.db.put(key, b"")?;
        Ok(())
    }
}

struct Indexer {
    search_index: Arc<RwLock<crate::index::Index>>,
    worker: IndexingWorker,
}

enum CrawlResults {
    None,
    HasInserts,
}

struct Crawler {
    feeds: Vec<Feeds>,
    indexer: Arc<Indexer>,
    downloaded_db: DownloadedDb,
    config: Arc<CrawlerConfig>,
    client: reqwest::Client,
}

impl Crawler {
    fn new(
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
            urls: urls.clone().into_iter().map(RetrieableUrl::from).collect(),
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

    async fn check_feeds(&mut self) -> CrawlResults {
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

struct Index {
    search_index: Arc<RwLock<crate::index::Index>>,
}

impl Index {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut search_index = crate::index::Index::open(path.as_ref().join("index"))?;
        search_index.set_auto_merge_policy();

        let search_index = Arc::new(RwLock::new(search_index));

        Ok(Self { search_index })
    }

    fn commit(&self) {
        self.search_index
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .commit()
            .ok();
    }

    fn prune(&self) {
        self.search_index
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .delete_all_before(SystemTime::now() - TTL)
            .ok();
    }

    fn clone_inner_index(&self) -> Arc<RwLock<crate::index::Index>> {
        self.search_index.clone()
    }
}

#[async_trait::async_trait]
impl DatumStream for Indexer {
    async fn write(&self, crawl_datum: CrawlDatum) -> Result<()> {
        let webpage = self.worker.prepare_webpage(
            &crawl_datum.body,
            crawl_datum.url.as_str(),
            crawl_datum.fetch_time_ms,
        )?;

        self.search_index
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .insert(webpage)?;

        Ok(())
    }

    async fn finish(&self) -> Result<()> {
        self.search_index
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .commit()?;

        Ok(())
    }
}

impl From<&LiveIndexConfig> for CrawlerConfig {
    fn from(live: &LiveIndexConfig) -> Self {
        Self {
            num_worker_threads: 1, // no impact
            user_agent: live.user_agent.clone(),
            robots_txt_cache_sec: live.robots_txt_cache_sec,
            politeness_factor: live.politeness_factor,
            min_crawl_delay_ms: live.min_crawl_delay_ms,
            max_crawl_delay_ms: live.max_crawl_delay_ms,
            max_politeness_factor: live.max_politeness_factor,
            max_url_slowdown_retry: live.max_url_slowdown_retry,
            max_redirects: live.max_redirects,
            dry_run: false,
            timeout_seconds: live.timeout_seconds,
            // no impact
            s3: crate::config::S3Config {
                bucket: String::new(),
                folder: String::new(),
                access_key: String::new(),
                secret_key: String::new(),
                endpoint: String::new(),
            },
            router_hosts: Vec::new(),
        }
    }
}

pub struct IndexManager {
    index: Arc<Index>,
    crawler: Crawler,
}

impl IndexManager {
    pub fn new(config: LiveIndexConfig) -> Result<Self> {
        let index = Index::new(&config.index_path)?;
        let indexer = Arc::new(Indexer {
            search_index: index.clone_inner_index(),
            worker: IndexingWorker::new(
                config.host_centrality_store_path.clone(),
                config.page_centrality_store_path.clone(),
                config.page_webgraph_path.clone(),
                None,
                config.safety_classifier_path.clone(),
            ),
        });

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
}
