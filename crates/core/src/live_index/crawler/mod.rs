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

mod budgets;
mod checker;
mod crawlable_site;
mod downloaded_db;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::ampc::dht::ShardId;
use crate::config::GossipConfig;
use crate::distributed::cluster::Cluster;
use crate::distributed::sonic::replication::{
    AllShardsSelector, RandomReplicaSelector, RandomShardSelector, ShardedClient,
};
use crate::entrypoint::search_server;
use crate::entrypoint::site_stats::FinalSiteStats;
use crate::{
    distributed::sonic::replication::ReusableShardedClient,
    entrypoint::{indexer::IndexableWebpage, live_index},
};
use crate::{webgraph, Result};
use budgets::DailyBudget;
use crawlable_site::CrawlableSite;
use downloaded_db::ShardedDownloadedDb;
use tokio::sync::Mutex;
use url::Url;

const SITE_URL_BATCH_SIZE: usize = 100;
const DEFAULT_CONSISTENCY_FRACTION: f64 = 0.5;
const BLOG_FRACTION_THRESHOLD: f64 = 0.5;
const NEWS_FRACTION_THRESHOLD: f64 = 0.5;

struct Client {
    live_index: Mutex<ReusableShardedClient<live_index::LiveIndexService>>,
    search: Mutex<ReusableShardedClient<search_server::SearchService>>,
}

impl Client {
    pub async fn new(cluster: Arc<Cluster>) -> Result<Self> {
        let live_index = Mutex::new(ReusableShardedClient::new(cluster.clone()).await);
        let search = Mutex::new(ReusableShardedClient::new(cluster.clone()).await);

        Ok(Self { live_index, search })
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

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct LiveCrawlerConfig {
    pub downloaded_db_path: PathBuf,
    pub gossip: GossipConfig,
    pub site_stats_path: PathBuf,
    pub host_centrality_path: PathBuf,
    pub daily_budget: DailyBudget,
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
    client: Client,
    db: ShardedDownloadedDb,
    sites: Vec<CrawlableSite>,
}

impl Crawler {
    pub async fn new(config: LiveCrawlerConfig) -> Result<Self> {
        let cluster = Arc::new(
            Cluster::join_as_spectator(
                config.gossip.addr,
                config.gossip.seed_nodes.unwrap_or_default(),
            )
            .await?,
        );

        let client = Client::new(cluster).await?;
        let db = ShardedDownloadedDb::open(config.downloaded_db_path)?;

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
            for url in client.get_site_urls(&site.site().as_str()).await? {
                if !db.has_downloaded(&url)? {
                    db.insert(&url)?;
                }
            }

            if let Some(drip_rate) = budgets.drip_rate(&site.site()) {
                crawlable_sites.push(CrawlableSite::new(site, drip_rate)?);
            }
        }

        Ok(Self {
            client,
            db,
            sites: crawlable_sites,
        })
    }

    pub async fn run(self) -> Result<()> {
        todo!()
    }
}
