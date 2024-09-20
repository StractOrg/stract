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

mod downloaded_db;

use std::sync::Arc;
use std::time::Duration;

use crate::ampc::dht::ShardId;
use crate::config::GossipConfig;
use crate::distributed::cluster::Cluster;
use crate::distributed::sonic::replication::{
    AllShardsSelector, RandomReplicaSelector, RandomShardSelector, ShardedClient,
};
use crate::entrypoint::search_server;
use crate::Result;
use crate::{
    distributed::sonic::replication::ReusableShardedClient,
    entrypoint::{indexer::IndexableWebpage, live_index},
};
use downloaded_db::ShardedDownloadedDb;
use tokio::sync::Mutex;
use url::Url;

const SITE_URL_BATCH_SIZE: usize = 100;
const DEFAULT_CONSISTENCY_FRACTION: f64 = 0.5;

struct Client {
    live_index: Mutex<ReusableShardedClient<live_index::LiveIndexService>>,
    search: Mutex<ReusableShardedClient<search_server::SearchService>>,
}

impl Client {
    pub async fn new() -> Result<Self> {
        todo!()
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

struct LiveCrawlerConfig {
    gossip: GossipConfig,
}

pub struct Crawler {
    db: ShardedDownloadedDb,
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

        todo!()
    }
}
