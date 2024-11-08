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

use crate::{
    config::ApiThresholds,
    distributed::{
        cluster::Cluster,
        sonic::replication::{
            AllShardsSelector, RandomReplicaSelector, ReusableShardedClient, ShardedClient,
        },
    },
    entity_index::EntityMatch,
    entrypoint::entity_search_server,
    image_store::Image,
    ranking::pipeline::RecallRankingWebpage,
    searcher::{DistributedSearcher, SearchClient},
    Result,
};
use std::{cmp::Ordering, sync::Arc};

use optics::Optic;
use tokio::sync::Mutex;
use url::Url;

use crate::{
    search_prettifier::{create_stackoverflow_sidebar, DisplayedSidebar},
    searcher::{distributed, SearchQuery},
};

pub struct SidebarManager {
    distributed_searcher: DistributedSearcher,
    entity_searcher: Mutex<ReusableShardedClient<entity_search_server::SearchService>>,
    thresholds: ApiThresholds,
}

impl SidebarManager {
    pub async fn new(cluster: Arc<Cluster>, thresholds: ApiThresholds) -> Self {
        Self {
            distributed_searcher: DistributedSearcher::new(cluster.clone()).await,
            entity_searcher: Mutex::new(ReusableShardedClient::new(cluster.clone()).await),
            thresholds,
        }
    }

    async fn entity_conn(&self) -> Arc<ShardedClient<entity_search_server::SearchService, ()>> {
        self.entity_searcher.lock().await.conn().await
    }

    pub async fn get_entity_image(
        &self,
        image_id: &str,
        max_height: Option<u64>,
        max_width: Option<u64>,
    ) -> Result<Option<Image>> {
        let client = self.entity_conn().await;

        Ok(client
            .send(
                entity_search_server::GetEntityImage {
                    image_id: image_id.to_string(),
                    max_height,
                    max_width,
                },
                &AllShardsSelector,
                &RandomReplicaSelector,
            )
            .await
            .map_err(|_| anyhow::anyhow!("Failed to get entity image"))?
            .into_iter()
            .flatten()
            .next()
            .and_then(|(_, mut v)| v.pop())
            .and_then(|(_, v)| v))
    }

    async fn search_entity(&self, query: &str) -> Option<EntityMatch> {
        let client = self.entity_conn().await;

        client
            .send(
                entity_search_server::Search {
                    query: query.to_string(),
                },
                &AllShardsSelector,
                &RandomReplicaSelector,
            )
            .await
            .ok()?
            .into_iter()
            .flatten()
            .next()
            .and_then(|(_, mut v)| v.pop())
            .and_then(|(_, v)| v)
    }

    pub async fn stackoverflow(&self, query: &str) -> Result<Option<DisplayedSidebar>> {
        let query = SearchQuery {
            query: query.to_string(),
            num_results: 1,
            optic: Some(Optic::parse(include_str!("stackoverflow.optic")).unwrap()),
            ..Default::default()
        };

        let mut results: Vec<_> = self
            .distributed_searcher
            .search_initial(&query)
            .await
            .into_iter()
            .filter_map(|result| {
                result
                    .local_result
                    .websites
                    .first()
                    .cloned()
                    .map(|website| (result.shard, website))
            })
            .collect();

        results
            .sort_by(|(_, a), (_, b)| a.score().partial_cmp(&b.score()).unwrap_or(Ordering::Equal));

        if let Some((shard, website)) = results.pop() {
            let score = website.score();
            tracing::debug!(?score, ?self.thresholds.stackoverflow, "stackoverflow score");
            if website.score() > self.thresholds.stackoverflow {
                let website = RecallRankingWebpage::new(website, Default::default());
                let scored_websites = vec![distributed::ScoredWebpagePointer { website, shard }];
                let mut retrieved = self
                    .distributed_searcher
                    .retrieve_webpages(&scored_websites, &query.query)
                    .await;

                if let Some(res) = retrieved.pop() {
                    let res = res.into_retrieved_webpage();
                    return Ok(Some(create_stackoverflow_sidebar(
                        res.schema_org,
                        Url::parse(&res.url).unwrap(),
                    )?));
                }
            }
        }

        Ok(None)
    }

    pub async fn sidebar(&self, query: &str) -> Option<DisplayedSidebar> {
        let (entity, stackoverflow) =
            futures::join!(self.search_entity(query), self.stackoverflow(query));

        if let Some(entity) = entity {
            if entity.score as f64 > self.thresholds.entity_sidebar {
                return Some(DisplayedSidebar::Entity(entity.into()));
            }
        }

        stackoverflow.ok().flatten()
    }
}
