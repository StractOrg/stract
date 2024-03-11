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
    distributed::{
        cluster::Cluster,
        member::Service,
        sonic::replication::{
            AllShardsSelector, RandomReplicaSelector, RemoteClient, ReplicatedClient, Shard,
            ShardIdentifier, ShardedClient, SpecificShardSelector,
        },
    },
    entity_index::EntityMatch,
    entrypoint::{
        entity_search_server,
        search_server::{self, SearchService},
    },
    image_store::Image,
    inverted_index::{RetrievedWebpage, WebpagePointer},
    ranking::pipeline::{PrecisionRankingWebpage, RecallRankingWebpage},
    Result,
};

use std::{collections::HashMap, sync::Arc};

use fnv::FnvHashMap;
use futures::future::join_all;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::future::Future;
use thiserror::Error;
use url::Url;

use super::{InitialWebsiteResult, SearchQuery};

#[derive(Error, Debug)]
pub enum Error {
    #[error("Failed to get search result")]
    SearchFailed,

    #[error("Query cannot be empty")]
    EmptyQuery,

    #[error("Webpage not found")]
    WebpageNotFound,
}

#[derive(Clone, Debug)]
pub struct ScoredWebpagePointer {
    pub website: RecallRankingWebpage,
    pub shard: ShardId,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Copy, Debug)]
pub struct ShardId(u64);

impl ShardId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

impl ShardIdentifier for ShardId {}

#[derive(Debug)]
pub struct InitialSearchResultShard {
    pub local_result: InitialWebsiteResult,
    pub shard: ShardId,
}

pub struct DistributedSearcher {
    cluster: Arc<Cluster>,
}

impl DistributedSearcher {
    pub fn new(cluster: Arc<Cluster>) -> Self {
        Self { cluster }
    }

    async fn client(&self) -> ShardedClient<SearchService, ShardId> {
        let mut shards = HashMap::new();
        for member in self.cluster.members().await {
            if let Service::Searcher { host, shard } = member.service {
                shards.entry(shard).or_insert_with(Vec::new).push(host);
            }
        }

        let mut shard_clients = Vec::new();

        for (id, replicas) in shards {
            let replicated =
                ReplicatedClient::new(replicas.into_iter().map(RemoteClient::new).collect());
            let shard = Shard::new(id, replicated);
            shard_clients.push(shard);
        }

        ShardedClient::new(shard_clients)
    }

    async fn entity_client(&self) -> ReplicatedClient<entity_search_server::SearchService> {
        let mut replicas = Vec::new();
        for member in self.cluster.members().await {
            if let Service::EntitySearcher { host } = member.service {
                replicas.push(RemoteClient::new(host));
            }
        }

        ReplicatedClient::new(replicas)
    }

    async fn retrieve_webpages_from_shard(
        &self,
        shard: ShardId,
        client: &ShardedClient<SearchService, ShardId>,
        query: &str,
        pointers: Vec<(usize, WebpagePointer)>,
    ) -> Vec<(usize, RetrievedWebpage)> {
        let (idxs, pointers): (Vec<usize>, Vec<WebpagePointer>) = pointers.into_iter().unzip();

        match client
            .send(
                &search_server::RetrieveWebsites {
                    websites: pointers,
                    query: query.to_string(),
                },
                &SpecificShardSelector(shard),
                &RandomReplicaSelector,
            )
            .await
        {
            Ok(v) => v
                .into_iter()
                .flat_map(|(_, v)| v)
                .flatten()
                .flatten()
                .zip_eq(idxs)
                .map(|(v, i)| (i, v))
                .collect(),
            _ => vec![],
        }
    }
}

impl SearchClient for DistributedSearcher {
    async fn search_initial(&self, query: &SearchQuery) -> Vec<InitialSearchResultShard> {
        let client = self.client().await;
        let mut results = Vec::new();

        if let Ok(res) = client
            .send(
                &search_server::Search {
                    query: query.clone(),
                },
                &AllShardsSelector,
                &RandomReplicaSelector,
            )
            .await
        {
            for (shard_id, mut res) in res {
                if let Some(Some(res)) = res.pop() {
                    results.push(InitialSearchResultShard {
                        local_result: res,
                        shard: shard_id,
                    });
                }
            }
        }

        results
    }

    async fn retrieve_webpages(
        &self,
        top_websites: &[(usize, ScoredWebpagePointer)],
        query: &str,
    ) -> Vec<(usize, PrecisionRankingWebpage)> {
        let mut rankings = FnvHashMap::default();
        let mut pointers: HashMap<_, Vec<_>> = HashMap::new();

        for (i, pointer) in top_websites {
            pointers
                .entry(pointer.shard)
                .or_default()
                .push((*i, pointer.website.pointer.clone()));

            rankings.insert(*i, pointer.website.clone());
        }

        let client = self.client().await;
        let mut futures = Vec::new();
        for (shard, pointers) in pointers {
            futures.push(self.retrieve_webpages_from_shard(shard, &client, query, pointers));
        }

        let mut retrieved_webpages = Vec::new();
        for pages in join_all(futures).await {
            for (i, page) in pages {
                retrieved_webpages
                    .push((i, PrecisionRankingWebpage::new(page, rankings[&i].clone())));
            }
        }

        debug_assert_eq!(retrieved_webpages.len(), top_websites.len());

        retrieved_webpages.sort_by(|(a, _), (b, _)| a.cmp(b));

        retrieved_webpages
    }

    async fn get_webpage(&self, url: &str) -> Result<Option<RetrievedWebpage>> {
        let client = self.client().await;

        let res = client
            .send(
                &search_server::GetWebpage {
                    url: url.to_string(),
                },
                &AllShardsSelector,
                &RandomReplicaSelector,
            )
            .await
            .map_err(|_| Error::SearchFailed)?;

        if let Some(res) = res.into_iter().flat_map(|(_, v)| v).flatten().next() {
            Ok(Some(res))
        } else {
            Err(Error::WebpageNotFound.into())
        }
    }

    async fn get_homepage_descriptions(&self, urls: &[Url]) -> HashMap<Url, String> {
        let client = self.client().await;

        let res = client
            .send(
                &search_server::GetHomepageDescriptions {
                    urls: urls.to_vec(),
                },
                &AllShardsSelector,
                &RandomReplicaSelector,
            )
            .await;

        match res {
            Ok(v) => v.into_iter().flat_map(|(_, v)| v).flatten().collect(),
            _ => HashMap::new(),
        }
    }

    async fn get_entity_image(
        &self,
        image_id: &str,
        max_height: Option<u64>,
        max_width: Option<u64>,
    ) -> Result<Option<Image>> {
        let client = self.entity_client().await;

        Ok(client
            .send(
                &entity_search_server::GetEntityImage {
                    image_id: image_id.to_string(),
                    max_height,
                    max_width,
                },
                &RandomReplicaSelector,
            )
            .await
            .map_err(|_| Error::SearchFailed)?
            .pop()
            .flatten())
    }

    async fn search_entity(&self, query: &str) -> Option<EntityMatch> {
        let client = self.entity_client().await;

        client
            .send(
                &entity_search_server::Search {
                    query: query.to_string(),
                },
                &RandomReplicaSelector,
            )
            .await
            .ok()?
            .pop()
            .flatten()
    }
}

pub trait SearchClient {
    fn search_initial(
        &self,
        query: &SearchQuery,
    ) -> impl Future<Output = Vec<InitialSearchResultShard>> + Send;

    fn retrieve_webpages(
        &self,
        top_websites: &[(usize, ScoredWebpagePointer)],
        query: &str,
    ) -> impl Future<Output = Vec<(usize, PrecisionRankingWebpage)>> + Send;

    fn search_entity(&self, query: &str) -> impl Future<Output = Option<EntityMatch>> + Send;

    fn get_webpage(
        &self,
        url: &str,
    ) -> impl Future<Output = Result<Option<RetrievedWebpage>>> + Send;

    fn get_homepage_descriptions(
        &self,
        urls: &[Url],
    ) -> impl Future<Output = HashMap<Url, String>> + Send;

    fn get_entity_image(
        &self,
        image_id: &str,
        max_height: Option<u64>,
        max_width: Option<u64>,
    ) -> impl Future<Output = Result<Option<Image>>> + Send;
}
