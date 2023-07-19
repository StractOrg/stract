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
    distributed::{cluster::Cluster, member::Service, retry_strategy::ExponentialBackoff},
    inverted_index::{self, RetrievedWebpage},
    ranking::pipeline::{AsRankingWebsite, RankingWebsite},
    webpage::Url,
    Result,
};

use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::distributed::sonic;

use super::{InitialWebsiteResult, SearchQuery};

const NUM_REPLICA_RETRIES: usize = 3;

struct RemoteSearcher {
    addr: SocketAddr,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Failed to get search result")]
    SearchFailed,

    #[error("Query cannot be empty")]
    EmptyQuery,

    #[error("Webpage not found")]
    WebpageNotFound,
}

impl RemoteSearcher {
    async fn search(&self, query: &SearchQuery) -> Result<InitialWebsiteResult> {
        let mut conn = self.conn();

        if let Ok(sonic::Response::Content(body)) = conn
            .send_with_timeout(&Request::Search(query.clone()), Duration::from_secs(1))
            .await
        {
            return Ok(body);
        }

        Err(Error::SearchFailed.into())
    }

    async fn retrieve_websites(
        &self,
        pointers: &[inverted_index::WebsitePointer],
        original_query: &str,
    ) -> Result<Vec<RetrievedWebpage>> {
        let mut conn = self.conn();

        if let Ok(sonic::Response::Content(body)) = conn
            .send_with_timeout(
                &Request::RetrieveWebsites {
                    websites: pointers.to_vec(),
                    query: original_query.to_string(),
                },
                Duration::from_secs(1),
            )
            .await
        {
            return Ok(body);
        }
        Err(Error::SearchFailed.into())
    }

    async fn get_webpage(&self, url: &str) -> Result<Option<RetrievedWebpage>> {
        let mut conn = self.conn();

        if let Ok(sonic::Response::Content(body)) = conn
            .send_with_timeout(
                &Request::GetWebpage {
                    url: url.to_string(),
                },
                Duration::from_secs(1),
            )
            .await
        {
            return Ok(body);
        }

        Err(Error::WebpageNotFound.into())
    }

    async fn get_homepage_descriptions(&self, urls: &[String]) -> HashMap<Url, String> {
        let mut conn = self.conn();

        if let Ok(sonic::Response::Content(body)) = conn
            .send_with_timeout(
                &Request::GetHomepageDescriptions {
                    urls: urls.to_vec(),
                },
                Duration::from_secs(1),
            )
            .await
        {
            return body;
        }

        HashMap::new()
    }

    fn conn(&self) -> sonic::ResilientConnection<impl Iterator<Item = Duration>> {
        let retry = ExponentialBackoff::from_millis(30)
            .with_limit(Duration::from_millis(200))
            .take(5);

        sonic::ResilientConnection::create(self.addr, retry)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Debug)]
pub struct ShardId(u64);

pub struct Shard {
    id: ShardId,
    replicas: Vec<RemoteSearcher>,
}

impl Shard {
    pub fn new(id: u64, replicas: Vec<String>) -> Self {
        let mut parsed_replicas = Vec::new();

        for replica in replicas {
            parsed_replicas.push(RemoteSearcher {
                addr: replica.parse().unwrap(),
            });
        }

        Self {
            id: ShardId(id),
            replicas: parsed_replicas,
        }
    }

    fn replica(&self) -> &RemoteSearcher {
        self.replicas.choose(&mut rand::thread_rng()).unwrap()
    }

    async fn search(&self, query: &SearchQuery) -> Result<InitialSearchResultShard> {
        for _ in 0..NUM_REPLICA_RETRIES {
            if let Ok(result) = self.replica().search(query).await {
                return Ok(InitialSearchResultShard {
                    local_result: result,
                    shard: self.id.clone(),
                });
            }
        }

        Err(Error::SearchFailed.into())
    }

    async fn retrieve_websites(
        &self,
        pointers: &[inverted_index::WebsitePointer],
        original_query: &str,
    ) -> Result<Vec<RetrievedWebpage>> {
        for _ in 0..NUM_REPLICA_RETRIES {
            if let Ok(res) = self
                .replica()
                .retrieve_websites(pointers, original_query)
                .await
            {
                return Ok(res);
            }
        }

        Err(Error::SearchFailed.into())
    }

    async fn get_webpage(&self, url: &str) -> Result<Option<RetrievedWebpage>> {
        for _ in 0..NUM_REPLICA_RETRIES {
            if let Ok(res) = self.replica().get_webpage(url).await {
                return Ok(res);
            }
        }

        Err(Error::SearchFailed.into())
    }

    async fn get_homepage_descriptions(&self, urls: &[String]) -> HashMap<Url, String> {
        self.replica().get_homepage_descriptions(urls).await
    }
}

#[derive(Debug)]
pub struct InitialSearchResultShard {
    pub local_result: InitialWebsiteResult,
    pub shard: ShardId,
}

#[derive(Serialize, Deserialize)]
pub enum Request {
    Search(SearchQuery),
    RetrieveWebsites {
        websites: Vec<inverted_index::WebsitePointer>,
        query: String,
    },
    GetWebpage {
        url: String,
    },
    GetHomepageDescriptions {
        urls: Vec<String>,
    },
}

#[derive(Clone, Debug)]
pub struct ScoredWebsitePointer {
    pub website: RankingWebsite,
    pub shard: ShardId,
}

impl AsRankingWebsite for ScoredWebsitePointer {
    fn as_ranking(&self) -> &RankingWebsite {
        &self.website
    }

    fn as_mut_ranking(&mut self) -> &mut RankingWebsite {
        &mut self.website
    }
}

pub struct DistributedSearcher {
    cluster: Arc<Cluster>,
}

impl DistributedSearcher {
    pub fn new(cluster: Arc<Cluster>) -> Self {
        Self { cluster }
    }

    async fn shards(&self) -> Vec<Shard> {
        let mut shards = HashMap::new();
        for member in self.cluster.members().await {
            if let Service::Searcher { host, shard } = member.service {
                shards.entry(shard).or_insert_with(Vec::new).push(host);
            }
        }

        shards
            .into_iter()
            .map(|(shard, replicas)| Shard {
                id: shard,
                replicas: replicas
                    .into_iter()
                    .map(|addr| RemoteSearcher { addr })
                    .collect(),
            })
            .collect()
    }

    pub async fn retrieve_webpages(
        &self,
        top_websites: &[ScoredWebsitePointer],
        query: &str,
    ) -> Vec<RetrievedWebpage> {
        let mut retrieved_webpages = Vec::new();

        for _ in 0..top_websites.len() {
            retrieved_webpages.push(None);
        }

        for shard in self.shards().await.iter() {
            let (indexes, pointers): (Vec<_>, Vec<_>) = top_websites
                .iter()
                .enumerate()
                .filter(|(_, pointer)| pointer.shard == shard.id)
                .map(|(idx, pointer)| (idx, pointer.website.pointer.clone()))
                .unzip();

            if let Ok(websites) = shard.retrieve_websites(&pointers, query).await {
                for (index, website) in indexes.into_iter().zip(websites) {
                    retrieved_webpages[index] = Some(website);
                }
            }
        }

        let retrieved_webpages: Vec<_> = retrieved_webpages.into_iter().flatten().collect();

        debug_assert_eq!(retrieved_webpages.len(), top_websites.len());

        retrieved_webpages
    }

    pub async fn search_initial(&self, query: &SearchQuery) -> Vec<InitialSearchResultShard> {
        self.shards()
            .await
            .iter()
            .map(|shard| shard.search(query))
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .filter_map(|result| result.ok())
            .collect::<Vec<_>>()
    }

    pub async fn get_webpage(&self, url: &str) -> Result<RetrievedWebpage> {
        self.shards()
            .await
            .iter()
            .map(|shard| shard.get_webpage(url))
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .filter_map(|result| result.ok())
            .flatten()
            .collect::<Vec<_>>()
            .pop()
            .ok_or(Error::WebpageNotFound.into())
    }

    pub async fn get_homepage_descriptions(&self, urls: &[String]) -> HashMap<Url, String> {
        self.shards()
            .await
            .iter()
            .map(|shard| shard.get_homepage_descriptions(urls))
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .flatten()
            .collect()
    }
}
