// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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
    collector::{self, BucketCollector},
    exponential_backoff::ExponentialBackoff,
    inverted_index::{self, RetrievedWebpage},
    search_prettifier::DisplayedWebpage,
    searcher::{PrettifiedWebsitesResult, SearchResult, WebsitesResult, NUM_RESULTS_PER_PAGE},
};

use std::{net::SocketAddr, time::Instant};

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::sonic;

use super::{
    InitialPrettifiedSearchResult, InitialSearchResult, PrettifiedSearchResult, SearchQuery,
};

type Result<T> = std::result::Result<T, Error>;

struct RemoteSearcher {
    addr: SocketAddr,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Failed to get search result")]
    SearchFailed,

    #[error("Query cannot be empty")]
    EmptyQuery,
}

impl RemoteSearcher {
    async fn search(&self, query: &SearchQuery) -> Result<InitialSearchResult> {
        for timeout in ExponentialBackoff::from_millis(30).take(5) {
            if let Ok(connection) = sonic::Connection::create_with_timeout(self.addr, timeout).await
            {
                if let Ok(sonic::Response::Content(body)) =
                    connection.send(Request::Search(query.clone())).await
                {
                    return Ok(body);
                }
            }
        }

        Err(Error::SearchFailed)
    }

    async fn search_prettified(
        &self,
        query: &SearchQuery,
    ) -> Result<InitialPrettifiedSearchResult> {
        for timeout in ExponentialBackoff::from_millis(30).take(5) {
            if let Ok(connection) = sonic::Connection::create_with_timeout(self.addr, timeout).await
            {
                if let Ok(sonic::Response::Content(body)) = connection
                    .send(Request::SearchPrettified(query.clone()))
                    .await
                {
                    return Ok(body);
                }
            }
        }

        Err(Error::SearchFailed)
    }

    async fn retrieve_websites(
        &self,
        pointers: &[inverted_index::WebsitePointer],
        original_query: &str,
    ) -> Result<Vec<RetrievedWebpage>> {
        for timeout in ExponentialBackoff::from_millis(30).take(5) {
            if let Ok(connection) = sonic::Connection::create_with_timeout(self.addr, timeout).await
            {
                if let Ok(sonic::Response::Content(body)) = connection
                    .send(Request::RetrieveWebites {
                        websites: pointers.to_vec(),
                        query: original_query.to_string(),
                    })
                    .await
                {
                    return Ok(body);
                }
            }
        }

        Err(Error::SearchFailed)
    }

    async fn retrieve_websites_prettified(
        &self,
        pointers: &[inverted_index::WebsitePointer],
        original_query: &str,
    ) -> Result<Vec<DisplayedWebpage>> {
        for timeout in ExponentialBackoff::from_millis(30).take(5) {
            if let Ok(connection) = sonic::Connection::create_with_timeout(self.addr, timeout).await
            {
                if let Ok(sonic::Response::Content(body)) = connection
                    .send(Request::RetrievePrettifiedWebites {
                        websites: pointers.to_vec(),
                        query: original_query.to_string(),
                    })
                    .await
                {
                    return Ok(body);
                }
            }
        }

        Err(Error::SearchFailed)
    }
}

#[derive(Clone, PartialEq, Eq)]
struct ShardId(u32);

pub struct Shard {
    id: ShardId,
    replicas: Vec<RemoteSearcher>,
}

impl Shard {
    pub fn new(id: u32, replicas: Vec<String>) -> Self {
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

    async fn search(&self, query: &SearchQuery) -> Result<InitialSearchResultShard> {
        match self
            .replicas
            .iter()
            .map(|remote| remote.search(query))
            .collect::<FuturesUnordered<_>>()
            .next()
            .await
        {
            Some(result) => Ok(InitialSearchResultShard {
                local_result: result?,
                shard: self.id.clone(),
            }),
            None => Err(Error::SearchFailed),
        }
    }

    async fn search_prettified(
        &self,
        query: &SearchQuery,
    ) -> Result<InitialPrettifiedSearchResultShard> {
        match self
            .replicas
            .iter()
            .map(|remote| remote.search_prettified(query))
            .collect::<FuturesUnordered<_>>()
            .next()
            .await
        {
            Some(result) => Ok(InitialPrettifiedSearchResultShard {
                local_result: result?,
                shard: self.id.clone(),
            }),
            None => Err(Error::SearchFailed),
        }
    }

    async fn retrieve_websites(
        &self,
        pointers: &[inverted_index::WebsitePointer],
        original_query: &str,
    ) -> Result<Vec<RetrievedWebpage>> {
        match self
            .replicas
            .iter()
            .map(|remote| remote.retrieve_websites(pointers, original_query))
            .collect::<FuturesUnordered<_>>()
            .next()
            .await
        {
            Some(Ok(websites)) => Ok(websites),
            _ => Err(Error::SearchFailed),
        }
    }

    async fn retrieve_websites_prettified(
        &self,
        pointers: &[inverted_index::WebsitePointer],
        original_query: &str,
    ) -> Result<Vec<DisplayedWebpage>> {
        match self
            .replicas
            .iter()
            .map(|remote| remote.retrieve_websites_prettified(pointers, original_query))
            .collect::<FuturesUnordered<_>>()
            .next()
            .await
        {
            Some(Ok(websites)) => Ok(websites),
            _ => Err(Error::SearchFailed),
        }
    }
}

struct InitialSearchResultShard {
    local_result: InitialSearchResult,
    shard: ShardId,
}

struct InitialPrettifiedSearchResultShard {
    local_result: InitialPrettifiedSearchResult,
    shard: ShardId,
}

#[derive(Serialize, Deserialize)]
pub enum Request {
    Search(SearchQuery),
    SearchPrettified(SearchQuery),
    RetrieveWebites {
        websites: Vec<inverted_index::WebsitePointer>,
        query: String,
    },
    RetrievePrettifiedWebites {
        websites: Vec<inverted_index::WebsitePointer>,
        query: String,
    },
}

pub struct DistributedSearcher {
    shards: Vec<Shard>,
}

#[derive(Clone)]
struct ScoredWebsitePointer {
    local_pointer: inverted_index::WebsitePointer,
    shard: ShardId,
}

impl collector::Doc for ScoredWebsitePointer {
    fn score(&self) -> &f64 {
        &self.local_pointer.score
    }

    fn id(&self) -> &tantivy::DocId {
        &self.local_pointer.address.doc_id
    }

    fn hashes(&self) -> collector::Hashes {
        self.local_pointer.hashes
    }
}

impl DistributedSearcher {
    pub fn new(shards: Vec<Shard>) -> Self {
        Self { shards }
    }

    pub async fn search_api(&self, query: &SearchQuery) -> Result<SearchResult> {
        let start = Instant::now();

        if query.is_empty() {
            return Err(Error::EmptyQuery);
        }

        // search shards
        let initial_results = self
            .shards
            .iter()
            .map(|shard| shard.search(query))
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .filter_map(|result| result.ok())
            .collect::<Vec<_>>();

        // check if any result has a bang hit
        if let Some(result) = initial_results
            .iter()
            .find(|result| matches!(result.local_result, InitialSearchResult::Bang(_)))
        {
            if let InitialSearchResult::Bang(bang) = &result.local_result {
                return Ok(SearchResult::Bang(bang.clone()));
            }
        }

        let spell_corrected_query = initial_results.first().and_then(|result| {
            if let InitialSearchResult::Websites(result) = &result.local_result {
                result.spell_corrected_query.clone()
            } else {
                None
            }
        });

        let entity = initial_results.first().and_then(|result| {
            if let InitialSearchResult::Websites(result) = &result.local_result {
                result.entity.clone()
            } else {
                None
            }
        });

        let num_docs = initial_results
            .iter()
            .map(|result| {
                if let InitialSearchResult::Websites(result) = &result.local_result {
                    result.websites.num_websites
                } else {
                    0
                }
            })
            .sum();

        // combine results
        let top_n = NUM_RESULTS_PER_PAGE + query.skip_pages.unwrap_or(0);
        let mut collector = BucketCollector::new(top_n);

        for result in initial_results {
            if let InitialSearchResult::Websites(local_result) = result.local_result {
                for website in local_result.websites.top_websites {
                    let pointer = ScoredWebsitePointer {
                        local_pointer: website,
                        shard: result.shard.clone(),
                    };

                    collector.insert(pointer);
                }
            }
        }

        let top_websites = collector
            .into_sorted_vec(true)
            .into_iter()
            .skip(query.skip_pages.unwrap_or(0))
            .take(NUM_RESULTS_PER_PAGE)
            .collect::<Vec<_>>();

        // retrieve webpages
        let mut retrieved_webpages = Vec::new();

        for _ in 0..top_websites.len() {
            retrieved_webpages.push(None);
        }

        for shard in &self.shards {
            let (indexes, pointers): (Vec<_>, Vec<_>) = top_websites
                .iter()
                .enumerate()
                .filter(|(_, pointer)| pointer.shard == shard.id)
                .map(|(idx, pointer)| (idx, pointer.local_pointer.clone()))
                .unzip();

            if let Ok(websites) = shard.retrieve_websites(&pointers, &query.original).await {
                for (index, website) in indexes.into_iter().zip(websites.into_iter()) {
                    retrieved_webpages[index] = Some(website);
                }
            }
        }

        let retrieved_webpages: Vec<_> = retrieved_webpages.into_iter().flatten().collect();

        debug_assert_eq!(retrieved_webpages.len(), top_websites.len());

        if retrieved_webpages.is_empty() && !top_websites.is_empty() {
            return Err(Error::SearchFailed);
        }

        Ok(SearchResult::Websites(WebsitesResult {
            spell_corrected_query,
            webpages: inverted_index::SearchResult {
                num_docs,
                documents: retrieved_webpages,
            },
            entity,
            search_duration_ms: start.elapsed().as_millis(),
        }))
    }

    pub async fn search_prettified(&self, query: &SearchQuery) -> Result<PrettifiedSearchResult> {
        let start = Instant::now();

        if query.is_empty() {
            return Err(Error::EmptyQuery);
        }

        // search shards
        let initial_results = self
            .shards
            .iter()
            .map(|shard| shard.search_prettified(query))
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .filter_map(|result| result.ok())
            .collect::<Vec<_>>();

        // check if any result has a bang hit
        if let Some(result) = initial_results
            .iter()
            .find(|result| matches!(result.local_result, InitialPrettifiedSearchResult::Bang(_)))
        {
            if let InitialPrettifiedSearchResult::Bang(bang) = &result.local_result {
                return Ok(PrettifiedSearchResult::Bang(bang.clone()));
            }
        }

        let spell_corrected_query = initial_results.first().and_then(|result| {
            if let InitialPrettifiedSearchResult::Websites(result) = &result.local_result {
                result.spell_corrected_query.clone()
            } else {
                None
            }
        });

        let entity = initial_results.first().and_then(|result| {
            if let InitialPrettifiedSearchResult::Websites(result) = &result.local_result {
                result.entity.clone()
            } else {
                None
            }
        });

        let num_docs = initial_results
            .iter()
            .map(|result| {
                if let InitialPrettifiedSearchResult::Websites(result) = &result.local_result {
                    result.websites.num_websites
                } else {
                    0
                }
            })
            .sum();

        // combine results
        let top_n = NUM_RESULTS_PER_PAGE + query.skip_pages.unwrap_or(0);
        let mut collector = BucketCollector::new(top_n);

        for result in initial_results {
            if let InitialPrettifiedSearchResult::Websites(local_result) = result.local_result {
                for website in local_result.websites.top_websites {
                    let pointer = ScoredWebsitePointer {
                        local_pointer: website,
                        shard: result.shard.clone(),
                    };

                    collector.insert(pointer);
                }
            }
        }

        let top_websites = collector
            .into_sorted_vec(true)
            .into_iter()
            .skip(query.skip_pages.unwrap_or(0))
            .take(NUM_RESULTS_PER_PAGE)
            .collect::<Vec<_>>();

        // retrieve webpages
        let mut retrieved_webpages = Vec::new();

        for _ in 0..top_websites.len() {
            retrieved_webpages.push(None);
        }

        for shard in &self.shards {
            let (indexes, pointers): (Vec<_>, Vec<_>) = top_websites
                .iter()
                .enumerate()
                .filter(|(_, pointer)| pointer.shard == shard.id)
                .map(|(idx, pointer)| (idx, pointer.local_pointer.clone()))
                .unzip();

            if let Ok(websites) = shard
                .retrieve_websites_prettified(&pointers, &query.original)
                .await
            {
                for (index, website) in indexes.into_iter().zip(websites.into_iter()) {
                    retrieved_webpages[index] = Some(website);
                }
            }
        }

        let retrieved_webpages: Vec<_> = retrieved_webpages.into_iter().flatten().collect();

        debug_assert_eq!(retrieved_webpages.len(), top_websites.len());

        if retrieved_webpages.is_empty() && !top_websites.is_empty() {
            return Err(Error::SearchFailed);
        }

        Ok(PrettifiedSearchResult::Websites(PrettifiedWebsitesResult {
            spell_corrected_query,
            num_docs,
            webpages: retrieved_webpages,
            entity,
            search_duration_ms: start.elapsed().as_millis(),
        }))
    }
}
