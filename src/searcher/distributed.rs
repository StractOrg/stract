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
    exponential_backoff::ExponentialBackoff,
    inverted_index::{self, RetrievedWebpage},
};

use std::{net::SocketAddr, time::Instant};

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::task::JoinHandle;

use crate::{sonic, webpage::region::Region};

use super::{local, LocalSearcher, SearchResult};

type Result<T> = std::result::Result<T, Error>;

struct RemoteSearcher {
    addr: SocketAddr,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Failed to get search result")]
    NoResult,
}

impl RemoteSearcher {
    async fn search(&self, query: &SearchQuery) -> Result<local::InitialSearchResult> {
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

        Err(Error::NoResult)
    }

    async fn retrieve_websites(
        &self,
        pointers: &[inverted_index::WebsitePointer],
    ) -> Vec<RetrievedWebpage> {
        todo!();
    }
}

#[derive(Clone)]
struct ShardId(String);

pub struct Shard {
    id: ShardId,
    replicas: Vec<RemoteSearcher>,
}

impl Shard {
    async fn search(&self, query: &SearchQuery) -> Result<InitialSearchResult> {
        match self
            .replicas
            .iter()
            .map(|remote| remote.search(query))
            .collect::<FuturesUnordered<_>>()
            .next()
            .await
        {
            Some(result) => Ok(InitialSearchResult {
                local_result: result?,
                shard: self.id.clone(),
            }),
            None => Err(Error::NoResult),
        }
    }

    async fn retrieve_websites(
        &self,
        pointers: &[inverted_index::WebsitePointer],
    ) -> Vec<RetrievedWebpage> {
        todo!();
    }
}

struct InitialSearchResult {
    local_result: local::InitialSearchResult,
    shard: ShardId,
}

#[derive(Serialize, Deserialize, Clone)]
struct SearchQuery {
    query: String,
    selected_region: Option<Region>,
    goggle_program: Option<String>,
    skip_pages: Option<usize>,
}

#[derive(Serialize, Deserialize)]
enum Request {
    Search(SearchQuery),
}

pub struct DistributedSearcher {
    shards: Vec<Shard>,
    handle: JoinHandle<()>,
}

impl DistributedSearcher {
    pub async fn bind(addr: SocketAddr, local_searcher: LocalSearcher, shards: Vec<Shard>) -> Self {
        let handle = tokio::task::spawn(Self::start_server(addr, local_searcher));

        Self { handle, shards }
    }

    async fn start_server(addr: SocketAddr, local_searcher: LocalSearcher) {
        let server = sonic::Server::bind(addr).await.unwrap();

        loop {
            if let Ok(req) = server.accept::<Request>().await {
                match &req.body {
                    Request::Search(search) => {
                        match local_searcher.search_initial(
                            &search.query,
                            search.selected_region,
                            search.goggle_program.clone(),
                            search.skip_pages,
                        ) {
                            Ok(response) => {
                                req.respond(sonic::Response::Content(response)).await.ok();
                            }
                            Err(_) => {
                                req.respond::<SearchResult>(sonic::Response::Empty)
                                    .await
                                    .ok();
                            }
                        }
                    }
                }
            }
        }
    }

    pub async fn search(
        &self,
        query: &str,
        selected_region: Option<Region>,
        goggle_program: Option<String>,
        skip_pages: Option<usize>,
    ) -> Result<SearchResult> {
        let start = Instant::now();

        let query = SearchQuery {
            query: query.to_string(),
            selected_region,
            goggle_program,
            skip_pages,
        };

        // search shards
        let results = self
            .shards
            .iter()
            .map(|shard| shard.search(&query))
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .filter_map(|result| result.ok())
            .collect::<Vec<_>>();

        // check if any result has a bang hit

        // combine results

        // retrieve webpages

        // return result

        todo!();
    }
}
