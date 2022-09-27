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

use crate::{bangs::BangHit, entity_index::StoredEntity, inverted_index::RetrievedWebpage, Result};

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use tantivy::DocAddress;
use tokio::task::JoinHandle;

use crate::{sonic, webpage::region::Region};

use super::{LocalSearcher, SearchResult};

struct RemoteSearcher {
    addr: SocketAddr,
}

impl RemoteSearcher {
    async fn search(&self, query: &SearchQuery) {
        todo!()
    }
}

struct Replica {
    searchers: Vec<RemoteSearcher>,
}

impl Replica {
    async fn search(&self, query: &SearchQuery) -> InitialSearchResult {
        todo!()
    }

    async fn retrieve_websites(&self, pointers: &[WebsitePointer]) -> Vec<RetrievedWebpage> {
        todo!();
    }

    async fn retrieve_entity(&self, pointer: EntityPointer) -> StoredEntity {
        todo!();
    }
}

struct ShardId(String);

struct Shard {
    id: ShardId,
    replicas: Vec<Replica>,
}

impl Shard {
    async fn search(&self, query: &SearchQuery) -> InitialSearchResult {
        todo!()
    }

    async fn retrieve_websites(&self, pointers: &[WebsitePointer]) -> Vec<RetrievedWebpage> {
        todo!();
    }

    async fn retrieve_entity(&self, pointer: EntityPointer) -> StoredEntity {
        todo!();
    }
}

enum InitialSearchResult {
    Bang(BangHit),
    Websites(InitialWebsiteResult),
}

impl InitialSearchResult {
    fn combine(self, other: Self) -> Self {
        todo!();
    }
}

struct InitialWebsiteResult {
    pub spell_corrected_query: Option<String>,
    pub webpages: Vec<WebsitePointer>,
    pub entity: Option<EntityPointer>,
}

impl InitialWebsiteResult {
    fn combine(self, other: Self) -> Self {
        todo!();
    }
}

struct WebsitePointer {
    score: f64,
    shard: ShardId,
    doc_address: DocAddress,
}

struct EntityPointer {
    score: f64,
    shard: ShardId,
    doc_address: DocAddress,
}

#[derive(Serialize, Deserialize)]
struct SearchQuery {
    query: String,
    selected_region: Option<Region>,
    goggle_program: Option<String>,
    skip_pages: Option<usize>,
}

pub struct DistributedSearcher {
    shards: Vec<Shard>,
    handle: JoinHandle<()>,
}

impl DistributedSearcher {
    pub async fn bind(addr: SocketAddr, local_searcher: LocalSearcher) -> Self {
        let handle = tokio::task::spawn(Self::start_server(addr, local_searcher));

        Self {
            handle,
            shards: Vec::new(),
        }
    }

    async fn start_server(addr: SocketAddr, local_searcher: LocalSearcher) {
        let server = sonic::Server::bind(addr).await.unwrap();

        loop {
            if let Ok(req) = server.accept::<SearchQuery>().await {
                match local_searcher.search(
                    &req.body.query,
                    req.body.selected_region,
                    req.body.goggle_program.clone(),
                    req.body.skip_pages,
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

    pub async fn search(
        &self,
        query: &str,
        selected_region: Option<Region>,
        goggle_program: Option<String>,
        skip_pages: Option<usize>,
    ) -> Result<SearchResult> {
        let query = SearchQuery {
            query: query.to_string(),
            selected_region,
            goggle_program,
            skip_pages,
        };

        todo!();
    }
}
