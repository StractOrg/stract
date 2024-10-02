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

const SITE_URL_BATCH_SIZE: usize = 100;

use std::sync::Arc;

use url::Url;

use crate::{
    ampc::dht::ShardId,
    distributed::{
        sonic::{
            replication::{AllShardsSelector, RandomReplicaSelector, ShardedClient},
            service::Service,
        },
        streaming_response::StreamingResponse,
    },
    entrypoint::{live_index, search_server},
    Result,
};

pub struct SiteUrlStream<S: Service> {
    site: String,
    offset: usize,
    conn: Arc<ShardedClient<S, ShardId>>,
}

impl<S: Service> SiteUrlStream<S> {
    pub fn new(site: String, conn: Arc<ShardedClient<S, ShardId>>) -> Self {
        Self {
            site,
            offset: 0,
            conn,
        }
    }
}

impl StreamingResponse for SiteUrlStream<search_server::SearchService> {
    type Item = Url;

    async fn next_batch(&mut self) -> Result<Vec<Self::Item>> {
        let req = search_server::GetSiteUrls {
            site: self.site.clone(),
            offset: self.offset as u64,
            limit: SITE_URL_BATCH_SIZE as u64,
        };

        self.offset += SITE_URL_BATCH_SIZE;

        let res = self
            .conn
            .send(req, &AllShardsSelector, &RandomReplicaSelector)
            .await?;

        Ok(res
            .into_iter()
            .flat_map(|(_, v)| v.into_iter().flat_map(|(_, v)| v.urls))
            .collect())
    }
}

impl StreamingResponse for SiteUrlStream<live_index::LiveIndexService> {
    type Item = Url;

    async fn next_batch(&mut self) -> Result<Vec<Self::Item>> {
        let req = search_server::GetSiteUrls {
            site: self.site.clone(),
            offset: self.offset as u64,
            limit: SITE_URL_BATCH_SIZE as u64,
        };

        let res = self
            .conn
            .send(req, &AllShardsSelector, &RandomReplicaSelector)
            .await?;

        Ok(res
            .into_iter()
            .flat_map(|(_, v)| v.into_iter().flat_map(|(_, v)| v.urls))
            .collect())
    }
}
