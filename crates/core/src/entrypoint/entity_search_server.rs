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

use serde::{Deserialize, Serialize};

use crate::{
    config,
    distributed::sonic::service::sonic_service,
    distributed::{
        cluster::Cluster,
        member::{Member, Service},
        sonic,
    },
    entity_index::EntityIndex,
    image_store::Image,
};
use anyhow::Result;

sonic_service!(SearchService, [Search, GetEntityImage]);

pub struct SearchService {
    index: EntityIndex,
    // dropping the handle leaves the cluster
    #[allow(unused)]
    cluster_handle: Cluster,
}
impl SearchService {
    async fn new(config: config::EntitySearchServerConfig) -> Result<Self> {
        let index = EntityIndex::open(config.index_path)?;

        let cluster_handle = Cluster::join(
            Member {
                id: config.cluster_id,
                service: Service::EntitySearcher { host: config.host },
            },
            config.gossip_addr,
            config.gossip_seed_nodes.unwrap_or_default(),
        )
        .await?;

        Ok(SearchService {
            index,
            cluster_handle,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Search {
    pub query: String,
}

impl sonic::service::Message<SearchService> for Search {
    type Response = Option<crate::entity_index::EntityMatch>;
    async fn handle(self, server: &SearchService) -> Self::Response {
        server.index.search(&self.query)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetEntityImage {
    pub image_id: String,
    pub max_width: Option<u64>,
    pub max_height: Option<u64>,
}
impl sonic::service::Message<SearchService> for GetEntityImage {
    type Response = Option<Image>;
    async fn handle(self, server: &SearchService) -> Self::Response {
        server.index.retrieve_image(&self.image_id).map(|img| {
            let max_width = self.max_width.unwrap_or(u64::MAX) as u32;
            let max_height = self.max_height.unwrap_or(u64::MAX) as u32;

            img.resize_max(max_width, max_height)
        })
    }
}

pub async fn run(config: config::EntitySearchServerConfig) -> Result<()> {
    let addr = config.host;
    let server = SearchService::new(config).await?.bind(addr).await.unwrap();

    tracing::info!(
        "entity search server is ready to accept requests on {}",
        addr
    );

    loop {
        if let Err(e) = server.accept().await {
            tracing::error!("{:?}", e);
        }
    }
}
