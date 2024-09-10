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

use std::sync::Arc;

use crate::{
    config::LiveIndexConfig,
    distributed::{
        cluster::Cluster,
        member::{Member, Service},
        sonic::{self, service::sonic_service},
    },
    inverted_index,
    live_index::{IndexManager, LiveIndex},
    searcher::{InitialWebsiteResult, LocalSearcher},
};
use anyhow::Result;
use tracing::info;

use super::{
    indexer::IndexableWebpage,
    search_server::{RetrieveWebsites, Search},
};

sonic_service!(LiveIndexService, [RetrieveWebsites, Search, IndexWebpages]);

pub struct LiveIndexService {
    local_searcher: LocalSearcher<Arc<LiveIndex>>,
    index: Arc<LiveIndex>,
    // dropping the handle leaves the cluster
    #[allow(unused)]
    cluster_handle: Cluster,
}

impl LiveIndexService {
    async fn new(config: LiveIndexConfig) -> Result<Self> {
        let manager = IndexManager::new(config.clone())?;
        let local_searcher = LocalSearcher::new(manager.index());

        let index = manager.index();

        tokio::task::spawn(manager.run());

        let cluster_handle = Cluster::join(
            Member {
                id: config.cluster_id,
                service: Service::LiveIndex {
                    host: config.host,
                    shard: config.shard_id,
                    state: crate::distributed::member::LiveIndexState::InSetup,
                },
            },
            config.gossip_addr,
            config.gossip_seed_nodes.unwrap_or_default(),
        )
        .await?;

        todo!("check if there are other nodes in cluster with same shard and download index from them if this is the case");

        Ok(Self {
            local_searcher,
            cluster_handle,
            index,
        })
    }
}

impl sonic::service::Message<LiveIndexService> for RetrieveWebsites {
    type Response = Option<Vec<inverted_index::RetrievedWebpage>>;
    async fn handle(self, server: &LiveIndexService) -> Self::Response {
        server
            .local_searcher
            .retrieve_websites(&self.websites, &self.query)
            .ok()
    }
}

impl sonic::service::Message<LiveIndexService> for Search {
    type Response = Option<InitialWebsiteResult>;
    async fn handle(self, server: &LiveIndexService) -> Self::Response {
        server.local_searcher.search_initial(&self.query, true).ok()
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct IndexWebpages {
    pages: Vec<IndexableWebpage>,
}

impl sonic::service::Message<LiveIndexService> for IndexWebpages {
    type Response = ();

    async fn handle(self, server: &LiveIndexService) -> Self::Response {
        server.index.insert(&self.pages);

        todo!("send write to all other replicas and make sure we get response from `config.consistency_fraction` before succeeding");
    }
}

pub async fn serve(config: LiveIndexConfig) -> Result<()> {
    let addr = config.host;

    let server = LiveIndexService::new(config)
        .await?
        .bind(&addr)
        .await
        .unwrap();

    info!("live index is ready to accept requests on {}", addr);

    loop {
        if let Err(e) = server.accept().await {
            tracing::error!("{:?}", e);
        }
    }
}
