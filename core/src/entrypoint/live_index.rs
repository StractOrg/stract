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

use std::{path::Path, sync::Arc};

use crate::{
    config::{LiveIndexConfig, LiveIndexSchedulerConfig},
    distributed::{
        cluster::Cluster,
        member::{Member, Service},
        sonic,
    },
    feed::{self, index::FeedIndex},
    inverted_index,
    kv::rocksdb_store::RocksDbStore,
    live_index::{Index, IndexManager},
    ranking::inbound_similarity::InboundSimilarity,
    searcher::{InitialWebsiteResult, LocalSearcher},
    sonic_service,
    webgraph::WebgraphBuilder,
};
use anyhow::Result;
use tracing::info;

use super::search_server::{RetrieveWebsites, Search};

sonic_service!(SearchService, [RetrieveWebsites, Search]);

pub struct SearchService {
    local_searcher: LocalSearcher<Arc<Index>>,
    // dropping the handle leaves the cluster
    #[allow(unused)]
    cluster_handle: Cluster,
}

impl SearchService {
    async fn new(config: LiveIndexConfig) -> Result<Self> {
        let inbound_similarity = InboundSimilarity::open(
            Path::new(&config.host_centrality_store_path).join("inbound_similarity"),
        )?;

        let manager = IndexManager::new(config.clone())?;
        let mut local_searcher = LocalSearcher::new(manager.index());

        local_searcher.set_inbound_similarity(inbound_similarity);

        tokio::task::spawn(manager.run());

        let cluster_handle = Cluster::join(
            Member {
                id: config.cluster_id,
                service: Service::LiveIndex {
                    host: config.host,
                    split_id: config.split_id,
                },
            },
            config.gossip_addr,
            config.gossip_seed_nodes.unwrap_or_default(),
        )
        .await?;

        Ok(Self {
            local_searcher,
            cluster_handle,
        })
    }
}

impl sonic::service::Message<SearchService> for RetrieveWebsites {
    type Response = Option<Vec<inverted_index::RetrievedWebpage>>;
    async fn handle(self, server: &SearchService) -> sonic::Result<Self::Response> {
        match server
            .local_searcher
            .retrieve_websites(&self.websites, &self.query)
        {
            Ok(response) => Ok(Some(response)),
            Err(_) => Ok(None),
        }
    }
}

impl sonic::service::Message<SearchService> for Search {
    type Response = Option<InitialWebsiteResult>;
    async fn handle(self, server: &SearchService) -> sonic::Result<Self::Response> {
        match server.local_searcher.search_initial(&self.query, true) {
            Ok(result) => Ok(Some(result)),
            Err(_) => Ok(None),
        }
    }
}

pub async fn serve(config: LiveIndexConfig) -> Result<()> {
    let addr = config.host;

    let server = SearchService::new(config).await?.bind(&addr).await.unwrap();

    info!("live index is ready to accept requests on {}", addr);

    loop {
        if let Err(e) = server.accept().await {
            tracing::error!("{:?}", e);
        }
    }
}

pub fn schedule(config: LiveIndexSchedulerConfig) -> Result<()> {
    let feed_index = FeedIndex::open(config.feed_index_path)?;
    let host_harmonic =
        RocksDbStore::open(Path::new(&config.host_centrality_store_path).join("harmonic"));
    let host_graph = WebgraphBuilder::new(config.host_graph_path).open();

    let schedule =
        feed::scheduler::schedule(&feed_index, &host_harmonic, &host_graph, config.num_splits);
    schedule.save(config.schedule_path)?;

    Ok(())
}
