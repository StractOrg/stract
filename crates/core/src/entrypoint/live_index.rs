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

use std::{net::SocketAddr, path::Path, sync::Arc, time::Duration};

use crate::{
    config::LiveIndexConfig,
    distributed::{
        cluster::Cluster,
        member::{LiveIndexState, Member, Service},
        sonic::{self, service::sonic_service},
    },
    inverted_index,
    live_index::{IndexManager, LiveIndex},
    searcher::{InitialWebsiteResult, LocalSearcher},
};
use anyhow::Result;
use futures::stream::FuturesUnordered;
use itertools::Itertools;
use simple_wal::Wal;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use tracing::info;

use super::{
    indexer::IndexableWebpage,
    search_server::{RetrieveWebsites, Search},
};

const INDEXING_TIMEOUT: Duration = Duration::from_secs(60);
const INDEXING_RETRIES: usize = 3;

sonic_service!(LiveIndexService, [RetrieveWebsites, Search, IndexWebpages]);

fn start_manager(index: Arc<LiveIndex>) {
    let manager = IndexManager::new(index);
    tokio::task::spawn_blocking(|| manager.run());
}

async fn setup(index: Arc<LiveIndex>, temp_wal: TempWal) {
    todo!("check if there are other nodes in cluster with same shard and download index from them if this is the case");

    let mut wal = temp_wal
        .lock()
        .await
        .take()
        .expect("temp_wal should always exist before setup has been run");

    for pages in wal.iter().unwrap().chunks(512).into_iter() {
        let pages: Vec<_> = pages.into_iter().collect();
        index.insert(&pages);
    }

    wal.clear().unwrap();

    start_manager(index);
}

type TempWal = Arc<Mutex<Option<Wal<IndexableWebpage>>>>;

pub struct LiveIndexService {
    local_searcher: LocalSearcher<Arc<LiveIndex>>,
    temp_wal: TempWal,
    index: Arc<LiveIndex>,
    cluster_handle: Cluster,
}

impl LiveIndexService {
    async fn new(config: LiveIndexConfig) -> Result<Self> {
        let cluster_handle = Cluster::join(
            Member {
                id: config.cluster_id.clone(),
                service: Service::LiveIndex {
                    host: config.host,
                    shard: config.shard_id,
                    state: crate::distributed::member::LiveIndexState::InSetup,
                },
            },
            config.gossip_addr.clone(),
            config.gossip_seed_nodes.clone().unwrap_or_default(),
        )
        .await?;

        let index = Arc::new(LiveIndex::new(config.clone())?);
        let local_searcher = LocalSearcher::new(index.clone());

        let temp_wal = Arc::new(Mutex::new(Some(Wal::open(
            Path::new(&config.index_path).join("temp.wal"),
        )?)));

        Ok(Self {
            local_searcher,
            cluster_handle,
            index,
            temp_wal,
        })
    }

    fn background_setup(&self) {
        let index = self.index.clone();
        let temp_wal = self.temp_wal.clone();

        tokio::task::spawn_blocking(|| setup(index, temp_wal));
    }

    async fn index_webpages_in_replicas(
        &self,
        webpages: &[IndexableWebpage],
        consistency_fraction: f64,
    ) -> Result<(), IndexingError> {
        let self_member = self
            .cluster_handle
            .self_node()
            .expect("node should be participating part of cluster");

        let self_id = self_member.id.clone();
        let Service::LiveIndex {
            host: _,
            shard: self_shard,
            state: _,
        } = self_member.service
        else {
            panic!("self_member should always be a live index")
        };

        let live_indexes: Vec<_> = self
            .cluster_handle
            .members()
            .await
            .into_iter()
            .filter_map(|member| {
                if let Service::LiveIndex { host, shard, state } = member.service {
                    if member.id != self_id && shard == self_shard {
                        Some(RemoteIndex { host, state })
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        let ready = live_indexes
            .iter()
            .filter(|index| matches!(index.state, LiveIndexState::Ready))
            .count();

        let mut futures = FuturesUnordered::new();

        for index in live_indexes {
            let pages = webpages.to_vec();

            futures.push(tokio::spawn(async move {
                index.index_webpages_without_consistency(&pages).await;
                index
            }));
        }

        let mut missing_responses = ((ready as f64) * consistency_fraction).ceil() as u64;

        while let Some(Ok(index)) = futures.next().await {
            if matches!(index.state, LiveIndexState::Ready) {
                missing_responses = missing_responses.saturating_sub(1);
            }

            if missing_responses == 0 {
                break;
            }
        }

        if missing_responses > 0 {
            return Err(IndexingError::InsufficientReplication);
        }

        Ok(())
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

#[derive(Clone, Debug)]
struct RemoteIndex {
    host: SocketAddr,
    state: LiveIndexState,
}

impl RemoteIndex {
    async fn index_webpages_without_consistency(&self, webpages: &[IndexableWebpage]) {
        let req = IndexWebpages {
            pages: webpages.to_vec(),
            consistency_fraction: None,
        };

        for _ in 0..INDEXING_RETRIES {
            let mut conn: sonic::service::Connection<LiveIndexService> =
                sonic::service::Connection::create(self.host)
                    .await
                    .expect(&format!("failed to connect to {}", self.host));

            if conn
                .send_with_timeout(req.clone(), INDEXING_TIMEOUT)
                .await
                .is_ok()
            {
                return;
            }
        }
    }
}

#[derive(thiserror::Error, Debug, bincode::Encode, bincode::Decode)]
pub enum IndexingError {
    #[error("failed to replicate to the necessary quorom")]
    InsufficientReplication,
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct IndexWebpages {
    pages: Vec<IndexableWebpage>,
    consistency_fraction: Option<f64>,
}

impl sonic::service::Message<LiveIndexService> for IndexWebpages {
    type Response = Result<(), IndexingError>;

    async fn handle(self, server: &LiveIndexService) -> Self::Response {
        if let Some(wal) = server.temp_wal.lock().await.as_mut() {
            wal.batch_write(self.pages.iter()).unwrap();
        } else {
            server.index.insert(&self.pages);

            if let Some(consistency_fraction) = self.consistency_fraction {
                server
                    .index_webpages_in_replicas(&self.pages, consistency_fraction)
                    .await?;
            }
        }

        Ok(())
    }
}

pub async fn serve(config: LiveIndexConfig) -> Result<()> {
    let addr = config.host;

    let service = LiveIndexService::new(config).await?;

    service.background_setup();

    let server = service.bind(&addr).await.unwrap();

    info!("live index is ready to accept requests on {}", addr);

    loop {
        if let Err(e) = server.accept().await {
            tracing::error!("{:?}", e);
        }
    }
}
