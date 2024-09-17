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

#[cfg(test)]
mod tests;

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crate::{
    ampc::dht::ShardId,
    config::LiveIndexConfig,
    distributed::{
        cluster::Cluster,
        member::{LiveIndexState, Member, Service},
        remote_cp,
        sonic::{self, service::sonic_service},
    },
    inverted_index,
    live_index::{IndexManager, LiveIndex},
    searcher::{InitialWebsiteResult, LocalSearcher},
};
use anyhow::{Context, Result};
use futures::stream::FuturesUnordered;
use itertools::Itertools;
use simple_wal::Wal;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use tracing::info;

use super::{
    indexer::{self, IndexableWebpage},
    search_server::{RetrieveWebsites, Search},
};

const INDEXING_TIMEOUT: Duration = Duration::from_secs(60);
const INDEXING_RETRIES: usize = 3;

sonic_service!(
    LiveIndexService,
    [
        RetrieveWebsites,
        Search,
        IndexWebpages,
        GetIndexPath,
        RemoteDownload
    ]
);

fn start_manager(index: Arc<LiveIndex>) {
    let manager = IndexManager::new(index);
    std::thread::spawn(|| manager.run());
}

struct FileDownloadStepper {
    conn: Mutex<sonic::service::Connection<LiveIndexService>>,
}

impl remote_cp::Stepper for FileDownloadStepper {
    async fn step(&self, req: remote_cp::Request) -> remote_cp::Response {
        self.conn
            .lock()
            .await
            .send(RemoteDownload { req })
            .await
            .unwrap()
    }
}

async fn other_replicas(cluster: &Cluster, shard: &ShardId, id: &str) -> Vec<SocketAddr> {
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    cluster
        .members()
        .await
        .into_iter()
        .filter_map(|member| {
            if member.id != id {
                if let Service::LiveIndex {
                    host,
                    shard: member_shard,
                    state,
                } = member.service
                {
                    if &member_shard == shard && matches!(state, LiveIndexState::Ready) {
                        return Some(host);
                    }
                }
            }

            None
        })
        .collect()
}

async fn setup(index: Arc<LiveIndex>, cluster: Arc<Cluster>, temp_wal: TempWal) -> Result<()> {
    let self_node = cluster
        .self_node()
        .expect("cluster should not be joined as spectator");

    let Service::LiveIndex {
        host,
        shard,
        state: _,
    } = self_node.service.clone()
    else {
        panic!("self node should be a live index")
    };

    let mut others = other_replicas(&cluster, &shard, &self_node.id).await;

    if let Some(other) = others.pop() {
        let mut conn: sonic::service::Connection<LiveIndexService> =
            sonic::service::Connection::create(other).await?;
        index.delete_all_pages();
        let local_path = index.path();
        let remote_path = conn.send(GetIndexPath).await?;

        remote_cp::Request::download(
            remote_path,
            local_path,
            &FileDownloadStepper {
                conn: Mutex::new(conn),
            },
        )
        .await;
    }

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

    cluster
        .set_service(Service::LiveIndex {
            host,
            shard,
            state: LiveIndexState::Ready,
        })
        .await?;

    Ok(())
}

type TempWal = Arc<Mutex<Option<Wal<IndexableWebpage>>>>;

pub struct LiveIndexService {
    local_searcher: LocalSearcher<Arc<LiveIndex>>,
    temp_wal: TempWal,
    index: Arc<LiveIndex>,
    cluster_handle: Arc<Cluster>,
}

impl LiveIndexService {
    async fn new(config: LiveIndexConfig) -> Result<Self> {
        let cluster_handle = Arc::new(
            Cluster::join(
                Member {
                    id: config.cluster_id.clone(),
                    service: Service::LiveIndex {
                        host: config.host,
                        shard: config.shard_id,
                        state: crate::distributed::member::LiveIndexState::InSetup,
                    },
                },
                config.gossip_addr,
                config.gossip_seed_nodes.clone().unwrap_or_default(),
            )
            .await?,
        );
        let index_path = Path::new(&config.index_path);

        let index = Arc::new(
            LiveIndex::new(
                index_path.join("index"),
                indexer::worker::Config {
                    host_centrality_store_path: config.host_centrality_store_path.clone(),
                    page_centrality_store_path: config.page_centrality_store_path.clone(),
                    page_webgraph: Some(indexer::worker::IndexerGraphConfig::Existing {
                        cluster: cluster_handle.clone(),
                    }),
                    safety_classifier_path: config.safety_classifier_path.clone(),
                    dual_encoder: None,
                },
            )
            .await?,
        );
        let local_searcher = LocalSearcher::new(index.clone());

        let temp_wal = Arc::new(Mutex::new(Some(Wal::open(index_path.join("wal.temp"))?)));

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
        let cluster = self.cluster_handle.clone();

        tokio::task::spawn(async move { setup(index, cluster, temp_wal).await.unwrap() });
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
                match index.index_webpages_without_consistency(&pages).await {
                    Ok(_) => Ok(index),
                    Err(e) => Err(e),
                }
            }));
        }

        let mut missing_responses =
            (((ready as f64) * consistency_fraction).ceil() as u64).min(ready as u64);

        while let Some(Ok(index)) = futures.next().await {
            if let Ok(index) = index {
                if matches!(index.state, LiveIndexState::Ready) {
                    missing_responses = missing_responses.saturating_sub(1);
                }

                if missing_responses == 0 {
                    break;
                }
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
    async fn index_webpages_without_consistency(
        &self,
        webpages: &[IndexableWebpage],
    ) -> Result<()> {
        let req = IndexWebpages {
            pages: webpages.to_vec(),
            consistency_fraction: None,
        };

        for _ in 0..INDEXING_RETRIES {
            let mut conn: sonic::service::Connection<LiveIndexService> =
                sonic::service::Connection::create(self.host)
                    .await
                    .with_context(|| format!("failed to connect to {}", self.host))?;

            if conn
                .send_with_timeout(req.clone(), INDEXING_TIMEOUT)
                .await
                .is_ok()
            {
                return Ok(());
            }
        }

        Err(anyhow::anyhow!("failed to replicate webpages"))
    }
}

#[derive(thiserror::Error, Debug, bincode::Encode, bincode::Decode)]
pub enum IndexingError {
    #[error("failed to replicate to the necessary quorom")]
    InsufficientReplication,
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct IndexWebpages {
    pub pages: Vec<IndexableWebpage>,
    pub consistency_fraction: Option<f64>,
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

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct GetIndexPath;

impl sonic::service::Message<LiveIndexService> for GetIndexPath {
    type Response = PathBuf;

    async fn handle(self, server: &LiveIndexService) -> Self::Response {
        server.index.path()
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct RemoteDownload {
    req: remote_cp::Request,
}
impl sonic::service::Message<LiveIndexService> for RemoteDownload {
    type Response = remote_cp::Response;

    async fn handle(self, _: &LiveIndexService) -> Self::Response {
        remote_cp::Response::handle(self.req).unwrap()
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
