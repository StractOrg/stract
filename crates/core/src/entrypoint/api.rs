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

use std::{future::IntoFuture, net::SocketAddr, sync::Arc};

use anyhow::Result;
use futures::TryFutureExt;
use tokio::net::TcpListener;
use tracing::info;

use crate::{
    api::{metrics_router, router, user_count, Counters},
    config,
    distributed::{
        cluster::Cluster,
        member::{Member, Service},
        sonic::{self, service::sonic_service},
    },
    inverted_index::KeyPhrase,
    metrics::Label,
    searcher::{DistributedSearcher, SearchClient},
};

use super::search_server::SearchService;

fn counters(registry: &mut crate::metrics::PrometheusRegistry) -> Result<Counters> {
    let search_counter_success = crate::metrics::Counter::default();
    let search_counter_fail = crate::metrics::Counter::default();
    let explore_counter = crate::metrics::Counter::default();
    let daily_active_users = user_count::UserCount::new()?;

    let group = registry
        .new_group(
            "stract_search_requests".to_string(),
            Some("Total number of incoming search requests.".to_string()),
        )
        .unwrap();

    group.register(
        search_counter_success.clone(),
        vec![Label {
            key: "status".to_string(),
            val: "success".to_string(),
        }],
    );
    group.register(
        search_counter_fail.clone(),
        vec![Label {
            key: "status".to_string(),
            val: "fail".to_string(),
        }],
    );

    let group = registry
        .new_group(
            "stract_explore_requests".to_string(),
            Some("Total number of incoming requests to explore api.".to_string()),
        )
        .unwrap();
    group.register(explore_counter.clone(), vec![]);

    let group = registry
        .new_group(
            "stract_daily_active_users".to_string(),
            Some("Approximate number of unique daily active users.".to_string()),
        )
        .unwrap();
    group.register(daily_active_users.metric(), vec![]);

    Ok(Counters {
        search_counter_success,
        search_counter_fail,
        explore_counter,
        daily_active_users,
    })
}

async fn cluster(config: &config::ApiConfig) -> Result<Cluster> {
    Cluster::join(
        Member::new(Service::Api { host: config.host }),
        config.gossip_addr,
        config.gossip_seed_nodes.clone().unwrap_or_default(),
    )
    .await
}

pub struct ManagementService {
    cluster: Arc<Cluster>,
    searcher: DistributedSearcher,
}
sonic_service!(ManagementService, [TopKeyphrases, ClusterStatus, Size]);

impl ManagementService {
    pub async fn new(cluster: Arc<Cluster>) -> Result<Self> {
        let searcher = DistributedSearcher::new(Arc::clone(&cluster)).await;
        Ok(ManagementService { cluster, searcher })
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct TopKeyphrases {
    pub top: usize,
}
impl sonic::service::Message<ManagementService> for TopKeyphrases {
    type Response = Vec<KeyPhrase>;
    async fn handle(self, server: &ManagementService) -> Self::Response {
        server.searcher.top_key_phrases(self.top).await
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct Status {
    pub members: Vec<Member>,
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct ClusterStatus;
impl sonic::service::Message<ManagementService> for ClusterStatus {
    type Response = Status;
    async fn handle(self, server: &ManagementService) -> Self::Response {
        Status {
            members: server.cluster.members().await,
        }
    }
}

#[derive(Debug, Clone, Copy, bincode::Encode, bincode::Decode)]
pub struct Size;

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct SizeResponse {
    pub pages: u64,
}

impl std::ops::Add for SizeResponse {
    type Output = Self;
    fn add(mut self, other: Self) -> Self {
        self += other;
        self
    }
}

impl std::ops::AddAssign for SizeResponse {
    fn add_assign(&mut self, other: Self) {
        self.pages += other.pages;
    }
}

impl sonic::service::Message<ManagementService> for Size {
    type Response = SizeResponse;
    async fn handle(self, server: &ManagementService) -> Self::Response {
        let mut res = SizeResponse { pages: 0 };

        let mut checked_shards = std::collections::HashSet::new();

        for member in server.cluster.members().await {
            if let Service::Searcher { host, shard } = member.service {
                if checked_shards.contains(&shard) {
                    continue;
                }

                let mut client: sonic::service::Connection<SearchService> =
                    sonic::service::Connection::create(host).await.unwrap();
                let size = client.send_without_timeout(Size).await.unwrap();

                res += size;

                checked_shards.insert(shard);
            }
        }

        res
    }
}

async fn run_management(addr: SocketAddr, cluster: Arc<Cluster>) -> Result<()> {
    let server = ManagementService::new(cluster).await?.bind(addr).await?;

    info!(
        "management interface is ready to accept requests on {}",
        addr
    );

    loop {
        if let Err(e) = server.accept().await {
            tracing::error!("{:?}", e);
        }
    }
}

pub async fn run(config: config::ApiConfig) -> Result<()> {
    let mut registry = crate::metrics::PrometheusRegistry::default();
    let counters = counters(&mut registry)?;

    let cluster = Arc::new(cluster(&config).await?);

    let app = router(&config, counters, cluster.clone()).await?;
    let metrics_app = metrics_router(registry);

    let addr = config.host;
    tracing::info!("api server listening on {}", addr);
    let server = axum::serve(
        TcpListener::bind(&addr).await.unwrap(),
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .into_future()
    .map_err(|e| anyhow::anyhow!(e));

    let addr = config.prometheus_host;
    tracing::info!("prometheus exporter listening on {}", addr);
    let metrics_server = axum::serve(
        TcpListener::bind(&addr).await.unwrap(),
        metrics_app.into_make_service(),
    )
    .into_future()
    .map_err(|e| e.into());

    let management = tokio::spawn(async move {
        run_management(config.management_host, cluster)
            .await
            .unwrap();
    })
    .map_err(|e| e.into());

    tokio::try_join!(server, metrics_server, management)?;

    Ok(())
}
