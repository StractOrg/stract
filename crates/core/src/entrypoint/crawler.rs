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

use std::{net::SocketAddr, sync::Arc};

use crate::{
    config,
    crawler::{self, planner::CrawlPlanner, CrawlCoordinator, Crawler},
    distributed::sonic::service::{sonic_service, Message},
    Result,
};

pub async fn worker(config: config::CrawlerConfig) -> Result<()> {
    let crawler = Crawler::new(config).await?;

    crawler.run().await;

    Ok(())
}

pub async fn coordinator(config: config::CrawlCoordinatorConfig) -> Result<()> {
    let coordinator = Arc::new(CrawlCoordinator::new(config.job_queue)?);

    let addr: SocketAddr = config.host;
    let server = coordinator::CoordinatorService { coordinator }
        .bind(addr)
        .await
        .unwrap();

    tracing::info!("Crawl coordinator listening on {}", addr);

    loop {
        let _ = server.accept().await;
    }
}

pub async fn router(config: config::CrawlRouterConfig) -> Result<()> {
    let router = crawler::Router::new(config.coordinator_addrs.clone()).await?;

    let addr: SocketAddr = config.host;

    let server = router::RouterService { router }.bind(addr).await.unwrap();

    tracing::info!("Crawl router listening on {}", addr);

    loop {
        let _ = server.accept().await;
    }
}

pub async fn planner(config: config::CrawlPlannerConfig) -> Result<()> {
    let page_centrality = speedy_kv::Db::open_or_create(&config.page_harmonic_path)?;
    let host_centrality = speedy_kv::Db::open_or_create(&config.host_harmonic_path)?;
    let host_centrality_rank =
        speedy_kv::Db::open_or_create(&config.host_centrality_rank_store_path)?;

    let gossip = config.gossip.clone();
    let cluster = Arc::new(
        crate::distributed::cluster::Cluster::join_as_spectator(
            gossip.addr,
            gossip.seed_nodes.unwrap_or_default(),
        )
        .await?,
    );

    let planner = CrawlPlanner::new(
        host_centrality,
        host_centrality_rank,
        page_centrality,
        cluster,
        config,
    )
    .await?;

    planner.build().await?;

    Ok(())
}

pub mod router {
    use crate::crawler::Job;

    use super::*;
    pub struct RouterService {
        pub router: crawler::Router,
    }

    sonic_service!(RouterService, [NewJob]);

    #[derive(
        Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
    )]
    pub struct NewJob {}

    impl Message<RouterService> for NewJob {
        type Response = Option<Job>;

        async fn handle(self, server: &RouterService) -> Self::Response {
            server.router.sample_job().await.ok().flatten()
        }
    }
}

pub mod coordinator {
    use crate::crawler::Job;

    use super::*;

    pub struct CoordinatorService {
        pub coordinator: Arc<CrawlCoordinator>,
    }

    sonic_service!(CoordinatorService, [GetJob]);

    #[derive(
        Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
    )]
    pub struct GetJob {}

    impl Message<CoordinatorService> for GetJob {
        type Response = Option<Job>;

        async fn handle(self, server: &CoordinatorService) -> Self::Response {
            server.coordinator.sample_job().ok().flatten()
        }
    }
}
