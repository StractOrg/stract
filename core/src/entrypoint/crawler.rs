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

use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

use crate::{
    config,
    crawler::{self, CrawlCoordinator, Crawler, Domain, DomainCrawled, JobResponse, UrlToInsert},
    distributed::sonic::{self, service::Message},
    sonic_service, Result,
};

pub async fn worker(config: config::CrawlerConfig) -> Result<()> {
    let crawler = Crawler::new(config).await?;

    crawler.run().await;

    Ok(())
}

pub async fn coordinator(config: config::CrawlCoordinatorConfig) -> Result<()> {
    let coordinator = Arc::new(CrawlCoordinator::new(
        config.crawldb_folder,
        config.seed_urls,
    )?);

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
    let router = crawler::Router::new(config.coordinator_addrs.clone());

    let addr: SocketAddr = config.host;

    let server = router::RouterService { router }.bind(addr).await.unwrap();

    tracing::info!("Crawl router listening on {}", addr);

    loop {
        let _ = server.accept().await;
    }
}

pub mod router {
    use super::*;
    pub struct RouterService {
        pub router: crawler::Router,
    }

    sonic_service!(RouterService, [NewJobs]);

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct NewJobs {
        pub responses: Vec<JobResponse>,
        pub num_jobs: usize,
    }

    #[async_trait::async_trait]
    impl Message<RouterService> for NewJobs {
        type Response = crate::crawler::Response;

        async fn handle(self, server: &RouterService) -> sonic::Result<Self::Response> {
            server.router.add_responses(&self.responses).await?;
            let jobs = server.router.sample_jobs(self.num_jobs).await?;
            Ok(crate::crawler::Response::NewJobs { jobs })
        }
    }
}

pub mod coordinator {
    use crate::crawler::Job;

    use super::*;

    pub struct CoordinatorService {
        pub coordinator: Arc<CrawlCoordinator>,
    }

    sonic_service!(CoordinatorService, [InsertUrls, GetJobs, MarkJobsComplete]);

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct InsertUrls {
        pub urls: HashMap<Domain, Vec<UrlToInsert>>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct GetJobs {
        pub num_jobs: usize,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct MarkJobsComplete {
        pub domains: Vec<DomainCrawled>,
    }

    #[async_trait::async_trait]
    impl Message<CoordinatorService> for InsertUrls {
        type Response = ();

        async fn handle(self, server: &CoordinatorService) -> sonic::Result<Self::Response> {
            server.coordinator.insert_urls(self.urls)?;
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl Message<CoordinatorService> for GetJobs {
        type Response = Vec<Job>;

        async fn handle(self, server: &CoordinatorService) -> sonic::Result<Self::Response> {
            let jobs = server.coordinator.sample_jobs(self.num_jobs)?;
            Ok(jobs)
        }
    }

    #[async_trait::async_trait]
    impl Message<CoordinatorService> for MarkJobsComplete {
        type Response = ();

        async fn handle(self, server: &CoordinatorService) -> sonic::Result<Self::Response> {
            server.coordinator.mark_jobs_complete(&self.domains)?;
            Ok(())
        }
    }
}
