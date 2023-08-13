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

use serde::{Deserialize, Serialize};

use crate::{
    config,
    crawler::{CrawlCoordinator, Crawler, JobResponse},
    distributed::sonic::{self, service::Message},
    sonic_service, Result,
};

pub async fn worker(config: config::CrawlerConfig) -> Result<()> {
    let crawler = Crawler::new(config).await?;

    crawler.wait().await;

    Ok(())
}

pub struct CoordinatorService {
    coordinator: Arc<CrawlCoordinator>,
}

sonic_service!(CoordinatorService, [NewJobs]);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewJobs {
    pub responses: Vec<JobResponse>,
    pub num_jobs: usize,
}
#[async_trait::async_trait]
impl Message<CoordinatorService> for NewJobs {
    type Response = crate::crawler::Response;

    async fn handle(self, server: &CoordinatorService) -> sonic::Result<Self::Response> {
        server.coordinator.add_responses(&self.responses)?;

        if server.coordinator.is_done() {
            tracing::info!("Crawl is done. Waiting for workers to finish.");
            Ok(crate::crawler::Response::Done)
        } else {
            let jobs = server.coordinator.sample_jobs(self.num_jobs)?;

            Ok(crate::crawler::Response::NewJobs { jobs })
        }
    }
}

pub async fn coordinator(config: config::CrawlCoordinatorConfig) -> Result<()> {
    let coordinator = Arc::new(CrawlCoordinator::new(
        config.crawldb_folder,
        config.num_urls_to_crawl,
        config.seed_urls,
    )?);

    let addr: SocketAddr = config.host;
    let server = CoordinatorService { coordinator }.bind(addr).await.unwrap();

    tracing::info!("Crawl coordinator listening on {}", addr);

    loop {
        let _ = server.accept().await;
    }
}
