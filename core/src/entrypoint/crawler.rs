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

use std::net::SocketAddr;

use crate::{
    crawler::{CrawlCoordinator, Crawler},
    distributed::sonic,
    CrawlCoordinatorConfig, CrawlerConfig, Result,
};

pub async fn worker(config: CrawlerConfig) -> Result<()> {
    let crawler = Crawler::new(config).await?;

    crawler.wait().await;

    Ok(())
}

pub async fn coordinator(config: CrawlCoordinatorConfig) -> Result<()> {
    let coordinator = CrawlCoordinator::new(
        config.crawldb_folder,
        config.num_urls_to_crawl,
        config.seed_urls,
    )?;

    let addr: SocketAddr = config.host;
    let server = sonic::Server::bind(addr).await.unwrap();

    tracing::info!("Crawl coordinator listening on {}", addr);

    loop {
        if let Ok(req) = server.accept::<crate::crawler::Request>().await {
            match &req.body {
                crate::crawler::Request::NewJobs { num_jobs } => {
                    if coordinator.is_done() {
                        tracing::info!("Crawl is done. Waiting for workers to finish.");
                        req.respond(sonic::Response::Content(crate::crawler::Response::Done))
                            .await
                            .ok();
                    } else {
                        let jobs = coordinator.sample_jobs(*num_jobs)?;

                        req.respond(sonic::Response::Content(
                            crate::crawler::Response::NewJobs { jobs },
                        ))
                        .await
                        .ok();
                    }
                }
                crate::crawler::Request::CrawlResult { job_response } => {
                    coordinator.add_response(job_response)?;

                    req.respond(sonic::Response::Content(crate::crawler::Response::Ok))
                        .await
                        .ok();
                }
            }
        }
    }
}
