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

use crawler::{coordinator, planner::make_crawl_plan, router, CrawlCoordinator, Crawler};
use kv::rocksdb_store::RocksDbStore;
use webgraph::WebgraphBuilder;

use crate::Result;

pub async fn worker(config: stract_config::CrawlerConfig) -> Result<()> {
    let crawler = Crawler::new(config).await?;

    crawler.run().await;

    Ok(())
}

pub async fn coordinator(config: stract_config::CrawlCoordinatorConfig) -> Result<()> {
    let coordinator = Arc::new(CrawlCoordinator::new(config.job_queue.as_ref())?);

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

pub async fn router(config: stract_config::CrawlRouterConfig) -> Result<()> {
    let router = router::Router::new(config.coordinator_addrs.clone()).await?;

    let addr: SocketAddr = config.host;

    let server = router::RouterService { router }.bind(addr).await.unwrap();

    tracing::info!("Crawl router listening on {}", addr);

    loop {
        let _ = server.accept().await;
    }
}

pub fn planner(config: stract_config::CrawlPlannerConfig) -> Result<()> {
    let page_centrality = RocksDbStore::open(config.page_harmonic_path.as_ref());
    let host_centrality = RocksDbStore::open(config.host_harmonic_path.as_ref());
    let page_graph = WebgraphBuilder::new(config.page_graph_path.as_ref()).open();
    let host_graph = WebgraphBuilder::new(config.host_graph_path.as_ref()).open();
    let output_path = config.output_path.clone();

    make_crawl_plan(
        host_centrality,
        page_centrality,
        host_graph,
        page_graph,
        config,
        output_path.as_ref(),
    )?;

    Ok(())
}
