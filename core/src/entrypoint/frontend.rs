// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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

use anyhow::Result;

use crate::{
    api::{metrics_router, router},
    FrontendConfig,
};

pub async fn run(config: FrontendConfig) -> Result<()> {
    let search_counter = crate::metrics::Counter::default();
    let mut registry = crate::metrics::PrometheusRegistry::default();

    let group = registry
        .new_group(
            "total_search_requests".to_string(),
            Some("Total number of incoming search requests.".to_string()),
        )
        .unwrap();
    group.register(search_counter.clone(), Vec::new());

    let app = router(&config, search_counter)?;
    let metrics_app = metrics_router(registry)?;

    let addr = config.host.parse()?;
    tracing::info!("frontend server listening on {}", addr);
    let server = axum::Server::bind(&addr).serve(app.into_make_service());

    let addr = config.prometheus_host.parse()?;
    tracing::info!("prometheus exporter listening on {}", addr);
    let metrics_server = axum::Server::bind(&addr).serve(metrics_app.into_make_service());

    tokio::try_join!(server, metrics_server)?;

    Ok(())
}
