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

use anyhow::Result;

use crate::{
    api::{metrics_router, router, user_count, Counters},
    metrics::Label,
};

pub async fn run(config: stract_config::ApiConfig) -> Result<()> {
    let search_counter_success = crate::metrics::Counter::default();
    let search_counter_fail = crate::metrics::Counter::default();
    let explore_counter = crate::metrics::Counter::default();
    let daily_active_users = user_count::UserCount::new()?;

    let mut registry = crate::metrics::PrometheusRegistry::default();

    let group = registry
        .new_group(
            "search_requests".to_string(),
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
            "explore_requests".to_string(),
            Some("Total number of incoming requests to explore api.".to_string()),
        )
        .unwrap();
    group.register(explore_counter.clone(), vec![]);

    let group = registry
        .new_group(
            "daily_active_users".to_string(),
            Some("Approximate number of unique daily active users.".to_string()),
        )
        .unwrap();
    group.register(daily_active_users.metric(), vec![]);

    let counters = Counters {
        search_counter_success,
        search_counter_fail,
        explore_counter,
        daily_active_users,
    };

    let app = router(&config, counters).await?;
    let metrics_app = metrics_router(registry);

    let addr = config.host;
    tracing::info!("api server listening on {}", addr);
    let server =
        axum::Server::bind(&addr).serve(app.into_make_service_with_connect_info::<SocketAddr>());

    let addr = config.prometheus_host;
    tracing::info!("prometheus exporter listening on {}", addr);
    let metrics_server = axum::Server::bind(&addr).serve(metrics_app.into_make_service());

    tokio::try_join!(server, metrics_server)?;

    Ok(())
}
