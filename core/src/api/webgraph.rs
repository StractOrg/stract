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

use std::{net::SocketAddr, sync::Arc, time::Duration};

use axum::{extract, response::IntoResponse, Json};
use http::StatusCode;

use crate::distributed::{
    cluster::Cluster, member::Service, retry_strategy::ExponentialBackoff, sonic,
};

use super::State;

pub struct RemoteWebgraph {
    cluster: Arc<Cluster>,
}

impl RemoteWebgraph {
    pub fn new(cluster: Arc<Cluster>) -> Self {
        Self { cluster }
    }

    async fn host(&self) -> SocketAddr {
        self.cluster
            .members()
            .await
            .iter()
            .find_map(|member| match member.service {
                Service::Webgraph { host } => Some(host),
                _ => None,
            })
            .unwrap()
    }
}

#[derive(serde::Deserialize)]
pub struct SimilarSitesParams {
    pub sites: Vec<String>,
    pub top_n: usize,
}

#[allow(clippy::unused_async)]
pub async fn similar_sites(
    extract::State(state): extract::State<Arc<State>>,
    extract::Json(params): extract::Json<SimilarSitesParams>,
) -> std::result::Result<impl IntoResponse, StatusCode> {
    state.counters.explore_counter.inc();
    let host = state.remote_webgraph.host().await;

    let retry = ExponentialBackoff::from_millis(30)
        .with_limit(Duration::from_millis(200))
        .take(5);

    let conn = sonic::service::ResilientConnection::create(host, retry);

    match conn
        .send_with_timeout(
            crate::entrypoint::webgraph_server::SimilarSites {
                sites: params.sites,
                top_n: params.top_n,
            },
            Duration::from_secs(30),
        )
        .await
    {
        Ok(nodes) => Ok(Json(nodes)),
        Err(err) => {
            tracing::error!("Failed to send request to webgraph: {}", err);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[derive(serde::Deserialize)]
pub struct KnowsSiteParams {
    pub site: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "@type", rename_all = "camelCase")]
pub enum KnowsSite {
    Known { site: String },
    Unknown,
}

#[allow(clippy::unused_async)]
pub async fn knows_site(
    extract::State(state): extract::State<Arc<State>>,
    extract::Query(params): extract::Query<KnowsSiteParams>,
) -> std::result::Result<impl IntoResponse, StatusCode> {
    let host = state.remote_webgraph.host().await;

    let retry = ExponentialBackoff::from_millis(30)
        .with_limit(Duration::from_millis(200))
        .take(5);

    let conn = sonic::service::ResilientConnection::create(host, retry);

    match conn
        .send_with_timeout(
            crate::entrypoint::webgraph_server::Knows { site: params.site },
            Duration::from_secs(2),
        )
        .await
    {
        Ok(Some(node)) => Ok(Json(KnowsSite::Known { site: node.name })),
        Err(err) => {
            tracing::error!("Failed to send request to webgraph: {}", err);
            Ok(Json(KnowsSite::Unknown))
        }
        _ => Ok(Json(KnowsSite::Unknown)),
    }
}
