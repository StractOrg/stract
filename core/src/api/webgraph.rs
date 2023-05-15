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
use itertools::Itertools;

use crate::{
    distributed::{cluster::Cluster, member::Service, retry_strategy::ExponentialBackoff, sonic},
    similar_sites::ScoredNode,
    webgraph::Node,
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

#[derive(serde::Serialize, serde::Deserialize)]
struct ScoredSite {
    pub site: String,
    pub score: f64,
}

#[allow(clippy::unused_async)]
pub async fn similar_sites(
    extract::State(state): extract::State<Arc<State>>,
    extract::Json(params): extract::Json<SimilarSitesParams>,
) -> std::result::Result<impl IntoResponse, StatusCode> {
    let host = state.remote_webgraph.host().await;

    let retry = ExponentialBackoff::from_millis(30)
        .with_limit(Duration::from_millis(200))
        .take(5);

    let mut conn = sonic::ResilientConnection::create(host, retry);

    match conn
        .send_with_timeout::<_, Vec<ScoredNode>>(
            &crate::entrypoint::webgraph_server::Request::SimilarSites {
                sites: params.sites,
                top_n: params.top_n,
            },
            Duration::from_secs(30),
        )
        .await
    {
        Ok(sonic::Response::Content(nodes)) => Ok(Json(
            nodes
                .into_iter()
                .map(|node| ScoredSite {
                    site: node.node.name,
                    score: node.score,
                })
                .collect_vec(),
        )),
        Err(err) => {
            tracing::error!("Failed to send request to webgraph: {}", err);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
        _ => Err(StatusCode::INTERNAL_SERVER_ERROR),
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

    let mut conn = sonic::ResilientConnection::create(host, retry);

    match conn
        .send_with_timeout::<_, Option<Node>>(
            &crate::entrypoint::webgraph_server::Request::Knows { site: params.site },
            Duration::from_secs(2),
        )
        .await
    {
        Ok(sonic::Response::Content(Some(node))) => Ok(Json(KnowsSite::Known { site: node.name })),
        Err(err) => {
            tracing::error!("Failed to send request to webgraph: {}", err);
            Ok(Json(KnowsSite::Unknown))
        }
        _ => Ok(Json(KnowsSite::Unknown)),
    }
}
