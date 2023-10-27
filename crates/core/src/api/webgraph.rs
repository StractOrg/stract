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
use distributed::{cluster::Cluster, member::Service, retry_strategy::ExponentialBackoff};
use http::StatusCode;
use utoipa::{IntoParams, ToSchema};
use webgraph::{FullEdge, Node};

use crate::entrypoint::webgraph_server::GraphLevel;

use super::State;

pub struct RemoteWebgraph {
    cluster: Arc<Cluster>,
}

impl RemoteWebgraph {
    pub fn new(cluster: Arc<Cluster>) -> Self {
        Self { cluster }
    }

    async fn host(&self) -> Option<SocketAddr> {
        self.cluster
            .members()
            .await
            .iter()
            .find_map(|member| match member.service {
                Service::Webgraph { host } => Some(host),
                _ => None,
            })
    }
}

pub mod host {
    use super::*;

    #[derive(serde::Deserialize, ToSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct SimilarSitesParams {
        pub sites: Vec<String>,
        pub top_n: usize,
    }

    #[derive(serde::Deserialize, IntoParams)]
    #[serde(rename_all = "camelCase")]
    pub struct KnowsSiteParams {
        pub site: String,
    }

    #[derive(serde::Deserialize, IntoParams)]
    #[serde(rename_all = "camelCase")]
    pub struct HostLinksParams {
        pub site: String,
    }

    #[allow(clippy::unused_async)]
    #[utoipa::path(post,
        path = "/beta/api/webgraph/host/similar",
        request_body(content = SimilarSitesParams),
        responses(
            (status = 200, description = "List of similar sites", body = Vec<ScoredSite>),
        )
    )]
    pub async fn similar(
        extract::State(state): extract::State<Arc<State>>,
        extract::Json(params): extract::Json<SimilarSitesParams>,
    ) -> std::result::Result<impl IntoResponse, StatusCode> {
        state.counters.explore_counter.inc();
        let host = state
            .remote_webgraph
            .host()
            .await
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        let retry = ExponentialBackoff::from_millis(30)
            .with_limit(Duration::from_millis(200))
            .take(5);

        let conn = sonic::service::ResilientConnection::create_with_timeout(
            host,
            Duration::from_secs(30),
            retry,
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        match conn
            .send_with_timeout(
                &crate::entrypoint::webgraph_server::SimilarSites {
                    sites: params.sites,
                    top_n: params.top_n,
                },
                Duration::from_secs(60),
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

    #[utoipa::path(post,
        path = "/beta/api/webgraph/host/knows",
        params(KnowsSiteParams),
        responses(
            (status = 200, description = "Whether the site is known", body = KnowsSite),
        )
    )]
    pub async fn knows(
        extract::State(state): extract::State<Arc<State>>,
        extract::Query(params): extract::Query<KnowsSiteParams>,
    ) -> std::result::Result<impl IntoResponse, StatusCode> {
        let host = state
            .remote_webgraph
            .host()
            .await
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        let retry = ExponentialBackoff::from_millis(30)
            .with_limit(Duration::from_millis(200))
            .take(5);

        let conn = sonic::service::ResilientConnection::create_with_timeout(
            host,
            Duration::from_secs(30),
            retry,
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        match conn
            .send_with_timeout(
                &crate::entrypoint::webgraph_server::Knows { site: params.site },
                Duration::from_secs(60),
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

    #[utoipa::path(post,
        path = "/beta/api/webgraph/host/ingoing",
        params(HostLinksParams),
        responses(
            (status = 200, description = "Incoming links for a particular host", body = Vec<FullEdge>),
        )
    )]
    pub async fn ingoing_hosts(
        extract::State(state): extract::State<Arc<State>>,
        extract::Query(params): extract::Query<HostLinksParams>,
    ) -> std::result::Result<impl IntoResponse, StatusCode> {
        let node = Node::from(params.site).into_host();
        let links = ingoing_links(state, node, GraphLevel::Host)
            .await
            .map_err(|_| {
                tracing::error!("Failed to send request to webgraph");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

        Ok(Json(links))
    }

    #[utoipa::path(post,
        path = "/beta/api/webgraph/host/outgoing",
        params(HostLinksParams),
        responses(
            (status = 200, description = "Outgoing links for a particular host", body = Vec<FullEdge>),
        )
    )]
    pub async fn outgoing_hosts(
        extract::State(state): extract::State<Arc<State>>,
        extract::Query(params): extract::Query<HostLinksParams>,
    ) -> std::result::Result<impl IntoResponse, StatusCode> {
        let node = Node::from(params.site).into_host();
        let links = outgoing_links(state, node, GraphLevel::Host)
            .await
            .map_err(|_| {
                tracing::error!("Failed to send request to webgraph");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

        Ok(Json(links))
    }
}

pub mod page {
    use super::*;

    #[derive(serde::Deserialize, IntoParams)]
    #[serde(rename_all = "camelCase")]
    pub struct PageLinksParams {
        pub page: String,
    }

    #[utoipa::path(post,
        path = "/beta/api/webgraph/page/ingoing",
        params(PageLinksParams),
        responses(
            (status = 200, description = "Incoming links for a particular page", body = Vec<FullEdge>),
        )
    )]
    pub async fn ingoing_pages(
        extract::State(state): extract::State<Arc<State>>,
        extract::Query(params): extract::Query<PageLinksParams>,
    ) -> std::result::Result<impl IntoResponse, StatusCode> {
        let node = Node::from(params.page);
        let links = ingoing_links(state, node, GraphLevel::Page)
            .await
            .map_err(|_| {
                tracing::error!("Failed to send request to webgraph");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

        Ok(Json(links))
    }

    #[utoipa::path(post,
        path = "/beta/api/webgraph/page/outgoing",
        params(PageLinksParams),
        responses(
            (status = 200, description = "Outgoing links for a particular page", body = Vec<FullEdge>),
        )
    )]
    pub async fn outgoing_pages(
        extract::State(state): extract::State<Arc<State>>,
        extract::Query(params): extract::Query<PageLinksParams>,
    ) -> std::result::Result<impl IntoResponse, StatusCode> {
        let node = Node::from(params.page);
        let links = outgoing_links(state, node, GraphLevel::Page)
            .await
            .map_err(|_| {
                tracing::error!("Failed to send request to webgraph");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

        Ok(Json(links))
    }
}

async fn ingoing_links(
    state: Arc<State>,
    node: Node,
    level: GraphLevel,
) -> anyhow::Result<Vec<FullEdge>> {
    let host = state
        .remote_webgraph
        .host()
        .await
        .ok_or(anyhow::anyhow!("no remote webgraph"))?;

    let retry = ExponentialBackoff::from_millis(30)
        .with_limit(Duration::from_millis(200))
        .take(5);

    let conn = sonic::service::ResilientConnection::create_with_timeout(
        host,
        Duration::from_secs(30),
        retry,
    )
    .await?;

    Ok(conn
        .send_with_timeout(
            &crate::entrypoint::webgraph_server::IngoingLinks { node, level },
            Duration::from_secs(60),
        )
        .await?)
}

async fn outgoing_links(
    state: Arc<State>,
    node: Node,
    level: GraphLevel,
) -> anyhow::Result<Vec<FullEdge>> {
    let host = state
        .remote_webgraph
        .host()
        .await
        .ok_or(anyhow::anyhow!("no remote webgraph"))?;

    let retry = ExponentialBackoff::from_millis(30)
        .with_limit(Duration::from_millis(200))
        .take(5);

    let conn = sonic::service::ResilientConnection::create_with_timeout(
        host,
        Duration::from_secs(30),
        retry,
    )
    .await?;

    Ok(conn
        .send_with_timeout(
            &crate::entrypoint::webgraph_server::OutgoingLinks { node, level },
            Duration::from_secs(60),
        )
        .await?)
}

#[derive(serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum KnowsSite {
    Known { site: String },
    Unknown,
}
