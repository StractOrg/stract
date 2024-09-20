// Stract is an open source web search engine.
// Copyright (C) 2024 Stract ApS
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

use std::sync::Arc;

use axum::{extract, response::IntoResponse, Json};
use http::StatusCode;
use utoipa::{IntoParams, ToSchema};

use crate::{
    config::WebgraphGranularity,
    webgraph::{EdgeLimit, FullEdge, Node},
};

use super::State;

pub mod host {
    use url::Url;

    use crate::entrypoint::webgraph_server::ScoredHost;

    use super::*;

    #[derive(serde::Deserialize, ToSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct SimilarHostsParams {
        pub hosts: Vec<String>,
        pub top_n: usize,
    }

    #[derive(serde::Deserialize, IntoParams)]
    #[serde(rename_all = "camelCase")]
    pub struct KnowsHostParams {
        pub host: String,
    }

    #[derive(serde::Deserialize, IntoParams)]
    #[serde(rename_all = "camelCase")]
    pub struct HostLinksParams {
        pub host: String,
    }

    #[utoipa::path(post,
        path = "/beta/api/webgraph/host/similar",
        request_body(content = SimilarHostsParams),
        responses(
            (status = 200, description = "List of similar hosts", body = Vec<ScoredHost>),
        )
    )]
    pub async fn similar(
        extract::State(state): extract::State<Arc<State>>,
        extract::Json(params): extract::Json<SimilarHostsParams>,
    ) -> std::result::Result<impl IntoResponse, StatusCode> {
        state.counters.explore_counter.inc();

        let hosts: Vec<_> = params.hosts.into_iter().take(8).collect();

        Ok(Json(
            state
                .similar_hosts
                .find_similar_hosts(&hosts, params.top_n)
                .await
                .into_iter()
                .map(|node| ScoredHost {
                    host: node.node.as_str().to_string(),
                    score: node.score,
                    description: None,
                })
                .collect::<Vec<_>>(),
        ))
    }

    #[utoipa::path(post,
        path = "/beta/api/webgraph/host/knows",
        params(KnowsHostParams),
        responses(
            (status = 200, description = "Whether the host is known", body = KnowsHost),
        )
    )]
    pub async fn knows(
        extract::State(state): extract::State<Arc<State>>,
        extract::Query(params): extract::Query<KnowsHostParams>,
    ) -> std::result::Result<impl IntoResponse, StatusCode> {
        match state.host_webgraph.knows(params.host).await {
            Ok(Some(node)) => Ok(Json(KnowsHost::Known {
                host: node.as_str().to_string(),
            })),
            Err(err) => {
                tracing::error!("Failed to send request to webgraph: {}", err);
                Ok(Json(KnowsHost::Unknown))
            }
            _ => Ok(Json(KnowsHost::Unknown)),
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
        let url = Url::parse(&("http://".to_string() + params.host.as_str()))
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        let node = Node::from(url).into_host();
        let links = ingoing_links(state, node, WebgraphGranularity::Host)
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
        let url = Url::parse(&("http://".to_string() + params.host.as_str()))
            .map_err(|_| StatusCode::BAD_REQUEST)?;
        let node = Node::from(url).into_host();
        let links = outgoing_links(state, node, WebgraphGranularity::Host)
            .await
            .map_err(|_| {
                tracing::error!("Failed to send request to webgraph");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

        Ok(Json(links))
    }
}

pub mod page {
    use url::Url;

    use crate::webpage::url_ext::UrlExt;

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
        let page = Url::robust_parse(&params.page).map_err(|_| StatusCode::BAD_REQUEST)?;
        let node = Node::from(page);
        let links = ingoing_links(state, node, WebgraphGranularity::Page)
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
        let url = Url::robust_parse(&params.page).map_err(|_| StatusCode::BAD_REQUEST)?;
        let node = Node::from(url);
        let links = outgoing_links(state, node, WebgraphGranularity::Page)
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
    level: WebgraphGranularity,
) -> anyhow::Result<Vec<FullEdge>> {
    match level {
        WebgraphGranularity::Host => {
            state
                .host_webgraph
                .ingoing_edges(node, EdgeLimit::Limit(1024))
                .await
        }
        WebgraphGranularity::Page => {
            state
                .page_webgraph
                .ingoing_edges(node, EdgeLimit::Limit(1024))
                .await
        }
    }
}

async fn outgoing_links(
    state: Arc<State>,
    node: Node,
    level: WebgraphGranularity,
) -> anyhow::Result<Vec<FullEdge>> {
    match level {
        WebgraphGranularity::Host => {
            state
                .host_webgraph
                .outgoing_edges(node, EdgeLimit::Limit(1024))
                .await
        }
        WebgraphGranularity::Page => {
            state
                .page_webgraph
                .outgoing_edges(node, EdgeLimit::Limit(1024))
                .await
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, ToSchema)]
#[serde(tag = "_type", rename_all = "camelCase")]
pub enum KnowsHost {
    Known { host: String },
    Unknown,
}
