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
    webgraph::{
        query::{
            FullBacklinksQuery, FullForwardlinksQuery, FullHostBacklinksQuery,
            FullHostForwardlinksQuery,
        },
        EdgeLimit, Node, PrettyEdge,
    },
};

use super::State;

pub mod host {
    use url::Url;

    pub use crate::entrypoint::webgraph_server::ScoredHost;
    use crate::webpage::url_ext::UrlExt;

    use super::*;

    #[derive(serde::Serialize, serde::Deserialize, ToSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct SimilarHostsQuery {
        /// The hosts to find similar hosts for
        pub hosts: Vec<String>,
        /// The number of similar hosts to return
        pub top_n: usize,
        /// Filters the similar hosts to only include those that match the given filters
        pub filters: Option<Vec<String>>,
    }

    /// Similar Hosts
    ///
    /// Returns a list of hosts similar to the given hosts. The similarity between hosts is calculated
    /// based on how similar their inbound edges are - hosts that tend to be linked to by the
    /// same other hosts are considered more similar.
    ///
    /// For example, two news websites might be considered similar because they are both frequently
    /// linked to from social media sites, news aggregators, and blogs discussing current events.
    #[utoipa::path(post,
        path = "/beta/api/webgraph/host/similar",
        request_body(content = SimilarHostsQuery),
        responses(
            (status = 200, description = "List of similar hosts", body = Vec<ScoredHost>),
        )
    )]
    pub async fn similar(
        extract::State(state): extract::State<Arc<State>>,
        extract::Json(params): extract::Json<SimilarHostsQuery>,
    ) -> std::result::Result<impl IntoResponse, StatusCode> {
        state.counters.explore_counter.inc();

        let hosts: Vec<_> = params.hosts.into_iter().take(8).collect();

        Ok(Json(
            state
                .similar_hosts
                .find_similar_hosts(hosts, params.top_n, params.filters.unwrap_or_default())
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

    #[derive(serde::Serialize, serde::Deserialize, IntoParams)]
    #[serde(rename_all = "camelCase")]
    pub struct KnowsHostQuery {
        pub host: String,
    }

    #[utoipa::path(post,
        path = "/beta/api/webgraph/host/knows",
        params(KnowsHostQuery),
        responses(
            (status = 200, description = "Whether the host is known", body = KnowsHost)
        )
    )]
    pub async fn knows(
        extract::State(state): extract::State<Arc<State>>,
        extract::Query(params): extract::Query<KnowsHostQuery>,
    ) -> std::result::Result<impl IntoResponse, StatusCode> {
        match Url::robust_parse(&params.host) {
            Ok(url) => match url.tld() {
                None | Some("") => return Err(StatusCode::BAD_REQUEST),
                Some(_) => (),
            },
            Err(_) => return Err(StatusCode::BAD_REQUEST),
        }

        match state.webgraph.knows(params.host).await {
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

    #[derive(serde::Serialize, serde::Deserialize, IntoParams)]
    #[serde(rename_all = "camelCase")]
    pub struct HostLinksQuery {
        /// The host to get edges for
        pub host: String,
    }

    /// Ingoing Edges
    ///
    /// Returns the incoming edges (backlinks) for a particular host. For example, if site A links to site B,
    /// then site A would appear in the ingoing edges for site B.
    ///
    /// The results are limited to a reasonable number of the most important inbound edges.
    /// Edges are considered more important if they come from authoritative sites.
    #[utoipa::path(post,
        path = "/beta/api/webgraph/host/ingoing",
        params(HostLinksQuery),
        responses(
            (status = 200, description = "Incoming edges for a particular host", body = Vec<PrettyEdge>),
        )
    )]
    pub async fn ingoing_hosts(
        extract::State(state): extract::State<Arc<State>>,
        extract::Query(params): extract::Query<HostLinksQuery>,
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

    /// Outgoing Edges
    ///
    /// Returns the outgoing edges (forwardlinks) for a particular host. For example, if site A links to site B,
    /// then site B would appear in the outgoing edges for site A.
    ///
    /// The results are limited to a reasonable number of the most important outbound edges.
    #[utoipa::path(post,
        path = "/beta/api/webgraph/host/outgoing",
        params(HostLinksQuery),
        responses(
            (status = 200, description = "Outgoing edges for a particular host", body = Vec<PrettyEdge>),
        )
    )]
    pub async fn outgoing_hosts(
        extract::State(state): extract::State<Arc<State>>,
        extract::Query(params): extract::Query<HostLinksQuery>,
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

    #[derive(serde::Serialize, serde::Deserialize, IntoParams)]
    #[serde(rename_all = "camelCase")]
    pub struct PageLinksParams {
        pub page: String,
    }

    /// Ingoing Edges
    ///
    /// Returns the incoming edges (backlinks) for a particular page. For example, if page A links to page B,
    /// then page A would appear in the ingoing edges for page B.
    ///
    /// The results are limited to a reasonable number of the most important inbound edges.
    /// Edges are considered more important if they come from authoritative sites.
    #[utoipa::path(post,
        path = "/beta/api/webgraph/page/ingoing",
        params(PageLinksParams),
        responses(
            (status = 200, description = "Incoming edges for a particular page", body = Vec<PrettyEdge>),
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

    /// Outgoing Edges
    ///
    /// Returns the outgoing edges (forwardlinks) for a particular page. For example, if page A links to page B,
    /// then page A would appear in the outgoing edges for page B.
    ///
    /// The results are limited to a reasonable number of the most important outbound edges.
    #[utoipa::path(post,
        path = "/beta/api/webgraph/page/outgoing",
        params(PageLinksParams),
        responses(
            (status = 200, description = "Outgoing edges for a particular page", body = Vec<PrettyEdge>),
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
) -> anyhow::Result<Vec<PrettyEdge>> {
    match level {
        WebgraphGranularity::Host => state
            .webgraph
            .search(FullHostBacklinksQuery::new(node).with_limit(EdgeLimit::Limit(1024)))
            .await
            .map(|edges| edges.into_iter().map(PrettyEdge::from).collect()),
        WebgraphGranularity::Page => state
            .webgraph
            .search(FullBacklinksQuery::new(node).with_limit(EdgeLimit::Limit(1024)))
            .await
            .map(|edges| edges.into_iter().map(PrettyEdge::from).collect()),
    }
}

async fn outgoing_links(
    state: Arc<State>,
    node: Node,
    level: WebgraphGranularity,
) -> anyhow::Result<Vec<PrettyEdge>> {
    match level {
        WebgraphGranularity::Host => state
            .webgraph
            .search(FullHostForwardlinksQuery::new(node).with_limit(EdgeLimit::Limit(1024)))
            .await
            .map(|edges| edges.into_iter().map(PrettyEdge::from).collect()),
        WebgraphGranularity::Page => state
            .webgraph
            .search(FullForwardlinksQuery::new(node).with_limit(EdgeLimit::Limit(1024)))
            .await
            .map(|edges| edges.into_iter().map(PrettyEdge::from).collect()),
    }
}

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, ToSchema)]
#[serde(tag = "_type", rename_all = "camelCase")]
pub enum KnowsHost {
    Known { host: String },
    Unknown,
}
