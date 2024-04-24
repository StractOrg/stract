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

//! The api module contains the http api.
//! All http requests are handled using axum.

use axum::{body::Body, extract, middleware, Router};
use tokio::sync::Mutex;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::compression::CompressionLayer;

use crate::{
    autosuggest::Autosuggest,
    bangs::Bangs,
    config::ApiConfig,
    distributed::{
        cluster::Cluster,
        member::{Member, Service},
    },
    improvement::{store_improvements_loop, ImprovementEvent},
    leaky_queue::LeakyQueue,
    models::dual_encoder::DualEncoder,
    ranking::models::lambdamart::LambdaMART,
    searcher::{api::ApiSearcher, live::LiveSearcher, DistributedSearcher},
    webgraph::remote::RemoteWebgraph,
};

use crate::{ranking::models::cross_encoder::CrossEncoderModel, summarizer::Summarizer};

use anyhow::Result;
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    routing::post,
};

mod autosuggest;
mod docs;
mod explore;
mod hosts;
pub mod improvement;
mod metrics;
pub mod search;
mod summarize;
pub mod user_count;
mod webgraph;

pub struct Counters {
    pub search_counter_success: crate::metrics::Counter,
    pub search_counter_fail: crate::metrics::Counter,
    pub explore_counter: crate::metrics::Counter,
    pub daily_active_users: user_count::UserCount<user_count::Daily>,
}

pub struct State {
    pub config: ApiConfig,
    pub searcher: ApiSearcher<DistributedSearcher, LiveSearcher>,
    pub remote_webgraph_page: RemoteWebgraph,
    pub remote_webgraph_host: RemoteWebgraph,
    pub autosuggest: Autosuggest,
    pub counters: Counters,
    pub summarizer: Arc<Summarizer>,
    pub improvement_queue: Option<Arc<Mutex<LeakyQueue<ImprovementEvent>>>>,
    pub cluster: Arc<Cluster>,
}

pub async fn favicon() -> impl IntoResponse {
    Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(
            include_bytes!("../../../../frontend/static/favicon.ico").to_vec(),
        ))
        .unwrap()
}

fn build_router(state: Arc<State>) -> Router {
    let mut search = Router::new()
        .route("/beta/api/search", post(search::search))
        .route_layer(middleware::from_fn_with_state(state.clone(), search_metric))
        .layer(cors_layer());

    if let Some(limit) = state.config.max_concurrent_searches {
        search = search.layer(ConcurrencyLimitLayer::new(limit));
    }

    Router::new()
        .merge(search)
        .route("/favicon.ico", get(favicon))
        .merge(
            Router::new()
                .route("/improvement/click", post(improvement::click))
                .route("/improvement/store", post(improvement::store))
                .layer(cors_layer()),
        )
        .layer(CompressionLayer::new())
        .merge(docs::router())
        .nest(
            "/beta",
            Router::new()
                .route("/api/search/widget", post(search::widget))
                .route("/api/search/sidebar", post(search::sidebar))
                .route("/api/search/spellcheck", post(search::spellcheck))
                .route("/api/autosuggest", post(autosuggest::route))
                .route("/api/autosuggest/browser", get(autosuggest::browser))
                .route("/api/summarize", get(summarize::summarize_route))
                .route("/api/webgraph/host/similar", post(webgraph::host::similar))
                .route("/api/webgraph/host/knows", post(webgraph::host::knows))
                .route(
                    "/api/webgraph/host/ingoing",
                    post(webgraph::host::ingoing_hosts),
                )
                .route(
                    "/api/webgraph/host/outgoing",
                    post(webgraph::host::outgoing_hosts),
                )
                .route(
                    "/api/webgraph/page/ingoing",
                    post(webgraph::page::ingoing_pages),
                )
                .route(
                    "/api/webgraph/page/outgoing",
                    post(webgraph::page::outgoing_pages),
                )
                .route("/api/hosts/export", post(hosts::hosts_export_optic))
                .route("/api/explore/export", post(explore::explore_export_optic))
                .route("/api/entity_image", get(search::entity_image))
                .layer(cors_layer()),
        )
        .with_state(state)
}

pub async fn router(config: &ApiConfig, counters: Counters) -> Result<Router> {
    let autosuggest = Autosuggest::load_csv(&config.queries_csv_path)?;

    let lambda_model = match &config.lambda_model_path {
        Some(path) => Some(LambdaMART::open(path)?),
        None => None,
    };

    let dual_encoder_model = match &config.dual_encoder_model_path {
        Some(path) => Some(DualEncoder::open(path)?),
        None => None,
    };

    let query_store_queue = config.query_store_db_host.clone().map(|db_host| {
        let query_store_queue = Arc::new(Mutex::new(LeakyQueue::new(10_000)));
        tokio::spawn(store_improvements_loop(query_store_queue.clone(), db_host));
        query_store_queue
    });

    let bangs = Bangs::from_path(&config.bangs_path);

    let cluster = Arc::new(
        Cluster::join(
            Member {
                id: config.cluster_id.clone(),
                service: Service::Api { host: config.host },
            },
            config.gossip_addr,
            config.gossip_seed_nodes.clone().unwrap_or_default(),
        )
        .await?,
    );

    let remote_webgraph_host =
        RemoteWebgraph::new(cluster.clone(), crate::config::WebgraphGranularity::Host);
    let remote_webgraph_page =
        RemoteWebgraph::new(cluster.clone(), crate::config::WebgraphGranularity::Page);

    let dist_searcher = DistributedSearcher::new(Arc::clone(&cluster));
    let live_searcher = LiveSearcher::new(Arc::clone(&cluster));

    let state = {
        let mut cross_encoder = None;

        if let Some(path) = config.crossencoder_model_path.as_ref() {
            cross_encoder = Some(CrossEncoderModel::open(path)?);
        }

        let searcher = ApiSearcher::new(
            dist_searcher,
            Some(live_searcher),
            cross_encoder,
            lambda_model,
            dual_encoder_model,
            bangs,
            config.clone(),
        );

        Arc::new(State {
            config: config.clone(),
            searcher,
            autosuggest,
            counters,
            remote_webgraph_host,
            remote_webgraph_page,
            summarizer: Arc::new(Summarizer::new(
                &config.summarizer_path,
                config.llm.api_base.clone(),
                config.llm.model.clone(),
                config.llm.api_key.clone(),
            )?),
            improvement_queue: query_store_queue,
            cluster,
        })
    };

    Ok(build_router(state))
}

/// Enables CORS for development where the API and frontend are on
/// different hosts.
fn cors_layer() -> tower_http::cors::CorsLayer {
    #[cfg(feature = "cors")]
    return tower_http::cors::CorsLayer::permissive();
    #[cfg(not(feature = "cors"))]
    tower_http::cors::CorsLayer::new()
}

pub fn metrics_router(registry: crate::metrics::PrometheusRegistry) -> Router {
    Router::new()
        .route("/metrics", get(metrics::route))
        .with_state(Arc::new(registry))
}

async fn search_metric(
    extract::State(state): extract::State<Arc<State>>,
    extract::ConnectInfo(addr): extract::ConnectInfo<SocketAddr>,
    request: axum::extract::Request,
    next: middleware::Next,
) -> Response {
    // It is very important that the ip address is not stored. It is only used
    // for a probabilistic estimate of the number of unique users using a hyperloglog datastructure.
    let mut ip = None;

    if let Some(forwarded_for) = request.headers().get("x-forwarded-for") {
        let forwarded_for = forwarded_for.to_str().unwrap_or_default();
        if let Some(client_ip) = forwarded_for.split(',').next() {
            if let Ok(client_ip) = client_ip.trim().parse::<IpAddr>() {
                ip = Some(client_ip);
            }
        }
    }

    let ip = ip.unwrap_or_else(|| addr.ip());
    state.counters.daily_active_users.inc(&ip).ok();

    let response = next.run(request).await;

    if response.status().is_success() {
        state.counters.search_counter_success.inc();
    } else if response.status().is_server_error() {
        state.counters.search_counter_fail.inc();
    }

    response
}
