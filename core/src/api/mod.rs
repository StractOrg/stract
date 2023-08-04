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

//! The api module contains the http api and frontend.
//! All http requests are handled using axum.
//! The frontend is served using a combination of axum, askama and astro
//! (with some funky astro hacks to make askama and astro play nice together).

use axum::{body::Body, extract, middleware, routing::get_service, Router};
use tokio::sync::Mutex;
use tower_http::{compression::CompressionLayer, services::ServeDir};

use crate::{
    autosuggest::Autosuggest,
    bangs::Bangs,
    config::FrontendConfig,
    distributed::{
        cluster::Cluster,
        member::{Member, Service},
    },
    fact_check_model::FactCheckModel,
    improvement::{store_improvements_loop, ImprovementEvent},
    leaky_queue::LeakyQueue,
    qa_model::QaModel,
    ranking::models::{cross_encoder::CrossEncoderModel, lambdamart::LambdaMART},
    searcher::frontend::FrontendSearcher,
    summarizer::Summarizer,
};
use anyhow::Result;
use std::sync::Arc;

use askama::Template;
use axum::{
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::get,
    routing::post,
};

use self::webgraph::RemoteWebgraph;

mod about;
mod alice;
mod autosuggest;
mod chat;
mod crawler;
mod docs;
mod explore;
mod fact_check;
pub mod improvement;
mod index;
mod metrics;
mod opensearch;
mod optics;
mod privacy;
pub mod search;
mod sites;
mod summarize;
mod webgraph;

pub struct HtmlTemplate<T>(T);

pub struct Counters {
    pub search_counter_success: crate::metrics::Counter,
    pub search_counter_fail: crate::metrics::Counter,
    pub explore_counter: crate::metrics::Counter,
}

pub struct State {
    pub config: FrontendConfig,
    pub searcher: FrontendSearcher,
    pub remote_webgraph: RemoteWebgraph,
    pub autosuggest: Autosuggest,
    pub counters: Counters,
    pub summarizer: Arc<Summarizer>,
    pub fact_checker: Arc<FactCheckModel>,
    pub improvement_queue: Option<Arc<Mutex<LeakyQueue<ImprovementEvent>>>>,
    pub cluster: Arc<Cluster>,
}

impl<T> IntoResponse for HtmlTemplate<T>
where
    T: Template,
{
    fn into_response(self) -> Response {
        match self.0.render() {
            Ok(html) => Html(html).into_response(),
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to render template. Error: {err}"),
            )
                .into_response(),
        }
    }
}

#[allow(clippy::unused_async)]
pub async fn favicon() -> impl IntoResponse {
    Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(
            include_bytes!("../../../frontend/dist/favicon.ico").to_vec(),
        ))
        .unwrap()
}

pub async fn router(config: &FrontendConfig, counters: Counters) -> Result<Router> {
    let autosuggest = Autosuggest::load_csv(&config.queries_csv_path)?;
    let crossencoder = CrossEncoderModel::open(&config.crossencoder_model_path)?;

    let lambda_model = match &config.lambda_model_path {
        Some(path) => Some(LambdaMART::open(path)?),
        None => None,
    };

    let qa_model = match &config.qa_model_path {
        Some(path) => Some(QaModel::open(path)?),
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
                service: Service::Frontend { host: config.host },
            },
            config.gossip_addr,
            config.gossip_seed_nodes.clone().unwrap_or_default(),
        )
        .await?,
    );
    let remote_webgraph = RemoteWebgraph::new(cluster.clone());
    let searcher = FrontendSearcher::new(
        cluster.clone(),
        crossencoder,
        lambda_model,
        qa_model,
        bangs,
        config.collector.clone(),
        config.thresholds.clone(),
    );

    let state = Arc::new(State {
        config: config.clone(),
        searcher,
        autosuggest,
        counters,
        remote_webgraph,
        summarizer: Arc::new(Summarizer::open(&config.summarizer_path)?),
        fact_checker: Arc::new(FactCheckModel::open(&config.fact_check_model_path)?),
        improvement_queue: query_store_queue,
        cluster,
    });

    Ok(Router::new()
        .route("/", get(index::route))
        .merge(
            Router::new()
                .route("/search", get(search::route))
                .route("/beta/api/search", post(search::api))
                .route_layer(middleware::from_fn_with_state(state.clone(), search_metric)),
        )
        .route("/autosuggest", get(autosuggest::route))
        .route("/autosuggest/browser", get(autosuggest::browser))
        .route("/favicon.ico", get(favicon))
        .route("/explore", get(explore::route))
        .route("/explore/export", get(explore::export))
        .route("/chat", get(chat::route))
        .route("/about", get(about::route))
        .route("/settings", get(optics::route))
        .route("/settings/optics", get(optics::route))
        .route("/settings/sites", get(sites::route))
        .route("/settings/privacy", get(improvement::settings))
        .route("/privacy-and-happy-lawyers", get(privacy::route))
        .route("/webmasters", get(crawler::info_route))
        .route("/opensearch.xml", get(opensearch::route))
        .route("/improvement/click", post(improvement::click))
        .route("/improvement/store", post(improvement::store))
        .route(
            "/improvement/alice/new_chat_id",
            post(improvement::new_chat_id),
        )
        .route(
            "/improvement/alice/store_chat",
            post(improvement::store_chat),
        )
        .fallback(get_service(ServeDir::new("frontend/dist/")))
        .layer(CompressionLayer::new())
        .merge(docs::router())
        .nest(
            "/beta",
            Router::new()
                .route("/api/summarize", get(summarize::route))
                .route("/api/webgraph/similar_sites", post(webgraph::similar_sites))
                .route("/api/webgraph/knows_site", get(webgraph::knows_site))
                .route("/api/alice", get(alice::route))
                .route("/api/alice/save_state", post(alice::save_state))
                .route("/api/fact_check", post(fact_check::route)),
        )
        .with_state(state))
}

pub fn metrics_router(registry: crate::metrics::PrometheusRegistry) -> Router {
    Router::new()
        .route("/metrics", get(metrics::route))
        .with_state(Arc::new(registry))
}

async fn search_metric<B>(
    extract::State(state): extract::State<Arc<State>>,
    request: axum::http::Request<B>,
    next: middleware::Next<B>,
) -> Response {
    let response = next.run(request).await;

    if response.status().is_success() {
        state.counters.search_counter_success.inc();
    } else {
        state.counters.search_counter_fail.inc();
    }

    response
}
