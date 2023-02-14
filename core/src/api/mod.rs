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

use axum::{body::Body, routing::get_service, Extension, Router};
use tower_http::{compression::CompressionLayer, services::ServeDir};

use crate::{
    autosuggest::Autosuggest,
    bangs::Bangs,
    qa_model::QaModel,
    ranking::models::cross_encoder::CrossEncoderModel,
    searcher::{DistributedSearcher, Shard},
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

mod about;
mod autosuggest;
mod index;
mod opensearch;
mod optics;
mod privacy;
pub mod search;
mod sites;
mod summarize;

pub struct HtmlTemplate<T>(T);

pub struct State {
    pub searcher: DistributedSearcher,
    pub autosuggest: Autosuggest,
    pub summarizer: Arc<Summarizer>,
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

pub fn router(
    queries_csv_path: &str,
    crossencoder_model_path: &str,
    qa_model_path: &str,
    bangs_path: &str,
    summarizer_path: &str,
    shards: Vec<Vec<String>>,
) -> Result<Router> {
    let shards: Vec<_> = shards
        .into_iter()
        .enumerate()
        .map(|(id, replicas)| Shard::new(id as u64, replicas))
        .collect();

    let autosuggest = Autosuggest::load_csv(queries_csv_path)?;
    let crossencoder = CrossEncoderModel::open(crossencoder_model_path)?;
    let qa_model = QaModel::open(qa_model_path)?;
    let bangs = Bangs::from_path(bangs_path);
    let searcher = DistributedSearcher::new(shards, crossencoder, qa_model, bangs);

    let state = Arc::new(State {
        searcher,
        autosuggest,
        summarizer: Arc::new(Summarizer::open(summarizer_path)?),
    });

    Ok(Router::new()
        .route("/", get(index::route))
        .route("/search", get(search::route))
        .route("/beta/api/search", post(search::api))
        .route("/autosuggest", get(autosuggest::route))
        .route("/autosuggest/browser", get(autosuggest::browser))
        .route("/favicon.ico", get(favicon))
        .route("/about", get(about::route))
        .route("/settings", get(optics::route))
        .route("/settings/optics", get(optics::route))
        .route("/settings/sites", get(sites::route))
        .route("/privacy-and-happy-lawyers", get(privacy::route))
        .route("/opensearch.xml", get(opensearch::route))
        .fallback(get_service(ServeDir::new("frontend/dist/")).handle_error(
            |error: std::io::Error| async move {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Unhandled internal error: {error}"),
                )
            },
        ))
        .layer(CompressionLayer::new())
        .merge(Router::new().route("/summarize", get(summarize::route)))
        .layer(Extension(state)))
}
