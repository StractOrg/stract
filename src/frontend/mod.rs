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

use axum::{body::Body, Extension, Router};
use tower_http::compression::CompressionLayer;

use crate::{
    autosuggest::Autosuggest,
    searcher::{DistributedSearcher, Shard},
};
use anyhow::Result;
use std::sync::Arc;

use askama::Template;
use axum::{
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::get,
};
use axum_extra::routing::SpaRouter;

mod about;
mod api;
mod autosuggest;
mod goggles;
mod index;
mod opensearch;
mod privacy;
pub mod search;
mod sites;

pub struct HtmlTemplate<T>(T);

pub struct State {
    pub searcher: DistributedSearcher,
    pub autosuggest: Autosuggest,
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
                format!("Failed to render template. Error: {}", err),
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
            include_bytes!("../../frontend/dist/assets/favicon.ico").to_vec(),
        ))
        .unwrap()
}

pub fn router(queries_csv_path: &str, shards: Vec<Vec<String>>) -> Result<Router> {
    let shards: Vec<_> = shards
        .into_iter()
        .enumerate()
        .map(|(id, replicas)| Shard::new(id as u32, replicas))
        .collect();

    let autosuggest = Autosuggest::load_csv(queries_csv_path)?;
    let searcher = DistributedSearcher::new(shards);

    let state = Arc::new(State {
        searcher,
        autosuggest,
    });

    Ok(Router::new()
        .route("/", get(index::route))
        .route("/search", get(search::route))
        .route("/autosuggest", get(autosuggest::route))
        .route("/browser_autosuggest", get(autosuggest::browser))
        .route("/favicon.ico", get(favicon))
        .route("/about", get(about::route))
        .route("/settings", get(goggles::route))
        .route("/settings/goggles", get(goggles::route))
        .route("/settings/sites", get(sites::route))
        .route("/privacy-and-happy-lawyers", get(privacy::route))
        .route("/api/beta/search", get(api::search))
        .route("/opensearch.xml", get(opensearch::route))
        .merge(SpaRouter::new("/assets", "frontend/dist/assets"))
        .layer(Extension(state))
        .layer(CompressionLayer::new()))
}
