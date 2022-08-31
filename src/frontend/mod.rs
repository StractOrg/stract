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

use crate::{
    autosuggest::Autosuggest, bangs::Bangs, entity_index::EntityIndex, index::Index,
    searcher::Searcher,
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
mod autosuggest;
mod entity_image;
mod favicons;
mod index;
mod primary_image;
mod privacy;
pub mod search;

pub struct HtmlTemplate<T>(T);

pub struct State {
    pub searcher: Searcher,
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
            include_bytes!("../../static/images/favicon.ico").to_vec(),
        ))
        .unwrap()
}

pub fn router(
    index_path: &str,
    queries_csv_path: &str,
    entity_index_path: Option<String>,
    bangs_path: Option<String>,
) -> Result<Router> {
    let entity_index = entity_index_path.map(|path| EntityIndex::open(path).unwrap());
    let bangs = bangs_path.map(Bangs::from_path);
    let search_index = Index::open(index_path)?;
    let autosuggest = Autosuggest::load_csv(queries_csv_path)?;
    let searcher = Searcher::new(search_index, entity_index, bangs);

    let state = Arc::new(State {
        searcher,
        autosuggest,
    });

    Ok(Router::new()
        .route("/", get(index::route))
        .route("/search", get(search::route))
        .route("/autosuggest", get(autosuggest::route))
        .route("/favicons/:site", get(favicons::route))
        .route("/image/:uuid", get(primary_image::route))
        .route("/entity/image/:entity", get(entity_image::route))
        .route("/favicon.ico", get(favicon))
        .route("/about", get(about::route))
        .route("/privacy-and-happy-lawyers", get(privacy::route))
        .merge(SpaRouter::new("/static", "static"))
        .layer(Extension(state)))
}
