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

use axum::{Extension, Router};

use crate::index::Index;
use anyhow::Result;
use std::sync::Arc;

use askama::Template;
use axum::{
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::get,
};
use axum_extra::routing::SpaRouter;

mod index;
pub mod search;

pub struct HtmlTemplate<T>(T);

pub struct State {
    pub index: Index,
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

pub fn router(index_path: &str) -> Result<Router> {
    let search_index = Index::open(index_path)?;
    let state = Arc::new(State {
        index: search_index,
    });

    Ok(Router::new()
        .route("/", get(index::route))
        .route("/search", get(search::route))
        .merge(SpaRouter::new("/static", "static"))
        .layer(Extension(state)))
}
