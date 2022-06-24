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

use crate::index::{Index, RetrievedWebpage};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;

use crate::query::Query;
use crate::ranking::Ranker;
use askama::Template;
use axum::{
    extract,
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::get,
};

async fn index(
    extract::Query(params): extract::Query<HashMap<String, String>>,
    Extension(state): Extension<Arc<State>>,
) -> impl IntoResponse {
    let mut search_result = Vec::new();

    if let Some(query) = params.get("q") {
        let query = Query::parse(query).expect("Failed to parse query");
        let ranker = Ranker::new(query.clone());

        let result = state
            .index
            .search(&query, ranker.collector())
            .expect("Search failed");

        search_result = dbg!(result.documents);
    }

    let template = IndexTemplate { search_result };
    HtmlTemplate(template)
}

#[derive(Template)]
#[template(path = "index.html", escape = "none")]
struct IndexTemplate {
    search_result: Vec<RetrievedWebpage>,
}

struct HtmlTemplate<T>(T);

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

    Ok(Router::new().route("/", get(index)).layer(Extension(state)))
}
