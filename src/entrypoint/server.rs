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

use std::net::SocketAddr;

use axum::{Extension, Router};

use crate::{
    index::{Index, RetrievedWebpage},
    server::Server,
    Result, ServerConfig,
};
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

pub struct ServerEntrypoint {
    config: ServerConfig,
}

impl From<ServerConfig> for ServerEntrypoint {
    fn from(config: ServerConfig) -> Self {
        Self { config }
    }
}

impl ServerEntrypoint {
    pub async fn run(self) -> Result<()> {
        let search_index = Index::open(&self.config.index_path)?;
        let state = Arc::new(Server {
            index: search_index,
        });

        let app = Router::new().route("/", get(index)).layer(Extension(state));
        let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
        tracing::debug!("listening on {}", addr);
        axum::Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .unwrap();

        Ok(())
    }
}

async fn index(
    extract::Query(params): extract::Query<HashMap<String, String>>,
    Extension(state): Extension<Arc<Server>>,
) -> impl IntoResponse {
    let mut search_result = Vec::new();

    if let Some(query) = params.get("q") {
        let query = Query::parse(query).expect("Failed to parse query");
        let ranker = Ranker::new(query.clone());

        let result = state
            .index
            .search(&query, ranker.collector())
            .expect("Search failed");

        search_result = result.documents;
    }

    let template = IndexTemplate { search_result };
    HtmlTemplate(template)
}

#[derive(Template)]
#[template(path = "index.html")]
struct IndexTemplate {
    search_result: Vec<RetrievedWebpage>,
}

struct HtmlTemplate<T>(T);

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
