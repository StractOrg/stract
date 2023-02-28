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

use std::sync::Arc;

use askama::Template;
use axum::{extract, response::IntoResponse, Extension};
use serde::Deserialize;
use uuid::Uuid;

use crate::query_store::{ImprovementEvent, StoredQuery};

use super::{HtmlTemplate, State};

#[derive(Deserialize, Debug)]
pub struct ClickParams {
    pub qid: Uuid,
    pub click: usize,
}

#[derive(Deserialize, Debug)]
pub struct StoreParams {
    pub query: String,
    pub urls: Vec<String>,
}

#[allow(clippy::unused_async)]
pub async fn click(
    extract::Query(params): extract::Query<ClickParams>,
    Extension(state): Extension<Arc<State>>,
) {
    if let Some(q) = state.improvement_queue.as_ref() {
        q.lock().await.push(ImprovementEvent::Click {
            qid: params.qid,
            idx: params.click,
        })
    }
}

impl From<StoreParams> for StoredQuery {
    fn from(params: StoreParams) -> Self {
        StoredQuery::new(
            params.query,
            params.urls.into_iter().map(|s| s.into()).collect(),
        )
    }
}

#[allow(clippy::unused_async)]
pub async fn store(
    Extension(state): Extension<Arc<State>>,
    extract::Json(params): extract::Json<StoreParams>,
) -> impl IntoResponse {
    match state.improvement_queue.as_ref() {
        Some(q) => {
            let query: StoredQuery = params.into();
            let qid = *query.qid();
            q.lock().await.push(ImprovementEvent::StoreQuery(query));

            qid.to_string()
        }
        None => String::new(),
    }
}

#[allow(clippy::unused_async)]
pub async fn settings() -> impl IntoResponse {
    let template = SettingsTemplate {};

    HtmlTemplate(template)
}

#[derive(Template)]
#[template(path = "settings/privacy/index.html")]
struct SettingsTemplate {}
