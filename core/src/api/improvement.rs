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

use std::sync::Arc;

use askama::Template;
use axum::{extract, response::IntoResponse};
use serde::Deserialize;
use uuid::Uuid;

use crate::improvement::{AliceMessage, ImprovementEvent, StoredQuery};

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
    extract::State(state): extract::State<Arc<State>>,
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
    extract::State(state): extract::State<Arc<State>>,
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

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct AliceConversation {
    pub id: Uuid,
    pub messages: Vec<AliceMessage>,
}

#[allow(clippy::unused_async)]
pub async fn new_chat_id() -> impl IntoResponse {
    Uuid::new_v4().to_string()
}

#[allow(clippy::unused_async)]
pub async fn store_chat(
    extract::State(state): extract::State<Arc<State>>,
    extract::Json(params): extract::Json<AliceConversation>,
) {
    if let Some(q) = state.improvement_queue.as_ref() {
        q.lock().await.push(ImprovementEvent::Chat {
            chat: params.into(),
        })
    }
}

#[allow(clippy::unused_async)]
pub async fn settings(extract::State(state): extract::State<Arc<State>>) -> impl IntoResponse {
    let template = SettingsTemplate {
        with_alice: state.config.with_alice,
    };

    HtmlTemplate(template)
}

#[derive(Template)]
#[template(path = "settings/privacy/index.html")]
struct SettingsTemplate {
    with_alice: Option<bool>,
}
