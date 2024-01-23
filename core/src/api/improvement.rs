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

use axum::{extract, response::IntoResponse};
use serde::Deserialize;
use url::Url;
use uuid::Uuid;

use crate::improvement::{ImprovementEvent, StoredQuery};

use super::State;

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

impl TryFrom<StoreParams> for StoredQuery {
    type Error = anyhow::Error;
    fn try_from(params: StoreParams) -> Result<Self, Self::Error> {
        let mut urls = Vec::new();
        for url in params.urls {
            urls.push(Url::parse(&url)?);
        }

        Ok(StoredQuery::new(params.query, urls))
    }
}

pub async fn store(
    extract::State(state): extract::State<Arc<State>>,
    extract::Json(params): extract::Json<StoreParams>,
) -> impl IntoResponse {
    match state.improvement_queue.as_ref() {
        Some(q) => match StoredQuery::try_from(params) {
            Ok(query) => {
                let qid = *query.qid();
                q.lock().await.push(ImprovementEvent::StoreQuery(query));

                qid.to_string()
            }
            Err(_) => String::new(),
        },
        None => String::new(),
    }
}
