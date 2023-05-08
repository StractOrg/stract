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

use axum::extract;
use http::StatusCode;
use serde::Deserialize;

use super::State;

#[derive(Deserialize)]
pub struct Params {
    pub url: String,
    pub query: String,
}

#[allow(clippy::unused_async)]
pub async fn route(
    extract::Query(params): extract::Query<Params>,
    extract::State(state): extract::State<Arc<State>>,
) -> std::result::Result<String, StatusCode> {
    let webpage = state
        .searcher
        .get_webpage(&params.url)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(state.summarizer.summarize(&params.query, &webpage.body))
}
