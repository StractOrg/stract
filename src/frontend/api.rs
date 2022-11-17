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

use std::{collections::HashMap, sync::Arc};

use crate::{searcher::SearchQuery, webpage::region::Region};

use super::State;
use axum::{extract, response::IntoResponse, Extension, Json};

#[allow(clippy::unused_async)]
pub async fn search(
    extract::Query(params): extract::Query<HashMap<String, String>>,
    Extension(state): Extension<Arc<State>>,
) -> impl IntoResponse {
    let query = params.get("q").cloned().unwrap_or_default();

    let skip_pages = params.get("p").and_then(|p| p.parse().ok());

    let selected_region = params.get("gl").and_then(|gl| {
        if let Ok(region) = Region::from_gl(gl) {
            Some(region)
        } else {
            None
        }
    });

    match state
        .searcher
        .search_api(&SearchQuery {
            original: query.to_string(),
            selected_region,
            optic_program: None,
            site_rankings: None,
            skip_pages,
        })
        .await
    {
        Ok(result) => Json(result),
        Err(_) => panic!("Search failed"), // TODO: show 500 status to user here
    }
}
