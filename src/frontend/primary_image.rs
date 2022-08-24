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

use axum::body::Body;
use axum::extract::Path;
use axum::http::Response;
use axum::response::IntoResponse;
use axum::Extension;
use reqwest::StatusCode;

use super::State;

#[allow(clippy::unused_async)]
pub async fn route(
    Path(uuid): Path<String>,
    Extension(state): Extension<Arc<State>>,
) -> impl IntoResponse {
    let img = state.searcher.primary_image(uuid);

    match img {
        Some(img) => Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(img.as_raw_bytes()))
            .unwrap(),
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap(),
    }
}
