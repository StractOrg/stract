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

use crate::webpage::Url;

use super::State;

#[allow(clippy::unused_async)]
pub async fn route(
    Path(site): Path<String>,
    Extension(state): Extension<Arc<State>>,
) -> impl IntoResponse {
    let img = state.searcher.favicon(&Url::from(site));

    let bytes = match img {
        Some(img) => img.as_raw_bytes(),
        None => include_bytes!("../../frontend/dist/assets/images/globe.png").to_vec(),
    };

    Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(bytes))
        .unwrap()
}
