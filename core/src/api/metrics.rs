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

use axum::{response::IntoResponse, Extension};
use axum_macros::debug_handler;

#[allow(clippy::unused_async)]
#[allow(clippy::match_wild_err_arm)]
#[debug_handler]
pub async fn route(
    Extension(registry): Extension<Arc<crate::metrics::PrometheusRegistry>>,
) -> impl IntoResponse {
    format!("{}", registry)
}
