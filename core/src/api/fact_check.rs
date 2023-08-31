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

use axum::{extract, response::IntoResponse, Json};
use utoipa::ToSchema;

#[derive(serde::Deserialize, serde::Serialize, Debug, ToSchema)]
pub struct FactCheckParams {
    claim: String,
    evidence: String,
}

#[derive(serde::Deserialize, serde::Serialize, Debug, ToSchema)]
pub struct FactCheckResponse {
    score: f64,
}

#[utoipa::path(
    post,
    path = "/beta/api/fact_check",
    request_body(content = FactCheckParams),
    responses(
        (status = 200, description = "Fact check the given claim against the given evidence", body = FactCheckResponse),
    )
)]
pub async fn fact_check_route(
    extract::State(state): extract::State<Arc<super::State>>,
    extract::Json(params): extract::Json<FactCheckParams>,
) -> Result<impl IntoResponse, http::StatusCode> {
    let score = state
        .fact_checker
        .run(&params.claim, &params.evidence)
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(FactCheckResponse { score }))
}
