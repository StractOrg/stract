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

use super::{HtmlTemplate, State};
use askama::Template;
use axum::{extract, response::IntoResponse, Json};
use http::StatusCode;
use optics::{Optic, SiteRankings};
use utoipa::ToSchema;

#[allow(clippy::unused_async)]
pub async fn route(extract::State(state): extract::State<Arc<State>>) -> impl IntoResponse {
    let template = SitesTemplate {
        with_alice: state.config.with_alice,
    };

    HtmlTemplate(template)
}

#[derive(Template)]
#[template(path = "settings/sites/index.html")]
struct SitesTemplate {
    with_alice: Option<bool>,
}

#[derive(serde::Deserialize)]
pub struct ExportParams {
    pub data: String,
}

#[allow(clippy::unused_async)]
pub async fn export(
    extract::Query(ExportParams { data }): extract::Query<ExportParams>,
) -> Result<impl IntoResponse, StatusCode> {
    let data = data.replace(' ', "+");
    match lz_str::decompress_from_base64(&data) {
        Some(bytes) => match String::from_utf16(&bytes) {
            Ok(s) => match serde_json::from_str::<SiteRankings>(&s) {
                Ok(site_rankings) => {
                    let optic = Optic {
                        site_rankings,
                        rules: vec![],
                        ..Default::default()
                    };

                    Ok(optic.to_string())
                }
                Err(err) => {
                    tracing::error!("Failed to parse data: {}", err);
                    Err(StatusCode::BAD_REQUEST)
                }
            },
            Err(err) => {
                tracing::error!("Failed to parse data: {}", err);
                Err(StatusCode::BAD_REQUEST)
            }
        },
        None => {
            tracing::error!("Failed to de-compress data");
            Err(StatusCode::BAD_REQUEST)
        }
    }
}

#[derive(serde::Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SitesExportOpticParams {
    site_rankings: SiteRankings,
}

#[allow(clippy::unused_async)]
#[utoipa::path(post,
    path = "/beta/api/sites/export",
    request_body(content = SitesExportOpticParams),
    responses(
        (status = 200, description = "Export site rankings as an optic", body = String),
    )
)]
pub async fn sites_export_optic(
    extract::Json(SitesExportOpticParams { site_rankings }): extract::Json<SitesExportOpticParams>,
) -> Result<Json<String>, StatusCode> {
    let optic = Optic {
        site_rankings,
        ..Default::default()
    };

    Ok(Json(optic.to_string()))
}
