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

use axum::{extract, Json};
use http::StatusCode;
use optics::{Optic, SiteRankings};
use utoipa::ToSchema;

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
