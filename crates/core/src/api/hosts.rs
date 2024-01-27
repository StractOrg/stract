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
use optics::{HostRankings, Optic};
use utoipa::ToSchema;

#[derive(serde::Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct HostsExportOpticParams {
    host_rankings: HostRankings,
}

#[utoipa::path(post,
    path = "/beta/api/hosts/export",
    request_body(content = HostsExportOpticParams),
    responses(
        (status = 200, description = "Export host rankings as an optic", body = String),
    )
)]
pub async fn hosts_export_optic(
    extract::Json(HostsExportOpticParams { host_rankings }): extract::Json<HostsExportOpticParams>,
) -> Result<Json<String>, StatusCode> {
    let optic = Optic {
        host_rankings,
        ..Default::default()
    };

    Ok(Json(optic.to_string()))
}
