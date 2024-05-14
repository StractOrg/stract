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

use axum::extract;
use http::StatusCode;
use optics::{HostRankings, Optic};
use utoipa::ToSchema;

#[derive(serde::Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ExploreExportOpticParams {
    chosen_hosts: Vec<String>,
    similar_hosts: Vec<String>,
}

#[allow(clippy::unused_async)]
#[utoipa::path(post,
    path = "/beta/api/explore/export",
    request_body(content = ExploreExportOpticParams),
    responses(
        (status = 200, description = "Export explored sites as an optic", body = String),
    )
)]
pub async fn explore_export_optic(
    extract::Json(ExploreExportOpticParams {
        chosen_hosts,
        similar_hosts,
    }): extract::Json<ExploreExportOpticParams>,
) -> Result<String, StatusCode> {
    let matches = similar_hosts
        .into_iter()
        .chain(chosen_hosts.clone().into_iter())
        .map(|site| {
            vec![optics::Matching {
                pattern: vec![
                    optics::PatternPart::Anchor,
                    optics::PatternPart::Raw(site),
                    optics::PatternPart::Anchor,
                ],
                location: optics::MatchLocation::Domain,
            }]
        })
        .collect();
    let rule = optics::Rule {
        matches,
        action: optics::Action::Boost(0),
    };

    let optic = Optic {
        host_rankings: HostRankings {
            liked: chosen_hosts,
            ..Default::default()
        },
        rules: vec![rule],
        discard_non_matching: true,
    };

    Ok(optic.to_string())
}
