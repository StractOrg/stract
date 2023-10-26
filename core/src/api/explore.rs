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
use optics::{Optic, SiteRankings};
use utoipa::ToSchema;

#[derive(serde::Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ExploreExportOpticParams {
    chosen_sites: Vec<String>,
    similar_sites: Vec<String>,
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
        chosen_sites,
        similar_sites,
    }): extract::Json<ExploreExportOpticParams>,
) -> Result<String, StatusCode> {
    let rules = similar_sites
        .into_iter()
        .chain(chosen_sites.clone().into_iter())
        .map(|site| optics::Rule {
            matches: vec![optics::Matching {
                pattern: vec![
                    optics::PatternPart::Anchor,
                    optics::PatternPart::Raw(site),
                    optics::PatternPart::Anchor,
                ],
                location: optics::MatchLocation::Site,
            }],
            action: optics::Action::Boost(0),
        })
        .collect();

    let optic = Optic {
        site_rankings: SiteRankings {
            liked: chosen_sites,
            ..Default::default()
        },
        rules,
        discard_non_matching: true,
        ..Default::default()
    };

    Ok(optic.to_string())
}
