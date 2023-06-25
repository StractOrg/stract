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

use std::{collections::HashMap, sync::Arc};

use askama::Template;
use axum::{extract, response::IntoResponse};
use http::StatusCode;
use optics::{Optic, SiteRankings};

use super::{HtmlTemplate, State};

#[allow(clippy::unused_async)]
pub async fn route(
    extract::State(state): extract::State<Arc<State>>,
    extract::Query(params): extract::Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let template = ExploreTemplate {
        query_url_part: serde_urlencoded::to_string(params).unwrap(),
        with_alice: state.config.with_alice,
    };
    HtmlTemplate(template)
}

#[derive(Template)]
#[template(path = "explore/index.html")]
struct ExploreTemplate {
    pub query_url_part: String,
    with_alice: Option<bool>,
}

#[derive(serde::Deserialize)]
pub struct ExportParams {
    pub data: String,
}

#[derive(serde::Deserialize, Debug)]
struct Data {
    chosen_sites: Vec<String>,
    similar_sites: Vec<String>,
}

#[allow(clippy::unused_async)]
pub async fn export(
    extract::Query(ExportParams { data }): extract::Query<ExportParams>,
) -> Result<impl IntoResponse, StatusCode> {
    let data = data.replace(' ', "+");
    match lz_str::decompress_from_base64(&data) {
        Some(bytes) => match String::from_utf16(&bytes) {
            Ok(s) => match serde_json::from_str::<Data>(&s) {
                Ok(val) => {
                    let rules = val
                        .similar_sites
                        .into_iter()
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
                            liked: val.chosen_sites,
                            ..Default::default()
                        },
                        rules,
                        discard_non_matching: true,
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
