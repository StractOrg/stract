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

use std::{collections::HashMap, sync::Arc};

use axum::Extension;

use crate::{
    ranking::site_rankings::SiteRankings,
    search_prettifier::{thousand_sep_number, DisplayedEntity, DisplayedWebpage},
    searcher::{self, PrettifiedSearchResult, SearchQuery},
    webpage::region::{Region, ALL_REGIONS},
};

use super::{
    goggles::{GoggleLink, DEFAULT_GOGGLES},
    HtmlTemplate, State,
};
use askama::Template;
use axum::{
    extract,
    response::{IntoResponse, Redirect},
};

#[derive(Template)]
#[template(path = "search/index.html", escape = "none")]
struct SearchTemplate {
    search_result: Vec<DisplayedWebpage>,
    query: String,
    entity: Option<DisplayedEntity>,
    spell_correction: Option<String>,
    num_matches: String,
    search_duration_sec: String,
    all_regions: Vec<RegionSelection>,
    current_page: usize,
    next_page_url: String,
    prev_page_url: Option<String>,
    default_goggles: Vec<GoggleLink>,
    current_goggle_url: Option<String>,
}

enum RegionSelection {
    Selected(Region),
    Unselected(Region),
}

#[allow(clippy::unused_async)]
#[allow(clippy::match_wild_err_arm)]
pub async fn route(
    extract::Query(params): extract::Query<HashMap<String, String>>,
    Extension(state): Extension<Arc<State>>,
    extract::OriginalUri(uri): extract::OriginalUri,
) -> impl IntoResponse {
    let query = params.get("q").cloned().unwrap_or_default();

    let skip_pages = params.get("p").and_then(|p| p.parse().ok());

    let mut goggle = None;
    let mut current_goggle_url = None;

    if let Some(url) = params.get("goggle") {
        if !url.is_empty() {
            if let Ok(res) = reqwest::get(url).await {
                if let Ok(text) = res.text().await {
                    goggle = Some(text);
                    current_goggle_url = Some(url.to_string());
                }
            }
        }
    }

    let selected_region = params.get("gl").and_then(|gl| {
        if let Ok(region) = Region::from_gl(gl) {
            Some(region)
        } else {
            None
        }
    });

    let site_rankings: Option<SiteRankings> = match params.get("sr") {
        Some(sr) => {
            if !sr.is_empty() {
                if let Ok(site_rankings) = base64::decode(sr) {
                    if let Ok(site_rankings) = std::str::from_utf8(&site_rankings) {
                        serde_json::from_str(site_rankings).ok()
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        }
        None => None,
    };

    match state
        .searcher
        .search_prettified(&SearchQuery {
            original: query.clone(),
            selected_region,
            goggle_program: goggle,
            skip_pages,
            site_rankings,
        })
        .await
    {
        Ok(result) => match result {
            PrettifiedSearchResult::Websites(result) => {
                let entity = result.entity;
                let spell_correction = result.spell_corrected_query;

                let num_matches = thousand_sep_number(result.num_docs);

                let search_duration_sec =
                    format!("{:.2}", result.search_duration_ms as f64 / 1000.0);

                let all_regions = ALL_REGIONS
                    .into_iter()
                    .map(|region| {
                        if let Some(selected_region) = selected_region {
                            if region == selected_region {
                                RegionSelection::Selected(region)
                            } else {
                                RegionSelection::Unselected(region)
                            }
                        } else {
                            RegionSelection::Unselected(region)
                        }
                    })
                    .collect();

                let current_page = skip_pages.unwrap_or(0) + 1;

                let mut next_page_params = params.clone();
                next_page_params.insert("p".to_string(), (skip_pages.unwrap_or(0) + 1).to_string());
                let next_page_url = uri.path().to_string()
                    + "?"
                    + serde_urlencoded::to_string(&next_page_params)
                        .unwrap()
                        .as_str();

                let prev_page_url = if current_page > 1 {
                    let mut prev_page_params = params;
                    prev_page_params
                        .insert("p".to_string(), (skip_pages.unwrap_or(0) - 1).to_string());
                    Some(
                        uri.path().to_string()
                            + "?"
                            + serde_urlencoded::to_string(&prev_page_params)
                                .unwrap()
                                .as_str(),
                    )
                } else {
                    None
                };

                let template = SearchTemplate {
                    search_result: result.webpages,
                    query,
                    entity,
                    spell_correction,
                    num_matches,
                    search_duration_sec,
                    all_regions,
                    current_page,
                    next_page_url,
                    prev_page_url,
                    default_goggles: DEFAULT_GOGGLES.to_vec(),
                    current_goggle_url,
                };

                HtmlTemplate(template).into_response()
            }
            PrettifiedSearchResult::Bang(result) => {
                Redirect::to(&result.redirect_to.full()).into_response()
            }
        },
        Err(searcher::distributed::Error::EmptyQuery) => Redirect::to("/").into_response(),
        Err(_) => panic!("Search failed"), // TODO: show 500 status to user here
    }
}
