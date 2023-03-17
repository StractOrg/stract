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

use http::{StatusCode, Uri};
use optics::SiteRankings;
use std::{collections::HashMap, sync::Arc};

use axum::Json;
use axum_macros::debug_handler;

use crate::{
    search_prettifier::{
        thousand_sep_number, CodeOrText, DisplayedAnswer, DisplayedWebpage,
        HighlightedSpellCorrection, Sidebar, Snippet,
    },
    searcher::{self, SearchQuery, SearchResult, WebsitesResult, NUM_RESULTS_PER_PAGE},
    webpage::region::{Region, ALL_REGIONS},
    widgets::Widget,
    Error,
};

use super::{
    optics::{OpticLink, DEFAULT_OPTICS},
    HtmlTemplate, State,
};
use askama::Template;
use axum::{
    extract,
    response::{IntoResponse, Redirect},
};

#[derive(Template)]
#[template(path = "search/index.html")]
struct SearchTemplate {
    search_result: Vec<DisplayedWebpage>,
    discussions: Option<Vec<DisplayedWebpage>>,
    query: String,
    sidebar: Option<Sidebar>,
    widget: Option<Widget>,
    direct_answer: Option<DisplayedAnswer>,
    spell_correction: Option<HighlightedSpellCorrection>,
    num_matches: String,
    search_duration_sec: String,
    all_regions: Vec<RegionSelection>,
    current_page: usize,
    next_page_url: String,
    prev_page_url: Option<String>,
    default_optics: Vec<OpticLink>,
    has_more_results: bool,
    has_code: bool,
}

enum RegionSelection {
    Selected(Region),
    Unselected(Region),
}

fn extract_site_rankings(params: &HashMap<String, String>) -> Option<SiteRankings> {
    match params.get("sr") {
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
    }
}

#[allow(clippy::unused_async)]
#[allow(clippy::match_wild_err_arm)]
#[debug_handler]
pub async fn route(
    extract::Query(params): extract::Query<HashMap<String, String>>,
    extract::State(state): extract::State<Arc<State>>,
    extract::OriginalUri(uri): extract::OriginalUri,
) -> Result<impl IntoResponse, StatusCode> {
    let query = params.get("q").cloned().unwrap_or_default();

    let skip_pages: usize = params
        .get("p")
        .and_then(|p| p.parse().ok())
        .unwrap_or_default();

    let mut optic = None;

    if let Some(url) = params.get("optic") {
        if !url.is_empty() {
            if let Ok(res) = reqwest::get(url).await {
                if let Ok(text) = res.text().await {
                    optic = Some(text);
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

    let site_rankings = extract_site_rankings(&params);

    match state
        .searcher
        .search(&SearchQuery {
            query: query.clone(),
            selected_region,
            optic_program: optic,
            page: skip_pages,
            site_rankings,
            num_results: NUM_RESULTS_PER_PAGE,
            return_ranking_signals: false,
        })
        .await
    {
        Ok(result) => match result {
            SearchResult::Websites(result) => {
                let num_matches = thousand_sep_number(result.num_hits);

                let search_duration_sec =
                    format!("{:.2}", result.search_duration_ms as f64 / 1000.0);

                let all_regions = generate_regions(selected_region);
                let next_page_url = next_page_url(&uri, params.clone(), skip_pages);
                let prev_page_url = prev_page_url(&uri, params, skip_pages);
                let has_code = has_code(&result);

                let current_page = skip_pages + 1;

                let template = SearchTemplate {
                    search_result: result.webpages,
                    discussions: result.discussions,
                    query,
                    sidebar: result.sidebar,
                    widget: result.widget,
                    direct_answer: result.direct_answer,
                    spell_correction: result.spell_corrected_query,
                    num_matches,
                    search_duration_sec,
                    all_regions,
                    current_page,
                    next_page_url,
                    prev_page_url,
                    default_optics: DEFAULT_OPTICS.to_vec(),
                    has_more_results: result.has_more_results,
                    has_code,
                };

                Ok(HtmlTemplate(template).into_response())
            }
            SearchResult::Bang(result) => {
                Ok(Redirect::to(&result.redirect_to.full()).into_response())
            }
        },
        Err(Error::DistributedSearcher(searcher::distributed::Error::EmptyQuery)) => {
            Ok(Redirect::to("/").into_response())
        }
        Err(err) => {
            tracing::error!("{:?}", err);

            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

fn has_code(result: &WebsitesResult) -> bool {
    if let Some(Sidebar::StackOverflow { .. }) = result.sidebar.as_ref() {
        return true;
    }

    result
        .webpages
        .iter()
        .any(|page| matches!(page.snippet, Snippet::StackOverflowQA { .. }))
}

fn generate_regions(selected_region: Option<Region>) -> Vec<RegionSelection> {
    ALL_REGIONS
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
        .collect()
}

fn prev_page_url(uri: &Uri, params: HashMap<String, String>, skip_pages: usize) -> Option<String> {
    if skip_pages > 0 {
        let mut prev_page_params = params;
        prev_page_params.insert("p".to_string(), (skip_pages - 1).to_string());
        Some(
            uri.path().to_string()
                + "?"
                + serde_urlencoded::to_string(&prev_page_params)
                    .unwrap()
                    .as_str(),
        )
    } else {
        None
    }
}

fn next_page_url(uri: &Uri, params: HashMap<String, String>, skip_pages: usize) -> String {
    let mut next_page_params = params;
    next_page_params.insert("p".to_string(), (skip_pages + 1).to_string());
    let next_page_url = uri.path().to_string()
        + "?"
        + serde_urlencoded::to_string(&next_page_params)
            .unwrap()
            .as_str();

    next_page_url
}

#[allow(clippy::unused_async)]
#[allow(clippy::match_wild_err_arm)]
#[debug_handler]
pub async fn api(
    extract::State(state): extract::State<Arc<State>>,
    extract::Json(mut query): extract::Json<SearchQuery>,
) -> impl IntoResponse {
    query.num_results = query.num_results.min(50 * NUM_RESULTS_PER_PAGE);

    match state.searcher.search(&query).await {
        Ok(result) => match result {
            SearchResult::Websites(result) => Json(result).into_response(),
            SearchResult::Bang(result) => Redirect::to(&result.redirect_to.full()).into_response(),
        },
        Err(Error::DistributedSearcher(searcher::distributed::Error::EmptyQuery)) => {
            Redirect::to("/").into_response()
        }
        Err(_) => panic!("Search failed"), // TODO: show 500 status to user here
    }
}
