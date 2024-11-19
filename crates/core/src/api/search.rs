// Stract is an open source web search engine.
// Copyright (C) 2024 Stract ApS
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

use crate::{
    config::defaults,
    enum_map::EnumMap,
    ranking::{SignalCoefficients, SignalEnum, SignalEnumDiscriminants},
    search_prettifier::{DisplayedSidebar, HighlightedSpellCorrection},
    widgets::Widget,
};
use http::StatusCode;
use optics::{HostRankings, Optic};
use std::{collections::HashMap, sync::Arc};
use utoipa::ToSchema;

use axum::Json;
use axum_macros::debug_handler;

use crate::{
    bangs::BangHit,
    searcher::{self, SearchQuery, SearchResult, WebsitesResult},
    webpage::region::Region,
};

use super::State;

use axum::{extract, response::IntoResponse};

#[derive(
    Clone,
    Copy,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    ToSchema,
)]
#[serde(tag = "_type", content = "value", rename_all = "camelCase")]
pub enum ReturnBody {
    All,
    Truncated(usize),
}

#[derive(
    Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, ToSchema,
)]
#[serde(rename_all = "camelCase")]
#[schema(title = "SearchQuery", example = json!({"query": "hello world"}))]
pub struct ApiSearchQuery {
    /// The search query string
    pub query: String,

    /// The page number to return
    pub page: Option<usize>,

    /// The number of results to return per page (max: 100)
    pub num_results: Option<usize>,

    /// Prioritize results for a specific geographic region
    pub selected_region: Option<Region>,

    /// Apply an `optic` to the query
    pub optic: Option<String>,

    /// Custom host ranking preferences
    pub host_rankings: Option<HostRankings>,

    /// Enable/disable nsfw filtering
    pub safe_search: Option<bool>,

    /// Custom weights for ranking signals
    pub signal_coefficients: Option<HashMap<SignalEnumDiscriminants, f64>>,

    /// Include ranking signal scores in results
    #[serde(default = "defaults::SearchQuery::return_ranking_signals")]
    pub return_ranking_signals: bool,

    /// Return flattened result format
    #[serde(default = "defaults::SearchQuery::flatten_response")]
    pub flatten_response: bool,

    /// Get exact vs estimated result count
    #[serde(default = "defaults::SearchQuery::count_results_exact")]
    pub count_results_exact: bool,

    /// Include structured schema.org data in results
    #[serde(default = "defaults::SearchQuery::return_structured_data")]
    pub return_structured_data: bool,

    /// Control whether or not the page content is returned
    #[cfg(feature = "return_body")]
    pub return_body: Option<ReturnBody>,
}

impl TryFrom<ApiSearchQuery> for SearchQuery {
    type Error = anyhow::Error;

    fn try_from(api: ApiSearchQuery) -> Result<Self, Self::Error> {
        let optic = if let Some(optic) = &api.optic {
            Some(Optic::parse(optic)?)
        } else {
            None
        };

        let signal_coefficients: Option<SignalCoefficients> =
            api.signal_coefficients.map(|coefficients| {
                coefficients
                    .into_iter()
                    .map(|(signal, coefficient)| (signal.into(), coefficient))
                    .collect::<EnumMap<SignalEnum, f64>>()
                    .into()
            });

        let default = SearchQuery::default();

        Ok(SearchQuery {
            query: api.query,
            page: api.page.unwrap_or(default.page),
            num_results: api.num_results.unwrap_or(default.num_results),
            selected_region: api.selected_region,
            optic,
            host_rankings: api.host_rankings,
            return_ranking_signals: api.return_ranking_signals,
            safe_search: api.safe_search.unwrap_or(default.safe_search),
            count_results_exact: api.count_results_exact,
            signal_coefficients: signal_coefficients.unwrap_or(default.signal_coefficients),
            #[cfg(feature = "return_body")]
            return_body: api.return_body,
            #[cfg(not(feature = "return_body"))]
            return_body: None,
            return_structured_data: api.return_structured_data,
        })
    }
}

#[derive(
    Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, ToSchema,
)]
#[serde(tag = "_type", rename_all = "camelCase")]
pub enum ApiSearchResult {
    Websites(WebsitesResult),
    Bang(Box<BangHit>),
}

impl From<SearchResult> for ApiSearchResult {
    fn from(result: SearchResult) -> Self {
        match result {
            SearchResult::Websites(result) => ApiSearchResult::Websites(result),
            SearchResult::Bang(result) => ApiSearchResult::Bang(result),
        }
    }
}

/// Web Search
///
/// The main search endpoint that powers Stract's web search functionality. It performs a full-text search
/// across all pages in the index and returns the most relevant results.
///
/// The endpoint supports the [optic syntax](https://github.com/StractOrg/sample-optics/blob/main/quickstart.optic)
/// that can be used to customize the search results and perform advanced filtering.
/// If the query matches the bang prefix `!`, the result will be a redirect to the bang target.
/// For example, `!w cats` redirects to Wikipedia's page on cats.
///
/// Results are paginated and can be flattened into a simpler format if desired. The response includes
/// rich metadata like snippets, titles, and ranking signal scores (when enabled).
#[debug_handler]
#[utoipa::path(
    post,
    path = "/beta/api/search",
    request_body(content = ApiSearchQuery),
    responses(
        (status = 200, description = "Search results", body = ApiSearchResult),
    )
)]
pub async fn search(
    extract::State(state): extract::State<Arc<State>>,
    extract::Json(query): extract::Json<ApiSearchQuery>,
) -> Result<impl IntoResponse, StatusCode> {
    tracing::debug!(?query);
    let flatten_result = query.flatten_response;
    let query = SearchQuery::try_from(query);

    if let Err(err) = query {
        tracing::error!("{:?}", err);
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut query = query.unwrap();

    query.num_results = query.num_results.min(100);

    match state.searcher.search(&query).await {
        Ok(result) => {
            if flatten_result {
                Ok(Json(ApiSearchResult::from(result)).into_response())
            } else {
                Ok(Json(result).into_response())
            }
        }

        Err(err) => match err.downcast_ref() {
            Some(searcher::distributed::Error::EmptyQuery) => {
                Ok(searcher::distributed::Error::EmptyQuery
                    .to_string()
                    .into_response())
            }
            _ => {
                tracing::error!("{:?}", err);
                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        },
    }
}

#[derive(
    Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, ToSchema,
)]
#[schema(title = "WidgetQuery", example = json!({"query": "2+2"}))]
pub struct WidgetQuery {
    /// The query string to search for
    pub query: String,
}

/// Widgets
///
/// The widget endpoint returns a widget that matches the query. Widgets are special UI components
/// that provide direct answers or interactive functionality for certain types of queries at the top of the search results.
/// A widget could for example be a calculator for math expressions, or a thesaurus for word definitions.
///
/// The endpoint returns an empty response if no widget matches the given query. When a widget does match,
/// it returns a structured response containing the widget type and its specific data needed to render it.
#[debug_handler]
#[utoipa::path(
    post,
    path = "/beta/api/search/widget",
    request_body(content = WidgetQuery),
    responses(
        (status = 200, description = "The resulting widget if one matches the query", body = Option<Widget>),
    )
)]
pub async fn widget(
    extract::State(state): extract::State<Arc<State>>,
    extract::Json(req): extract::Json<WidgetQuery>,
) -> impl IntoResponse {
    Json(state.searcher.widget(&req.query).await)
}

#[derive(
    Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, ToSchema,
)]
#[schema(title = "SidebarQuery", example = json!({"query": "Aristotle"}))]
pub struct SidebarQuery {
    /// The query string to search for
    pub query: String,
}

/// Sidebar
///
/// The sidebar endpoint returns a sidebar that matches the query. The sidebar is a UI component
/// that provides additional information or navigation options related to the query at the top of the search results.
///
/// The endpoint returns an empty response if no relevant sidebar content is found for the given query.
/// When content is found, it returns a structured response containing the sidebar data needed
/// to render the component.
///
/// For example, a query for "Aristotle" might return a sidebar with biographical information,
/// key philosophical concepts, and links to related ancient Greek philosophers.
#[debug_handler]
#[utoipa::path(
    post,
    path = "/beta/api/search/sidebar",
    request_body(content = SidebarQuery),
    responses(
        (status = 200, description = "The sidebar if one matches the query", body = Option<DisplayedSidebar>),
    )
)]
pub async fn sidebar(
    extract::State(state): extract::State<Arc<State>>,
    extract::Json(req): extract::Json<SidebarQuery>,
) -> impl IntoResponse {
    Json(state.searcher.sidebar(&req.query).await)
}

#[derive(
    Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, ToSchema,
)]
#[schema(title = "SpellcheckQuery", example = json!({"query": "hwllo eorld"}))]
pub struct SpellcheckQuery {
    /// The query string to possibly correct
    pub query: String,
}

/// Spellcheck
///
/// The spellcheck endpoint checks a query string for spelling errors and returns a corrected version if needed.
/// The correction is returned with highlighting to show what was changed.
///
/// For example, given the query "hwllo eorld", it might return "hello world" with "hello" and "world"
/// highlighted to show the corrections.
///
/// The corrections aim to preserve the user's intent while fixing obvious mistakes.
///
/// The correction dictionary has been trained on common words and phrases on the web, so it might make mistakes itself.
#[debug_handler]
#[utoipa::path(
    post,
    path = "/beta/api/search/spellcheck",
    request_body(content = SpellcheckQuery),
    responses(
        (status = 200, description = "The corrected string or an empty response if there is no correction to be made.", body = Option<HighlightedSpellCorrection>),
    )
)]
pub async fn spellcheck(
    extract::State(state): extract::State<Arc<State>>,
    extract::Json(req): extract::Json<SpellcheckQuery>,
) -> impl IntoResponse {
    Json(state.searcher.spell_check(&req.query))
}

#[derive(
    Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, ToSchema,
)]
#[serde(rename_all = "camelCase")]
pub struct EntityImageParams {
    pub image_id: String,
    pub max_width: Option<u64>,
    pub max_height: Option<u64>,
}

#[utoipa::path(
    post,
    path = "/beta/api/entity_image",
    request_body(content = ApiSearchQuery),
    responses(
        (status = 200, description = "Search results", body = ApiSearchResult),
    )
)]
pub async fn entity_image(
    extract::Query(query): extract::Query<EntityImageParams>,
    extract::State(state): extract::State<Arc<State>>,
) -> Result<impl IntoResponse, StatusCode> {
    match state
        .searcher
        .get_entity_image(&query.image_id, query.max_height, query.max_width)
        .await
    {
        Ok(Some(result)) => {
            let bytes = result.as_raw_bytes();

            Ok((
                ([(axum::http::header::CONTENT_TYPE, "image/webp")]),
                axum::response::AppendHeaders([(axum::http::header::CONTENT_TYPE, "image/webp")]),
                bytes,
            ))
        }
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(err) => {
            tracing::error!("{:?}", err);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
