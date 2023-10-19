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

use crate::config::defaults;
use http::StatusCode;
use optics::{Optic, SiteRankings};
use std::sync::Arc;
use utoipa::ToSchema;
use webpage::region::Region;

use axum::Json;
use axum_macros::debug_handler;

use crate::{
    bangs::BangHit,
    searcher::{self, SearchQuery, SearchResult, WebsitesResult},
};

use super::State;

use axum::{extract, response::IntoResponse};

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
pub struct SearchParams {
    /// Query
    pub q: Option<String>,
    /// Page
    pub p: Option<usize>,
    /// Language
    pub gl: Option<String>,
    pub optic: Option<String>,
    /// Site rankings
    pub sr: Option<String>,
    /// Safe search
    pub ss: Option<bool>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(title = "SearchQuery", example = json!({"query": "hello world"}))]
pub struct ApiSearchQuery {
    pub query: String,
    pub page: Option<usize>,
    pub num_results: Option<usize>,
    pub selected_region: Option<Region>,
    pub optic: Option<String>,
    pub site_rankings: Option<SiteRankings>,
    pub safe_search: Option<bool>,

    #[serde(default = "defaults::SearchQuery::return_ranking_signals")]
    pub return_ranking_signals: bool,

    #[serde(default = "defaults::SearchQuery::flatten_response")]
    pub flatten_response: bool,

    #[serde(default = "defaults::SearchQuery::fetch_discussions")]
    pub fetch_discussions: bool,

    #[serde(default = "defaults::SearchQuery::count_results")]
    pub count_results: bool,
}

impl TryFrom<ApiSearchQuery> for SearchQuery {
    type Error = anyhow::Error;

    fn try_from(api: ApiSearchQuery) -> Result<Self, Self::Error> {
        let optic = if let Some(optic) = &api.optic {
            Some(Optic::parse(optic)?)
        } else {
            None
        };

        let default = SearchQuery::default();

        Ok(SearchQuery {
            query: api.query,
            page: api.page.unwrap_or(default.page),
            num_results: api.num_results.unwrap_or(default.num_results),
            selected_region: api.selected_region,
            optic,
            site_rankings: api.site_rankings,
            return_ranking_signals: api.return_ranking_signals,
            safe_search: api.safe_search.unwrap_or(default.safe_search),
            fetch_discussions: api.fetch_discussions,
            count_results: api.count_results,
        })
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "camelCase")]
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

#[allow(clippy::unused_async)]
#[allow(clippy::match_wild_err_arm)]
#[debug_handler]
#[utoipa::path(
    post,
    path = "/beta/api/search",
    request_body(content = ApiSearchQuery),
    responses(
        (status = 200, description = "Search results", body = ApiSearchResult),
    )
)]
pub async fn api(
    extract::State(state): extract::State<Arc<State>>,
    extract::Json(query): extract::Json<ApiSearchQuery>,
) -> Result<impl IntoResponse, StatusCode> {
    let flatten_result = query.flatten_response;
    let query = SearchQuery::try_from(query);

    if let Err(err) = query {
        tracing::error!("{:?}", err);
        return Ok(err.to_string().into_response());
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

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct EntityImageParams {
    pub image_id: String,
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
    match state.searcher.get_entity_image(&query.image_id).await {
        Ok(Some(result)) => {
            let bytes = result.as_raw_bytes();

            Ok((
                ([(axum::http::header::CONTENT_TYPE, "image/png")]),
                axum::response::AppendHeaders([(axum::http::header::CONTENT_TYPE, "image/png")]),
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
