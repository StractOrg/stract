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

pub mod api;
pub mod distributed;
pub mod live;
pub mod local;

pub use distributed::*;
pub use local::*;
use optics::{HostRankings, Optic};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    bangs::BangHit,
    config::defaults,
    entity_index::EntityMatch,
    ranking::pipeline::RankingWebsite,
    search_prettifier::{
        DisplayedAnswer, DisplayedSidebar, DisplayedWebpage, HighlightedSpellCorrection,
    },
    webpage::region::Region,
    widgets::Widget,
};

pub const NUM_RESULTS_PER_PAGE: usize = 20;

#[derive(Debug, Serialize, Deserialize)]
pub enum SearchResult {
    Websites(WebsitesResult),
    Bang(Box<BangHit>),
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WebsitesResult {
    pub spell_corrected_query: Option<HighlightedSpellCorrection>,
    pub webpages: Vec<DisplayedWebpage>,
    pub num_hits: Option<usize>,
    pub sidebar: Option<DisplayedSidebar>,
    pub widget: Option<Widget>,
    pub direct_answer: Option<DisplayedAnswer>,
    pub discussions: Option<Vec<DisplayedWebpage>>,
    pub search_duration_ms: u128,
    pub has_more_results: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SearchQuery {
    pub query: String,
    pub page: usize,
    pub num_results: usize,
    pub selected_region: Option<Region>,
    pub optic: Option<Optic>,
    pub host_rankings: Option<HostRankings>,
    pub return_ranking_signals: bool,
    pub safe_search: bool,
    pub fetch_discussions: bool,
    pub count_results: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitialWebsiteResult {
    pub num_websites: Option<usize>,
    pub websites: Vec<RankingWebsite>,
    pub has_more: bool,
    pub entity_sidebar: Option<EntityMatch>,
}

impl Default for SearchQuery {
    fn default() -> Self {
        // This does not use `..Default::default()` as there should be
        // an explicit compile error when new fields are added to the `SearchQuery` struct
        // to ensure the developer considers what the default should be.
        Self {
            query: Default::default(),
            page: Default::default(),
            num_results: NUM_RESULTS_PER_PAGE,
            selected_region: Default::default(),
            optic: Default::default(),
            host_rankings: Default::default(),
            return_ranking_signals: defaults::SearchQuery::return_ranking_signals(),
            safe_search: defaults::SearchQuery::safe_search(),
            fetch_discussions: defaults::SearchQuery::fetch_discussions(),
            count_results: defaults::SearchQuery::count_results(),
        }
    }
}

impl SearchQuery {
    pub fn is_empty(&self) -> bool {
        self.query.is_empty()
    }
}
