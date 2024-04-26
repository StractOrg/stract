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

use utoipa::ToSchema;

use crate::{
    api::search::ReturnBody,
    bangs::BangHit,
    config::defaults,
    ranking::{pipeline::RecallRankingWebpage, SignalCoefficient},
    search_prettifier::DisplayedWebpage,
    webpage::region::Region,
};

pub const NUM_RESULTS_PER_PAGE: usize = 20;

#[derive(Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub enum SearchResult {
    Websites(WebsitesResult),
    Bang(Box<BangHit>),
}

#[derive(
    Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, ToSchema,
)]
#[serde(rename_all = "camelCase")]
pub struct WebsitesResult {
    pub webpages: Vec<DisplayedWebpage>,
    pub num_hits: Option<usize>,
    pub search_duration_ms: u128,
    pub has_more_results: bool,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Clone)]
pub struct SearchQuery {
    pub query: String,
    pub page: usize,
    pub num_results: usize,
    pub selected_region: Option<Region>,
    pub optic: Option<Optic>,
    pub host_rankings: Option<HostRankings>,
    pub return_ranking_signals: bool,
    pub safe_search: bool,
    pub count_results: bool,
    pub return_body: Option<ReturnBody>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct InitialWebsiteResult {
    pub num_websites: Option<usize>,
    pub websites: Vec<RecallRankingWebpage>,
    pub has_more: bool,
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
            count_results: defaults::SearchQuery::count_results(),
            return_body: None,
        }
    }
}

impl SearchQuery {
    pub fn is_empty(&self) -> bool {
        self.query.is_empty()
    }

    pub fn signal_coefficients(&self) -> SignalCoefficient {
        let mut signal_coefficients = SignalCoefficient::default();

        if let Some(optic) = &self.optic {
            signal_coefficients.merge_overwrite(SignalCoefficient::from_optic(optic));
        }

        signal_coefficients
    }
}
