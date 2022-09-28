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

mod distributed;
mod local;

pub use distributed::*;
pub use local::*;
use serde::Serialize;

use crate::{bangs::BangHit, entity_index::StoredEntity, inverted_index};

pub const NUM_RESULTS_PER_PAGE: usize = 20;

#[derive(Debug, Serialize)]
pub struct WebsitesResult {
    pub spell_corrected_query: Option<String>,
    pub webpages: inverted_index::SearchResult,
    pub entity: Option<StoredEntity>,
    pub search_duration_ms: u128,
}

#[derive(Debug, Serialize)]
pub enum SearchResult {
    Websites(WebsitesResult),
    Bang(BangHit),
}
