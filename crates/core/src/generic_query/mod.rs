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

//! # Main flow
//! ```md
//! `coordinator`  <------>  `searcher`
//! -----------------------------------
//! send query to searcher
//!                        search index
//!                        collect fruits
//!                        send fruits to coordinator
//! merge fruits
//! filter fruits
//!     for each shard
//! send fruits to searchers
//!                        construct intermediate output
//!                           from fruits
//!                        send intermediate output to coordinator
//! merge intermediate outputs
//! return final output
//! ---------------------------------------------------
//! ```

use crate::{inverted_index::ShardId, search_ctx, Result};

pub mod top_key_phrases;
pub use top_key_phrases::TopKeyPhrasesQuery;

pub mod size;
pub use size::SizeQuery;

pub mod get_webpage;
pub use get_webpage::GetWebpageQuery;

pub mod get_homepage;
pub use get_homepage::GetHomepageQuery;

pub mod get_site_urls;
pub use get_site_urls::GetSiteUrlsQuery;

pub mod collector;
pub use collector::Collector;

/// A generic query that can be executed on a searcher
/// against an index.
pub trait GenericQuery: Send + Sync + bincode::Encode + bincode::Decode + Clone {
    type Collector: Collector;
    type TantivyQuery: tantivy::query::Query;
    type IntermediateOutput: Send + Sync;
    type Output: Send + Sync;

    fn tantivy_query(&self, ctx: &search_ctx::Ctx) -> Self::TantivyQuery;
    fn collector(&self, ctx: &search_ctx::Ctx) -> Self::Collector;
    fn coordinator_collector(&self) -> Self::Collector;

    fn filter_fruit_shards(
        &self,
        shard_id: ShardId,
        fruit: <Self::Collector as Collector>::Fruit,
    ) -> <Self::Collector as Collector>::Fruit;

    fn retrieve(
        &self,
        ctx: &search_ctx::Ctx,
        fruit: <Self::Collector as Collector>::Fruit,
    ) -> Result<Self::IntermediateOutput>;

    fn merge_results(results: Vec<Self::IntermediateOutput>) -> Self::Output;
}
