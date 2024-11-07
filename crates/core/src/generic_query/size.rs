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

use rustc_hash::FxHashMap;

use crate::{
    generic_query::{Collector, GenericQuery},
    inverted_index::ShardId,
};

use super::collector::SizeCollector;

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    bincode::Encode,
    bincode::Decode,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct SizeResponse {
    pub pages: u64,
}

impl std::ops::Add for SizeResponse {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            pages: self.pages + rhs.pages,
        }
    }
}

impl std::ops::AddAssign for SizeResponse {
    fn add_assign(&mut self, rhs: Self) {
        self.pages += rhs.pages;
    }
}

#[derive(Debug, Default, Clone, Copy, bincode::Encode, bincode::Decode)]
pub struct SizeQuery;

impl GenericQuery for SizeQuery {
    type Collector = SizeCollector;
    type TantivyQuery = tantivy::query::EmptyQuery;
    type IntermediateOutput = SizeResponse;
    type Output = SizeResponse;

    fn tantivy_query(&self, _: &crate::search_ctx::Ctx) -> Self::TantivyQuery {
        tantivy::query::EmptyQuery
    }

    fn collector(&self, ctx: &crate::search_ctx::Ctx) -> Self::Collector {
        SizeCollector::new().with_shard_id(ctx.shard_id)
    }

    fn remote_collector(&self) -> Self::Collector {
        SizeCollector::new()
    }

    fn filter_fruit_shards(
        &self,
        shard_id: ShardId,
        fruit: <Self::Collector as Collector>::Fruit,
    ) -> <Self::Collector as Collector>::Fruit {
        let mut map = FxHashMap::default();

        for (fruit_shard_id, size_response) in fruit {
            if shard_id == fruit_shard_id {
                map.insert(shard_id, size_response);
            }
        }

        map
    }

    fn retrieve(
        &self,
        ctx: &crate::search_ctx::Ctx,
        fruit: <Self::Collector as Collector>::Fruit,
    ) -> crate::Result<Self::IntermediateOutput> {
        Ok(*fruit.get(&ctx.shard_id).unwrap_or(&SizeResponse::default()))
    }

    fn merge_results(results: Vec<Self::IntermediateOutput>) -> Self::Output {
        let mut res = SizeResponse { pages: 0 };
        for r in results {
            res += r;
        }
        res
    }
}
