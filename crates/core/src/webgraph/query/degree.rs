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
    ampc::dht::ShardId,
    webgraph::{
        schema::{Field, ToId},
        searcher::Searcher,
        NodeID,
    },
};

use super::{
    collector::{FastCountCollector, FastCountValue},
    raw, Query,
};

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct InDegreeQuery {
    node: NodeID,
}

impl Query for InDegreeQuery {
    type Collector = FastCountCollector;
    type TantivyQuery = raw::DummyQuery;
    type IntermediateOutput = FxHashMap<ShardId, u64>;
    type Output = u64;

    fn tantivy_query(&self, _: &Searcher) -> Self::TantivyQuery {
        raw::DummyQuery
    }

    fn collector(&self, shard_id: ShardId) -> Self::Collector {
        FastCountCollector::new(
            ToId.name().to_string(),
            FastCountValue::U64(self.node.as_u64()),
        )
        .with_shard_id(shard_id)
    }

    fn remote_collector(&self) -> Self::Collector {
        FastCountCollector::new(
            ToId.name().to_string(),
            FastCountValue::U64(self.node.as_u64()),
        )
    }

    fn retrieve(
        &self,
        _: &Searcher,
        fruit: <Self::Collector as super::collector::Collector>::Fruit,
    ) -> crate::Result<Self::IntermediateOutput> {
        Ok(fruit)
    }

    fn filter_fruit_shards(
        &self,
        shard_id: crate::ampc::dht::ShardId,
        fruit: <Self::Collector as super::Collector>::Fruit,
    ) -> <Self::Collector as super::Collector>::Fruit {
        fruit
            .into_iter()
            .filter(|(fruit_shard_id, _)| shard_id == *fruit_shard_id)
            .collect()
    }

    fn merge_results(results: Vec<Self::IntermediateOutput>) -> Self::Output {
        let mut map = FxHashMap::default();
        for result in results {
            for (shard_id, count) in result {
                *map.entry(shard_id).or_insert(0) += count;
            }
        }
        map.into_values().sum()
    }
}
