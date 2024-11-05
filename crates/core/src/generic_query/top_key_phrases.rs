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

use std::collections::HashMap;

use itertools::Itertools;

use crate::{ampc::dht::ShardId, inverted_index::KeyPhrase};

use super::{collector::TopKeyPhrasesCollector, GenericQuery};

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct TopKeyPhrasesQuery {
    pub top_n: usize,
}

impl TopKeyPhrasesQuery {
    pub fn new(top_n: usize) -> Self {
        Self { top_n }
    }
}

impl GenericQuery for TopKeyPhrasesQuery {
    type Collector = TopKeyPhrasesCollector;
    type TantivyQuery = tantivy::query::EmptyQuery;
    type IntermediateOutput = Vec<(ShardId, KeyPhrase)>;
    type Output = Vec<KeyPhrase>;

    fn tantivy_query(&self, _: &crate::search_ctx::Ctx) -> Self::TantivyQuery {
        tantivy::query::EmptyQuery
    }

    fn collector(&self, ctx: &crate::search_ctx::Ctx) -> Self::Collector {
        TopKeyPhrasesCollector::new(self.top_n).with_shard_id(ctx.shard_id)
    }

    fn remote_collector(&self) -> Self::Collector {
        TopKeyPhrasesCollector::new(self.top_n)
    }

    fn filter_fruit_shards(
        &self,
        shard_id: crate::ampc::dht::ShardId,
        fruit: <Self::Collector as super::Collector>::Fruit,
    ) -> <Self::Collector as super::Collector>::Fruit {
        fruit
            .into_iter()
            .filter(|(id, _)| id == &shard_id)
            .collect()
    }

    fn retrieve(
        &self,
        _: &crate::search_ctx::Ctx,
        fruit: <Self::Collector as super::Collector>::Fruit,
    ) -> crate::Result<Self::IntermediateOutput> {
        Ok(fruit)
    }

    fn merge_results(results: Vec<Self::IntermediateOutput>) -> Self::Output {
        let mut phrases = HashMap::new();

        for (_, phrase) in results.into_iter().flatten() {
            *phrases.entry(phrase.text().to_string()).or_default() += phrase.score();
        }

        phrases
            .into_iter()
            .map(|(phrase, score)| KeyPhrase::new(phrase, score))
            .sorted_by(|a, b| b.score().total_cmp(&a.score()))
            .collect()
    }
}
