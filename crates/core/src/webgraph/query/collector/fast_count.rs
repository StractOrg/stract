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

use crate::ampc::dht::ShardId;

use super::Collector;

pub enum FastCountValue {
    U128(u128),
}

impl FastCountValue {
    fn as_term(&self, field: tantivy::schema::Field) -> tantivy::schema::Term {
        match self {
            FastCountValue::U128(value) => tantivy::schema::Term::from_field_u128(field, *value),
        }
    }
}

pub struct FastCountCollector {
    field_name: String,
    value: FastCountValue,
    shard_id: Option<ShardId>,
}

impl FastCountCollector {
    pub fn new(field_name: String, value: FastCountValue) -> Self {
        Self {
            field_name,
            value,
            shard_id: None,
        }
    }

    pub fn with_shard_id(mut self, shard_id: ShardId) -> Self {
        self.shard_id = Some(shard_id);
        self
    }
}

impl Collector for FastCountCollector {
    type Fruit = FxHashMap<ShardId, u64>;

    type Child = FastSegmentCountCollector;

    fn for_segment(
        &self,
        _: tantivy::SegmentOrdinal,
        segment: &tantivy::SegmentReader,
    ) -> crate::Result<Self::Child> {
        let field = segment.schema().get_field(&self.field_name)?;
        let term = self.value.as_term(field);

        let num_docs = segment
            .inverted_index(field)?
            .read_postings(&term, tantivy::schema::IndexRecordOption::Basic)?
            .map(|postings| postings.doc_freq() as u64)
            .unwrap_or(0);

        Ok(FastSegmentCountCollector {
            shard_id: self.shard_id.unwrap(),
            count: num_docs,
        })
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as tantivy::collector::SegmentCollector>::Fruit>,
    ) -> crate::Result<Self::Fruit> {
        let mut map = FxHashMap::default();
        for result in segment_fruits {
            for (shard_id, count) in result {
                *map.entry(shard_id).or_insert(0) += count;
            }
        }
        Ok(map)
    }
}

pub struct FastSegmentCountCollector {
    shard_id: ShardId,
    count: u64,
}

impl tantivy::collector::SegmentCollector for FastSegmentCountCollector {
    type Fruit = FxHashMap<ShardId, u64>;

    fn collect(&mut self, _: tantivy::DocId, _: tantivy::Score) {}

    fn harvest(self) -> Self::Fruit {
        FxHashMap::from_iter(vec![(self.shard_id, self.count)])
    }
}
