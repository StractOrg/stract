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

use crate::{ampc::dht::ShardId, generic_query::size::SizeResponse};

use super::Collector;

pub struct SizeCollector {
    shard_id: Option<ShardId>,
}

impl Default for SizeCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl SizeCollector {
    pub fn new() -> Self {
        Self { shard_id: None }
    }

    pub fn with_shard_id(mut self, shard_id: ShardId) -> Self {
        self.shard_id = Some(shard_id);
        self
    }
}

impl Collector for SizeCollector {
    type Fruit = FxHashMap<ShardId, SizeResponse>;
    type Child = SizeSegmentCollector;

    fn for_segment(
        &self,
        _: tantivy::SegmentOrdinal,
        segment: &tantivy::SegmentReader,
    ) -> crate::Result<Self::Child> {
        Ok(SizeSegmentCollector::new(self.shard_id.unwrap(), segment))
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as tantivy::collector::SegmentCollector>::Fruit>,
    ) -> crate::Result<Self::Fruit> {
        let mut map = FxHashMap::default();

        for fruit in segment_fruits {
            for (shard_id, size_response) in fruit {
                map.entry(shard_id).or_insert_with(|| size_response);
            }
        }

        Ok(map)
    }

    fn collect_segment(
        &self,
        _: &dyn tantivy::query::Weight,
        segment_ord: u32,
        reader: &tantivy::SegmentReader,
    ) -> crate::Result<<Self::Child as tantivy::collector::SegmentCollector>::Fruit> {
        let child = self.for_segment(segment_ord, reader)?;
        Ok(child.fruit())
    }
}

pub struct SizeSegmentCollector {
    shard_id: ShardId,
    num_pages: u64,
}

impl SizeSegmentCollector {
    pub fn new(shard_id: ShardId, segment: &tantivy::SegmentReader) -> Self {
        Self {
            shard_id,
            num_pages: segment.num_docs() as u64,
        }
    }

    pub fn fruit(self) -> <Self as tantivy::collector::SegmentCollector>::Fruit {
        let mut map = FxHashMap::default();
        map.insert(
            self.shard_id,
            SizeResponse {
                pages: self.num_pages,
            },
        );
        map
    }
}

impl tantivy::collector::SegmentCollector for SizeSegmentCollector {
    type Fruit = FxHashMap<ShardId, SizeResponse>;

    fn collect(&mut self, _: tantivy::DocId, _: tantivy::Score) {
        unimplemented!()
    }

    fn harvest(self) -> Self::Fruit {
        unimplemented!()
    }
}
