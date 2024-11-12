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

use tantivy::{collector::SegmentCollector, DocId, SegmentOrdinal};

use super::Collector;
use crate::{
    ampc::dht::ShardId,
    webgraph::{
        doc_address::DocAddress,
        query::{ColumnFieldFilter, SegmentColumnFieldFilter},
        warmed_column_fields::WarmedColumnFields,
    },
};

pub struct FirstDocCollector {
    shard_id: Option<ShardId>,
    column_fields: Option<WarmedColumnFields>,
    filter: Option<Box<dyn ColumnFieldFilter>>,
}

impl FirstDocCollector {
    #[must_use]
    pub fn with_shard_id(shard_id: ShardId) -> Self {
        Self {
            shard_id: Some(shard_id),
            filter: None,
            column_fields: None,
        }
    }

    #[must_use]
    pub fn without_shard_id() -> Self {
        Self {
            shard_id: None,
            filter: None,
            column_fields: None,
        }
    }

    #[must_use]
    pub fn with_column_fields(self, warmed_column_fields: WarmedColumnFields) -> Self {
        Self {
            column_fields: Some(warmed_column_fields),
            ..self
        }
    }

    #[must_use]
    pub fn with_filter(self, filter: Box<dyn ColumnFieldFilter>) -> Self {
        Self {
            filter: Some(filter),
            ..self
        }
    }
}

impl Collector for FirstDocCollector {
    type Fruit = Option<DocAddress>;
    type Child = FirstDocSegmentCollector;

    fn for_segment(
        &self,
        segment_id: SegmentOrdinal,
        segment: &tantivy::SegmentReader,
    ) -> crate::Result<Self::Child> {
        let column_fields = self
            .column_fields
            .as_ref()
            .expect("Column fields are required to perform search")
            .segment(&segment.segment_id());

        let filter = self.filter.as_ref().map(|f| f.for_segment(column_fields));

        Ok(FirstDocSegmentCollector::new(
            segment_id,
            filter,
            self.shard_id.unwrap(),
        ))
    }

    fn merge_fruits(
        &self,
        fruits: Vec<<Self::Child as tantivy::collector::SegmentCollector>::Fruit>,
    ) -> crate::Result<Self::Fruit> {
        Ok(fruits.into_iter().flatten().next())
    }
}

pub struct FirstDocSegmentCollector {
    first_doc: Option<DocAddress>,
    segment_id: SegmentOrdinal,
    filter: Option<Box<dyn SegmentColumnFieldFilter>>,
    shard_id: ShardId,
}

impl FirstDocSegmentCollector {
    pub fn new(
        segment_id: SegmentOrdinal,
        filter: Option<Box<dyn SegmentColumnFieldFilter>>,
        shard_id: ShardId,
    ) -> Self {
        Self {
            first_doc: None,
            segment_id,
            filter,
            shard_id,
        }
    }
}

impl SegmentCollector for FirstDocSegmentCollector {
    type Fruit = Option<DocAddress>;

    fn collect(&mut self, doc: DocId, _: tantivy::Score) {
        if let Some(filter) = &self.filter {
            if filter.should_skip(doc) {
                return;
            }
        }

        if self.first_doc.is_none() {
            self.first_doc = Some(DocAddress::new(self.shard_id, self.segment_id, doc));
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.first_doc
    }
}
