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

use anyhow::anyhow;
use rustc_hash::{FxHashMap, FxHashSet};
use tantivy::columnar::Column;

use crate::webgraph::{
    schema::{Field, FieldEnum},
    warmed_column_fields::WarmedColumnFields,
};

use super::Collector;

pub struct GroupExactCollector {
    group_field: FieldEnum,
    value_field: FieldEnum,
    warmed_column_fields: Option<WarmedColumnFields>,
}

impl GroupExactCollector {
    pub fn new<Group: Field, Value: Field>(group_field: Group, value_field: Value) -> Self {
        Self {
            group_field: group_field.into(),
            value_field: value_field.into(),
            warmed_column_fields: None,
        }
    }

    pub fn with_column_fields(mut self, warmed_column_fields: WarmedColumnFields) -> Self {
        self.warmed_column_fields = Some(warmed_column_fields);
        self
    }
}

impl Collector for GroupExactCollector {
    type Fruit = FxHashMap<u64, FxHashSet<u64>>;
    type Child = GroupExactSegmentCollector;

    fn for_segment(
        &self,
        _: tantivy::SegmentOrdinal,
        segment: &tantivy::SegmentReader,
    ) -> crate::Result<Self::Child> {
        let warmed_column_fields = self.warmed_column_fields.as_ref().ok_or(anyhow!(
            "Warmed column fields must be set to construct segment collector"
        ))?;

        let group = warmed_column_fields
            .segment(&segment.segment_id())
            .u64(self.group_field)
            .ok_or(anyhow!("Group field missing from index"))?;

        let value = warmed_column_fields
            .segment(&segment.segment_id())
            .u64(self.value_field)
            .ok_or(anyhow!("Value field missing from index"))?;

        Ok(GroupExactSegmentCollector {
            group,
            value,
            groups: FxHashMap::default(),
        })
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as tantivy::collector::SegmentCollector>::Fruit>,
    ) -> crate::Result<Self::Fruit> {
        let mut groups: FxHashMap<u64, FxHashSet<u64>> = FxHashMap::default();

        for fruit in segment_fruits {
            for (group, set) in fruit {
                groups.entry(group).or_default().extend(set);
            }
        }

        Ok(groups)
    }
}

pub struct GroupExactSegmentCollector {
    group: Column<u64>,
    value: Column<u64>,
    groups: FxHashMap<u64, FxHashSet<u64>>,
}

impl tantivy::collector::SegmentCollector for GroupExactSegmentCollector {
    type Fruit = FxHashMap<u64, FxHashSet<u64>>;

    fn collect(&mut self, doc: tantivy::DocId, _: tantivy::Score) {
        let group = self.group.first(doc).unwrap();
        let value = self.value.first(doc).unwrap();

        self.groups.entry(group).or_default().insert(value);
    }

    fn harvest(self) -> Self::Fruit {
        self.groups
    }
}
