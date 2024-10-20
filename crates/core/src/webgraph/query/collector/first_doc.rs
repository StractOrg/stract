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

use tantivy::{collector::SegmentCollector, DocAddress, DocId, SegmentOrdinal};

use super::Collector;

pub struct FirstDocCollector;

impl Collector for FirstDocCollector {
    type Fruit = Option<DocAddress>;
    type Child = FirstDocSegmentCollector;

    fn for_segment(
        &self,
        segment_id: SegmentOrdinal,
        _: &tantivy::SegmentReader,
    ) -> crate::Result<Self::Child> {
        Ok(FirstDocSegmentCollector::new(segment_id))
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
}

impl FirstDocSegmentCollector {
    pub fn new(segment_id: SegmentOrdinal) -> Self {
        Self {
            first_doc: None,
            segment_id,
        }
    }
}

impl SegmentCollector for FirstDocSegmentCollector {
    type Fruit = Option<DocAddress>;

    fn collect(&mut self, doc: DocId, _: tantivy::Score) {
        if self.first_doc.is_none() {
            self.first_doc = Some(DocAddress::new(self.segment_id, doc));
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.first_doc
    }
}
