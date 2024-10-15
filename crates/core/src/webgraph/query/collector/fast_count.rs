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

use super::Collector;

pub enum FastCountValue {
    U64(u64),
}

impl FastCountValue {
    fn as_term(&self, field: tantivy::schema::Field) -> tantivy::schema::Term {
        match self {
            FastCountValue::U64(value) => tantivy::schema::Term::from_field_u64(field, *value),
        }
    }
}

pub struct FastCountCollector {
    field_name: String,
    value: FastCountValue,
}

impl FastCountCollector {
    pub fn new(field_name: String, value: FastCountValue) -> Self {
        Self { field_name, value }
    }
}

impl Collector for FastCountCollector {
    type Fruit = u64;

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

        Ok(FastSegmentCountCollector(num_docs))
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as tantivy::collector::SegmentCollector>::Fruit>,
    ) -> crate::Result<Self::Fruit> {
        Ok(segment_fruits.into_iter().sum())
    }
}

pub struct FastSegmentCountCollector(u64);

impl tantivy::collector::SegmentCollector for FastSegmentCountCollector {
    type Fruit = u64;

    fn collect(&mut self, _: tantivy::DocId, _: tantivy::Score) {}

    fn harvest(self) -> Self::Fruit {
        self.0
    }
}
