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

use tantivy::{columnar::Column, index::SegmentId};

use super::schema::{Field, FieldEnum, FieldEnumDiscriminants};
use crate::{enum_map::EnumMap, Result};

#[derive(Debug, Clone)]
pub struct SegmentColumnFields {
    u64_fields: EnumMap<FieldEnumDiscriminants, Column<u64>>,
    u128_fields: EnumMap<FieldEnumDiscriminants, Column<u128>>,
    f64_fields: EnumMap<FieldEnumDiscriminants, Column<f64>>,
}

impl SegmentColumnFields {
    pub fn u64<F: Field>(&self, field: F) -> Option<Column<u64>> {
        self.u64_by_enum(field.into())
    }

    pub fn u64_by_enum(&self, field: FieldEnum) -> Option<Column<u64>> {
        self.u64_fields
            .get(FieldEnumDiscriminants::from(field))
            .cloned()
    }

    pub fn u128<F: Field>(&self, field: F) -> Option<Column<u128>> {
        self.u128_by_enum(field.into())
    }

    pub fn u128_by_enum(&self, field: FieldEnum) -> Option<Column<u128>> {
        self.u128_fields
            .get(FieldEnumDiscriminants::from(field))
            .cloned()
    }

    pub fn f64<F: Field>(&self, field: F) -> Option<Column<f64>> {
        self.f64_by_enum(field.into())
    }

    pub fn f64_by_enum(&self, field: FieldEnum) -> Option<Column<f64>> {
        self.f64_fields
            .get(FieldEnumDiscriminants::from(field))
            .cloned()
    }
}

#[derive(Debug, Clone)]
pub struct WarmedColumnFields {
    segments: HashMap<SegmentId, SegmentColumnFields>,
}

impl WarmedColumnFields {
    pub fn new(tantivy_searcher: &tantivy::Searcher) -> Result<Self> {
        let mut segments = HashMap::new();
        for segment in tantivy_searcher.segment_readers() {
            let mut u64_fields = EnumMap::new();
            let mut u128_fields = EnumMap::new();
            let mut f64_fields = EnumMap::new();
            for field in FieldEnum::iter() {
                match field.field_type() {
                    tantivy::schema::FieldType::U64(numeric_options)
                        if numeric_options.is_columnar() =>
                    {
                        let column = segment.column_fields().u64(field.name())?;
                        u64_fields.insert(field.into(), column);
                    }
                    tantivy::schema::FieldType::U128(numeric_options)
                        if numeric_options.is_columnar() =>
                    {
                        let column = segment.column_fields().u128(field.name())?;
                        u128_fields.insert(field.into(), column);
                    }
                    tantivy::schema::FieldType::F64(numeric_options)
                        if numeric_options.is_columnar() =>
                    {
                        let column = segment.column_fields().f64(field.name())?;
                        f64_fields.insert(field.into(), column);
                    }
                    _ => {}
                }
            }
            segments.insert(
                segment.segment_id(),
                SegmentColumnFields {
                    u64_fields,
                    u128_fields,
                    f64_fields,
                },
            );
        }

        Ok(Self { segments })
    }

    pub fn segment(&self, segment_id: &SegmentId) -> &SegmentColumnFields {
        self.segments.get(segment_id).as_ref().unwrap()
    }
}
