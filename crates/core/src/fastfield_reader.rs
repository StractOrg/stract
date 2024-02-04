// Stract is an open source web search engine.
// Copyright (C) 2023 Stract ApS
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

use std::{collections::HashMap, sync::Arc};

use tantivy::{columnar::ColumnValues, DocId, SegmentId};

use crate::{
    enum_map::EnumMap,
    schema::{DataType, FastField, Field},
};

#[derive(Default, Clone)]
struct InnerFastFieldReader {
    segments: HashMap<SegmentId, Arc<SegmentReader>>,
}

#[derive(Default, Clone)]
pub struct FastFieldReader {
    inner: Arc<InnerFastFieldReader>,
}

impl FastFieldReader {
    pub fn get_segment(&self, segment: &SegmentId) -> Arc<SegmentReader> {
        Arc::clone(self.inner.segments.get(segment).unwrap())
    }
}

impl FastFieldReader {
    pub fn new(tv_searcher: &tantivy::Searcher) -> Self {
        let mut segments = HashMap::new();

        for reader in tv_searcher.segment_readers() {
            let fastfield_readers = reader.fast_fields();

            let mut field_readers = Vec::new();

            let mut tv_readers = EnumMap::new();

            for field in Field::all().filter_map(|field| field.as_fast()) {
                match field.data_type() {
                    DataType::U64 => {
                        let reader = fastfield_readers.u64(field.name()).unwrap();
                        tv_readers.insert(field, reader);
                    }
                };
            }

            for doc in 0..reader.max_doc() as usize {
                let mut field_reader = FieldReader {
                    data: EnumMap::new(),
                };

                for field in Field::all().filter_map(|field| field.as_fast()) {
                    match field.data_type() {
                        DataType::U64 => {
                            let reader = tv_readers.get(field).unwrap();
                            field_reader
                                .data
                                .insert(field, reader.values.get_val(doc as u32))
                        }
                    };
                }

                field_readers.push(field_reader);
            }

            segments.insert(
                reader.segment_id(),
                Arc::new(SegmentReader { field_readers }),
            );
        }

        Self {
            inner: Arc::new(InnerFastFieldReader { segments }),
        }
    }
}

pub struct FieldReader {
    data: EnumMap<FastField, u64>,
}

impl FieldReader {
    pub fn get(&self, field: &FastField) -> u64 {
        self.data.get(*field).copied().unwrap()
    }
}

pub struct SegmentReader {
    field_readers: Vec<FieldReader>,
}

impl SegmentReader {
    pub fn get_field_reader(&self, doc: &DocId) -> &FieldReader {
        &self.field_readers.as_slice()[*doc as usize]
    }
}
