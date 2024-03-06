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

            let mut segment_reader_data = Vec::new();

            let mut tv_readers = EnumMap::new();

            for field in Field::all().filter_map(Field::as_fast) {
                match field.data_type() {
                    DataType::U64 => {
                        let reader = fastfield_readers.u64(field.name()).unwrap();
                        tv_readers.insert(field, reader);
                    }
                };
            }

            for doc in 0..reader.max_doc() as usize {
                let mut data = Vec::new();

                for field in Field::all().filter_map(Field::as_fast) {
                    match field.data_type() {
                        DataType::U64 => {
                            let reader = tv_readers.get(field).unwrap();
                            data.push((field as usize, reader.values.get_val(doc as u32)));
                        }
                    };
                }

                data.sort_by_key(|(a, _)| *a);

                segment_reader_data.extend(data.into_iter().map(|(_, val)| val));
            }

            segments.insert(
                reader.segment_id(),
                Arc::new(SegmentReader {
                    data: segment_reader_data,
                    num_fields: tv_readers.len(),
                }),
            );
        }

        Self {
            inner: Arc::new(InnerFastFieldReader { segments }),
        }
    }
}

pub struct FieldReader<'a> {
    data: &'a [u64],
}

impl<'a> FieldReader<'a> {
    pub fn get(&self, field: FastField) -> u64 {
        self.data[field as usize]
    }
}

pub struct SegmentReader {
    data: Vec<u64>,
    num_fields: usize,
}

impl SegmentReader {
    pub fn get_field_reader(&self, doc: DocId) -> FieldReader<'_> {
        let data =
            &self.data[(doc as usize) * self.num_fields..(doc as usize + 1) * self.num_fields];
        FieldReader { data }
    }
}
