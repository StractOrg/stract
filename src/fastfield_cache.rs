// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use tantivy::{fastfield::Column, DocId, SegmentId};

use crate::schema::{DataType, FastField, Field, ALL_FIELDS};

#[derive(Default)]
pub struct FastFieldCache {
    segments: RwLock<HashMap<SegmentId, Arc<SegmentCache>>>,
}

impl FastFieldCache {
    pub fn get_segment(&self, segment: &SegmentId) -> Arc<SegmentCache> {
        Arc::clone(self.segments.read().unwrap().get(segment).unwrap())
    }
}

impl tantivy::Warmer for FastFieldCache {
    fn warm(&self, searcher: &tantivy::Searcher) -> tantivy::Result<()> {
        let mut segments_wrt = self.segments.write().unwrap();

        let mut fast_fields: Vec<_> = ALL_FIELDS
            .into_iter()
            .filter_map(|field| field.as_fast())
            .collect();
        fast_fields.sort_by_key(|field| *field as usize);

        let schema = searcher.schema();

        for reader in searcher.segment_readers() {
            let fastfield_readers = reader.fast_fields();
            let max_doc = reader.max_doc();

            let mut doc_caches = Vec::new();

            for field in &fast_fields {
                let tv_field = schema.get_field(Field::Fast(*field).name()).unwrap();

                let doc_cache = match field.data_type() {
                    DataType::U64 => {
                        let reader = fastfield_readers.u64(tv_field).unwrap();
                        let mut output = vec![0; max_doc as usize];
                        reader.get_range(0, &mut output);

                        let docs = output.into_iter().map(FieldValue::U64).collect();

                        DocCache { docs }
                    }
                    DataType::F64 => {
                        let reader = fastfield_readers.f64(tv_field).unwrap();
                        let mut output = vec![0.0; max_doc as usize];
                        reader.get_range(0, &mut output);

                        let docs = output.into_iter().map(FieldValue::F64).collect();
                        DocCache { docs }
                    }
                    DataType::U64s => {
                        let reader = fastfield_readers.u64s(tv_field).unwrap();
                        let mut docs = Vec::new();

                        for doc_id in 0..max_doc {
                            let mut output = Vec::new();
                            reader.get_vals(doc_id, &mut output);
                            docs.push(output);
                        }

                        let docs = docs.into_iter().map(FieldValue::U64s).collect();

                        DocCache { docs }
                    }
                };

                doc_caches.push((field, doc_cache));
            }

            doc_caches.sort_by_key(|(field, _)| **field as usize);

            segments_wrt.insert(
                reader.segment_id(),
                Arc::new(SegmentCache {
                    doc_caches: doc_caches.into_iter().map(|(_, cache)| cache).collect(),
                }),
            );
        }

        Ok(())
    }

    fn garbage_collect(&self, _live_generations: &[&tantivy::SearcherGeneration]) {}
}

pub enum FieldValue {
    U64(u64),
    U64s(Vec<u64>),
    F64(f64),
}

#[derive(Default)]
pub struct DocCache {
    docs: Vec<FieldValue>,
}

impl DocCache {
    pub fn get(&self, doc: &DocId) -> &FieldValue {
        self.docs.get(*doc as usize).unwrap()
    }

    pub fn get_u64(&self, doc: &DocId) -> Option<u64> {
        match self.get(doc) {
            FieldValue::U64(res) => Some(*res),
            _ => None,
        }
    }

    pub fn get_u64s(&self, doc: &DocId) -> Option<&[u64]> {
        match self.get(doc) {
            FieldValue::U64s(res) => Some(res),
            _ => None,
        }
    }
}

pub struct SegmentCache {
    doc_caches: Vec<DocCache>, // fast_field -> doc_cache
}

impl SegmentCache {
    pub fn get_doc_cache(&self, field: &FastField) -> &DocCache {
        self.doc_caches.get(*field as usize).unwrap()
    }
}
