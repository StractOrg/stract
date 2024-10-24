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

use std::{collections::HashMap, sync::Arc};

use tantivy::{columnar::ColumnValues, index::SegmentId, DocId};

use crate::{
    enum_map::EnumMap,
    schema::{
        numerical_field::{NumericalField, Orientation},
        DataType, Field, NumericalFieldEnum,
    },
};

#[derive(Default)]
struct InnerNumericalFieldReader {
    segments: HashMap<SegmentId, SegmentReader>,
}

#[derive(Default, Clone)]
pub struct NumericalFieldReader {
    inner: Arc<InnerNumericalFieldReader>,
}

impl NumericalFieldReader {
    pub fn borrow_segment(&self, segment: &SegmentId) -> &SegmentReader {
        self.inner.segments.get(segment).as_ref().unwrap()
    }
}

impl NumericalFieldReader {
    pub fn new(tv_searcher: &tantivy::Searcher) -> Self {
        let mut segments = HashMap::new();

        for reader in tv_searcher.segment_readers() {
            let mut field_ids = EnumMap::new();
            let columnfield_readers = reader.column_fields();

            let mut u64s = EnumMap::new();
            let mut bytes = EnumMap::new();
            let mut bools = EnumMap::new();
            let mut f64s = EnumMap::new();

            for field in Field::all().filter_map(|f| f.as_numerical()) {
                match field.data_type() {
                    DataType::U64 => {
                        if let Ok(reader) = columnfield_readers.u64(field.name()) {
                            u64s.insert(field, reader.values);
                        }
                    }
                    DataType::F64 => {
                        if let Ok(reader) = columnfield_readers.f64(field.name()) {
                            f64s.insert(field, reader.values);
                        }
                    }
                    DataType::Bool => {
                        if let Ok(reader) = columnfield_readers.bool(field.name()) {
                            bools.insert(field, reader.values);
                        }
                    }
                    DataType::Bytes => {
                        if let Ok(Some(reader)) = columnfield_readers.bytes(field.name()) {
                            bytes.insert(field, reader);
                        }
                    }
                };

                field_ids.insert(
                    field,
                    reader.schema().get_field(field.name()).unwrap().field_id(),
                );
            }

            segments.insert(
                reader.segment_id(),
                SegmentReader {
                    row_reader: reader.row_fields().clone(),
                    columnar_readers: ColumnarReaders {
                        u64s,
                        bytes,
                        bools,
                        f64s,
                    },
                    field_ids,
                    row: None,
                },
            );
        }

        Self {
            inner: Arc::new(InnerNumericalFieldReader { segments }),
        }
    }
}

#[derive(Clone)]
struct ColumnarReaders {
    u64s: EnumMap<NumericalFieldEnum, Arc<dyn ColumnValues<u64>>>,
    f64s: EnumMap<NumericalFieldEnum, Arc<dyn ColumnValues<f64>>>,
    bools: EnumMap<NumericalFieldEnum, Arc<dyn ColumnValues<bool>>>,
    bytes: EnumMap<NumericalFieldEnum, tantivy::columnar::BytesColumn>,
}

pub enum Value {
    U64(u64),
    F64(f64),
    Bytes(Vec<u8>),
    Bool(bool),
}

impl Value {
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Value::U64(val) => Some(*val),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::F64(val) => Some(*val),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(val) => Some(val),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(val) => Some(*val),
            _ => None,
        }
    }
}

impl From<u64> for Value {
    fn from(val: u64) -> Self {
        Value::U64(val)
    }
}

impl From<f64> for Value {
    fn from(val: f64) -> Self {
        Value::F64(val)
    }
}

impl From<Vec<u8>> for Value {
    fn from(val: Vec<u8>) -> Self {
        Value::Bytes(val)
    }
}

impl From<bool> for Value {
    fn from(val: bool) -> Self {
        Value::Bool(val)
    }
}

impl From<Value> for Option<u64> {
    fn from(val: Value) -> Self {
        val.as_u64()
    }
}

impl<'a> From<&'a Value> for Option<&'a [u8]> {
    fn from(val: &'a Value) -> Self {
        val.as_bytes()
    }
}

impl From<Value> for Option<Vec<u8>> {
    fn from(val: Value) -> Self {
        match val {
            Value::Bytes(val) => Some(val),
            _ => None,
        }
    }
}

impl From<Value> for Option<bool> {
    fn from(val: Value) -> Self {
        val.as_bool()
    }
}

pub struct FieldReader<'a> {
    columnar_readers: &'a ColumnarReaders,
    row: Option<&'a tantivy::roworder::Row>,
    field_ids: &'a EnumMap<NumericalFieldEnum, u32>,
    doc: DocId,
}

impl<'a> FieldReader<'a> {
    pub fn get(&self, field: NumericalFieldEnum) -> Option<Value> {
        if field.orientation().contains(Orientation::ROW) {
            let field_id = self.field_ids.get(field)?;

            self.row.as_ref().and_then(|row| match field.data_type() {
                DataType::U64 => row.get_u64(field_id).map(Value::U64),
                DataType::F64 => row.get_f64(field_id).map(Value::F64),
                DataType::Bool => row.get_bool(field_id).map(Value::Bool),
                DataType::Bytes => unimplemented!("bytes fields cannot be row oriented"),
            })
        } else if field.orientation().contains(Orientation::COLUMNAR) {
            match field.data_type() {
                DataType::U64 => Some(
                    self.columnar_readers
                        .u64s
                        .get(field)?
                        .get_val(self.doc)
                        .into(),
                ),

                DataType::F64 => Some(
                    self.columnar_readers
                        .f64s
                        .get(field)?
                        .get_val(self.doc)
                        .into(),
                ),

                DataType::Bool => Some(
                    self.columnar_readers
                        .bools
                        .get(field)?
                        .get_val(self.doc)
                        .into(),
                ),

                DataType::Bytes => {
                    let reader = self.columnar_readers.bytes.get(field)?;
                    let ord = reader.ords().values.get_val(self.doc);

                    if ord > reader.num_terms() as u64 || reader.num_terms() == 0 {
                        return None;
                    }

                    let mut bytes = Vec::new();
                    reader.ord_to_bytes(ord, &mut bytes).unwrap();

                    if bytes.is_empty() {
                        None
                    } else {
                        Some(bytes.into())
                    }
                }
            }
        } else {
            None
        }
    }
}

struct Row {
    doc: DocId,
    row: tantivy::roworder::Row,
}

pub struct SegmentReader {
    columnar_readers: ColumnarReaders,
    row_reader: tantivy::roworder::readers::RowFieldReaders,
    field_ids: EnumMap<NumericalFieldEnum, u32>,
    row: Option<Row>,
}

impl Clone for SegmentReader {
    fn clone(&self) -> Self {
        Self {
            columnar_readers: self.columnar_readers.clone(),
            row_reader: self.row_reader.clone(),
            field_ids: self.field_ids.clone(),
            row: None,
        }
    }
}

impl SegmentReader {
    pub fn prepare_row_for_doc(&mut self, doc: DocId) {
        self.row = Some(Row {
            doc,
            row: self.row_reader.row_index().get_row(doc as usize).unwrap(),
        });
    }

    pub fn get_field_reader(&self, doc: DocId) -> FieldReader<'_> {
        debug_assert!(
            self.row.as_ref().map(|r| r.doc == doc).unwrap_or(true),
            "Row prepared for wrong doc"
        );

        FieldReader {
            columnar_readers: &self.columnar_readers,
            field_ids: &self.field_ids,
            row: self.row.as_ref().map(|r| &r.row),
            doc,
        }
    }
}
