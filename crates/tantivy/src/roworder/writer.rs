use std::{collections::BTreeMap, io};

use crate::{
    indexer::doc_id_mapping::DocIdMapping,
    roworder::RowIndexer,
    schema::{
        document::{ReferenceValue, ReferenceValueLeaf},
        Schema, Value,
    },
};

use super::{Field, RowValue, RowValueType};

struct Row {
    values: Vec<RowValue>,
}

impl TryFrom<(crate::schema::Field, &crate::schema::FieldEntry)> for Field {
    type Error = crate::TantivyError;

    fn try_from(
        (field, entry): (crate::schema::Field, &crate::schema::FieldEntry),
    ) -> Result<Self, Self::Error> {
        let name = entry.name().to_string();
        let id = field.field_id();

        let value_type = match entry.field_type() {
            crate::schema::FieldType::U64(_) => RowValueType::U64,
            crate::schema::FieldType::I64(_) => RowValueType::I64,
            crate::schema::FieldType::F64(_) => RowValueType::F64,
            crate::schema::FieldType::Bool(_) => RowValueType::Bool,
            crate::schema::FieldType::U128(_) => RowValueType::U128,
            crate::schema::FieldType::Str(_)
            | crate::schema::FieldType::Date(_)
            | crate::schema::FieldType::Bytes(_)
            | crate::schema::FieldType::JsonObject(_)
            | crate::schema::FieldType::IpAddr(_) => {
                return Err(crate::TantivyError::SchemaError(format!(
                    "Field {:?} is not supported in row order",
                    entry
                )))
            }
        };

        Ok(Field::new(name, id, value_type))
    }
}

pub struct RowFieldsWriter {
    fields: Vec<Field>,
    rows: Vec<Row>,
    mem_usage: usize,

    field_ids: Vec<bool>,
}

impl RowFieldsWriter {
    pub fn from_schema(schema: &Schema) -> crate::Result<Self> {
        let mut fields = Vec::new();

        for (field, entry) in schema.fields().filter(|(_, entry)| entry.is_row_order()) {
            let field = Field::try_from((field, entry))?;
            fields.push(field);
        }
        let max_field_id = fields.iter().map(|field| field.id).max().unwrap_or(0);
        let mut field_ids = vec![false; max_field_id as usize + 1];
        for field in &fields {
            field_ids[field.id as usize] = true;
        }

        Ok(Self {
            fields,
            rows: Vec::new(),
            mem_usage: 0,
            field_ids,
        })
    }

    pub fn mem_usage(&self) -> usize {
        self.mem_usage
    }

    pub fn add_document<D>(&mut self, doc: &D) -> crate::Result<()>
    where
        D: crate::Document,
    {
        let mut buf = BTreeMap::new();

        for (field, value) in doc.iter_fields_and_values().filter(|(field, _)| {
            self.field_ids
                .get(field.field_id() as usize)
                .copied()
                .unwrap_or(false)
        }) {
            let value_access = value as D::Value<'_>;

            match value_access.as_value() {
                ReferenceValue::Leaf(leaf) => match leaf {
                    ReferenceValueLeaf::Null => {
                        unimplemented!("Null values are not supported in row order")
                    }
                    ReferenceValueLeaf::Date(_) => {
                        unimplemented!("Date values are not supported in row order")
                    }
                    ReferenceValueLeaf::Bytes(_) => {
                        unimplemented!("Bytes values are not supported in row order")
                    }
                    ReferenceValueLeaf::IpAddr(_) => {
                        unimplemented!("IpAddr values are not supported in row order")
                    }
                    ReferenceValueLeaf::PreTokStr(_) => {
                        unimplemented!("PreTokStr values are not supported in row order")
                    }
                    ReferenceValueLeaf::Str(_) => {
                        unimplemented!("String values are not supported in row order")
                    }

                    ReferenceValueLeaf::U64(val) => {
                        buf.insert(field.field_id(), RowValue::U64(val));
                    }
                    ReferenceValueLeaf::U128(val) => {
                        buf.insert(field.field_id(), RowValue::U128(val));
                    }
                    ReferenceValueLeaf::I64(val) => {
                        buf.insert(field.field_id(), RowValue::I64(val));
                    }
                    ReferenceValueLeaf::F64(val) => {
                        buf.insert(field.field_id(), RowValue::F64(val));
                    }
                    ReferenceValueLeaf::Bool(val) => {
                        buf.insert(field.field_id(), RowValue::Bool(val));
                    }
                },
                ReferenceValue::Array(_) => {
                    unimplemented!("Array values are not supported in row order")
                }
                ReferenceValue::Object(_) => {
                    unimplemented!("Json values are not supported in row order")
                }
            }
        }

        let row = Row {
            values: buf.into_values().collect(),
        };

        self.rows.push(row);

        Ok(())
    }

    pub fn serialize(
        self,
        wrt: &mut dyn io::Write,
        doc_id_map_opt: Option<&DocIdMapping>,
    ) -> io::Result<()> {
        let mut indexer = RowIndexer::new(wrt, self.fields)?;

        match doc_id_map_opt {
            Some(doc_id_map) => {
                for old_id in doc_id_map.iter_old_doc_ids() {
                    let row = &self.rows[old_id as usize];
                    indexer.write_row(&row.values);
                }
            }
            None => {
                for row in self.rows {
                    indexer.write_row(&row.values);
                }
            }
        }

        indexer.finish()?;

        Ok(())
    }
}
