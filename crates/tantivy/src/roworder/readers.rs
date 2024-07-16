use std::io;
use std::sync::Arc;

use crate::common::file_slice::FileSlice;
use crate::schema::Schema;
use crate::space_usage::{FieldUsage, PerFieldSpaceUsage};
use crate::{Result, TantivyError};

use super::RowIndex;

#[derive(Clone)]
pub struct RowFieldReaders {
    index: Arc<RowIndex>,
}

impl RowFieldReaders {
    pub fn open(slice: FileSlice) -> Result<Self> {
        Ok(Self {
            index: Arc::new(RowIndex::open(slice.read_bytes()?).map_err(|_| {
                TantivyError::InternalError("Row order index failed to open".to_string())
            })?),
        })
    }

    pub fn space_usage(&self, schema: &Schema) -> io::Result<PerFieldSpaceUsage> {
        let mut per_field_usages: Vec<FieldUsage> = Default::default();
        for (field, field_entry) in schema.fields() {
            if field_entry.is_row_order() {
                let mut field_usage = FieldUsage::empty(field);

                if let Some(field) = self.index.field_by_id(field.field_id()) {
                    let num_bytes = self.index.total_num_bytes_for_field(field).into();
                    field_usage.add_field_idx(0, num_bytes);
                }

                per_field_usages.push(field_usage);
            }
        }
        Ok(PerFieldSpaceUsage::new(per_field_usages))
    }

    pub fn row_index(&self) -> &RowIndex {
        &self.index
    }
}
