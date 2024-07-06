use std::result;

use crate::schema::FieldEntry;

/// `ColumnFieldNotAvailableError` is returned when the
/// user requested for a columnar field reader, and the field was not
/// defined in the schema as a columnar field.
#[derive(Debug, Error)]
#[error("Fast field not available: '{field_name:?}'")]
pub struct ColumnFieldNotAvailableError {
    pub(crate) field_name: String,
}

impl ColumnFieldNotAvailableError {
    /// Creates a `ColumnFieldNotAvailable` error.
    /// `field_entry` is the configuration of the field
    /// for which columnar fields are not available.
    pub fn new(field_entry: &FieldEntry) -> ColumnFieldNotAvailableError {
        ColumnFieldNotAvailableError {
            field_name: field_entry.name().to_string(),
        }
    }
}

/// Result when trying to access a columnar field reader.
pub type Result<R> = result::Result<R, ColumnFieldNotAvailableError>;
