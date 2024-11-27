//! # Tantivy-Columnar
//!
//! `tantivy-columnar`provides a columnar storage for tantivy.
//! The crate allows for efficient read operations on specific columns rather than entire records.
//!
//! ## Overview
//!
//! - **columnar**: Reading, writing, and merging multiple columns:
//!   - **[ColumnarWriter]**: Makes it possible to create a new columnar.
//!   - **[ColumnarReader]**: The ColumnarReader makes it possible to access a set of columns
//!     associated to field names.
//!   - **[merge_columnar]**: Contains the functionalities to merge multiple ColumnarReader or
//!     segments into a single one.
//!
//! - **column**: A single column, which contains
//!     - [column_index]: Resolves the rows for a document id. Manages the cardinality of the
//!       column.
//!     - [column_values]: Stores the values of a column in a dense format.

use std::fmt::Display;
use std::io;

mod block_accessor;
mod column;
pub mod column_index;
pub mod column_values;
mod columnar;
mod dictionary;
mod dynamic_column;
mod iterable;
pub(crate) mod utils;
mod value;

use crate::sstable::VoidSSTable;
pub use block_accessor::ColumnBlockAccessor;
pub use column::{BytesColumn, CachedColumn, Column};
pub use column_index::ColumnIndex;
pub use column_values::{
    ColumnValues, EmptyColumnValues, MonotonicallyMappableToU128, MonotonicallyMappableToU64,
};
pub use columnar::{
    merge_columnar, ColumnType, ColumnarReader, ColumnarWriter, HasAssociatedColumnType,
    MergeRowOrder, ShuffleMergeOrder, StackMergeOrder, Version, CURRENT_VERSION,
};
pub use value::{NumericalType, NumericalValue};

pub use self::dynamic_column::{DynamicColumn, DynamicColumnHandle};

pub type RowId = u32;
pub type DocId = u32;

#[derive(Clone, Copy, Debug)]
pub struct RowAddr {
    pub segment_ord: u32,
    pub row_id: RowId,
}

pub use crate::sstable::Dictionary;
pub type Streamer<'a> = crate::sstable::Streamer<'a, VoidSSTable>;

pub use crate::common::DateTime;

#[derive(Copy, Clone, Debug)]
pub struct InvalidData;

impl From<InvalidData> for io::Error {
    fn from(_: InvalidData) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, "Invalid data")
    }
}

/// Enum describing the number of values that can exist per document
/// (or per row if you will).
///
/// The cardinality must fit on 2 bits.
#[derive(Clone, Copy, Hash, Default, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Cardinality {
    /// All documents contain exactly one value.
    /// `Full` is the default for auto-detecting the Cardinality, since it is the most strict.
    #[default]
    Full = 0,
}

impl Display for Cardinality {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let short_str = match self {
            Cardinality::Full => "full",
        };
        write!(f, "{short_str}")
    }
}

impl Cardinality {
    pub fn is_full(&self) -> bool {
        matches!(self, Cardinality::Full)
    }
    pub(crate) fn to_code(self) -> u8 {
        self as u8
    }
    pub(crate) fn try_from_code(code: u8) -> Result<Cardinality, InvalidData> {
        match code {
            0 => Ok(Cardinality::Full),
            _ => Err(InvalidData),
        }
    }
}

#[cfg(test)]
mod tests;
