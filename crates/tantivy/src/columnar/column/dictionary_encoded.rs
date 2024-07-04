use std::sync::Arc;
use std::{fmt, io};

use crate::sstable::{Dictionary, VoidSSTable};

use super::RowId;
use crate::columnar::column::Column;

/// Dictionary encoded column.
///
/// The column simply gives access to a regular u64-column that, in
/// which the values are term-ordinals.
///
/// These ordinals are ids uniquely identify the bytes that are stored in
/// the column. These ordinals are small, and sorted in the same order
/// as the term_ord_column.
#[derive(Clone)]
pub struct BytesColumn {
    pub(crate) dictionary: Arc<Dictionary<VoidSSTable>>,
    pub(crate) term_ord_column: Column<u64>,
}

impl fmt::Debug for BytesColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BytesColumn")
            .field("term_ord_column", &self.term_ord_column)
            .finish()
    }
}

impl BytesColumn {
    /// Fills the given `output` buffer with the term associated to the ordinal `ord`.
    ///
    /// Returns `false` if the term does not exist (e.g. `term_ord` is greater or equal to the
    /// overll number of terms).
    pub fn ord_to_bytes(&self, ord: u64, output: &mut Vec<u8>) -> io::Result<bool> {
        self.dictionary.ord_to_term(ord, output)
    }

    /// Returns the number of rows in the column.
    pub fn num_rows(&self) -> RowId {
        self.term_ord_column.num_docs()
    }

    pub fn term_ords(&self, row_id: RowId) -> impl Iterator<Item = u64> + '_ {
        self.term_ord_column.first(row_id).into_iter()
    }

    /// Returns the column of ordinals
    pub fn ords(&self) -> &Column<u64> {
        &self.term_ord_column
    }

    pub fn num_terms(&self) -> usize {
        self.dictionary.num_terms()
    }

    pub fn dictionary(&self) -> &Dictionary<VoidSSTable> {
        self.dictionary.as_ref()
    }
}
