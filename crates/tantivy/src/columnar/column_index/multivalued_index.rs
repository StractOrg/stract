use std::io;
use std::io::Write;
use std::sync::Arc;

use crate::common::OwnedBytes;

use super::RowId;
use crate::columnar::column_values::{
    load_u64_based_column_values, serialize_u64_based_column_values, CodecType, ColumnValues,
};
use crate::columnar::iterable::Iterable;

pub fn serialize_multivalued_index(
    multivalued_index: &dyn Iterable<RowId>,
    output: &mut impl Write,
) -> io::Result<()> {
    serialize_u64_based_column_values(
        multivalued_index,
        &[CodecType::Bitpacked, CodecType::Linear],
        output,
    )?;
    Ok(())
}

pub fn open_multivalued_index(bytes: OwnedBytes) -> io::Result<MultiValueIndex> {
    let start_index_column: Arc<dyn ColumnValues<RowId>> = load_u64_based_column_values(bytes)?;
    Ok(MultiValueIndex { start_index_column })
}

#[derive(Clone)]
/// Index to resolve value range for given doc_id.
/// Starts at 0.
pub struct MultiValueIndex {
    pub start_index_column: Arc<dyn crate::columnar::ColumnValues<RowId>>,
}

impl std::fmt::Debug for MultiValueIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("MultiValuedIndex")
            .field("num_rows", &self.start_index_column.num_vals())
            .finish_non_exhaustive()
    }
}

impl From<Arc<dyn ColumnValues<RowId>>> for MultiValueIndex {
    fn from(start_index_column: Arc<dyn ColumnValues<RowId>>) -> Self {
        MultiValueIndex { start_index_column }
    }
}

impl MultiValueIndex {
    pub fn for_test(start_offsets: &[RowId]) -> MultiValueIndex {
        let mut buffer = Vec::new();
        serialize_multivalued_index(&start_offsets, &mut buffer).unwrap();
        let bytes = OwnedBytes::new(buffer);
        open_multivalued_index(bytes).unwrap()
    }

    /// Returns the number of documents in the index.
    #[inline]
    pub fn num_docs(&self) -> u32 {
        self.start_index_column.num_vals() - 1
    }
}
