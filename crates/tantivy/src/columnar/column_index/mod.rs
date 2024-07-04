//! # `column_index`
//!
//! `column_index` provides rank and select operations to associate positions when not all
//! documents have exactly one element.

mod merge;
mod serialize;

use std::ops::Range;

pub use merge::merge_column_index;
pub use serialize::{open_column_index, serialize_column_index, SerializableColumnIndex};

use super::{Cardinality, DocId, RowId};

#[derive(Clone, Debug)]
pub enum ColumnIndex {
    Full,
}

impl ColumnIndex {
    pub fn value_row_ids(&self, doc_id: DocId) -> Range<RowId> {
        match self {
            ColumnIndex::Full => doc_id..doc_id + 1,
        }
    }

    /// Translates a block of docis to row_ids.
    ///
    /// returns the row_ids and the matching docids on the same index
    /// e.g.
    /// DocId In:  [0, 5, 6]
    /// DocId Out: [0, 0, 6, 6]
    /// RowId Out: [0, 1, 2, 3]
    #[inline]
    pub fn docids_to_rowids(
        &self,
        doc_ids: &[DocId],
        doc_ids_out: &mut Vec<DocId>,
        row_ids: &mut Vec<RowId>,
    ) {
        match self {
            ColumnIndex::Full => {
                doc_ids_out.extend_from_slice(doc_ids);
                row_ids.extend_from_slice(doc_ids);
            }
        }
    }

    pub fn docid_range_to_rowids(&self, doc_id_range: Range<DocId>) -> Range<RowId> {
        match self {
            ColumnIndex::Full => doc_id_range,
        }
    }
}
