use crate::columnar::column_index::SerializableColumnIndex;
use crate::columnar::Cardinality;

/// Simple case:
/// The new mapping just consists in stacking the different column indexes.
///
/// There are no sort nor deletes involved.
pub fn merge_column_index_stacked(cardinality_after_merge: Cardinality) -> SerializableColumnIndex {
    match cardinality_after_merge {
        Cardinality::Full => SerializableColumnIndex::Full,
    }
}
