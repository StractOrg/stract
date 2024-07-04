use crate::columnar::column_index::SerializableColumnIndex;
use crate::columnar::Cardinality;

pub fn merge_column_index_shuffled(
    cardinality_after_merge: Cardinality,
) -> SerializableColumnIndex {
    match cardinality_after_merge {
        Cardinality::Full => SerializableColumnIndex::Full,
    }
}
