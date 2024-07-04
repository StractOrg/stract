mod shuffled;
mod stacked;

use shuffled::merge_column_index_shuffled;
use stacked::merge_column_index_stacked;

use crate::columnar::column_index::SerializableColumnIndex;
use crate::columnar::{Cardinality, MergeRowOrder};

pub fn merge_column_index(merge_row_order: &MergeRowOrder) -> SerializableColumnIndex {
    let cardinality_after_merge = Cardinality::Full;
    match merge_row_order {
        MergeRowOrder::Stack(_) => merge_column_index_stacked(cardinality_after_merge),
        MergeRowOrder::Shuffled(_) => merge_column_index_shuffled(cardinality_after_merge),
    }
}

// TODO actually, the shuffled code path is a bit too general.
// In practise, we do not really shuffle everything.
// The merge order restricted to a specific column keeps the original row order.
//
// This may offer some optimization that we have not explored yet.
