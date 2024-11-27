mod cached;
mod dictionary_encoded;
mod serialize;

use std::fmt::{self, Debug};
use std::io::Write;
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;

use crate::common::BinarySerializable;
pub use cached::CachedColumn;
pub use dictionary_encoded::BytesColumn;
pub use serialize::{
    open_column_bytes, open_column_u128, open_column_u64, serialize_column_mappable_to_u128,
    serialize_column_mappable_to_u64,
};

use super::column_index::ColumnIndex;
use super::column_values::monotonic_mapping::StrictlyMonotonicMappingToInternal;
use super::column_values::{monotonic_map_column, ColumnValues};
use super::{Cardinality, DocId, MonotonicallyMappableToU128, MonotonicallyMappableToU64, RowId};

#[derive(Clone)]
pub struct Column<T = u64> {
    pub index: ColumnIndex,
    pub values: Arc<dyn ColumnValues<T>>,
}

impl<T: Debug + PartialOrd + Send + Sync + Copy + 'static> Debug for Column<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let num_docs = self.num_docs();
        let entries = (0..num_docs)
            .map(|i| (i, self.first(i)))
            .collect::<Vec<_>>();
        f.debug_map().entries(entries).finish()
    }
}

impl<T: MonotonicallyMappableToU64> Column<T> {
    pub fn to_u64_monotonic(self) -> Column<u64> {
        let values = Arc::new(monotonic_map_column(
            self.values,
            StrictlyMonotonicMappingToInternal::<T>::new(),
        ));
        Column {
            index: self.index,
            values,
        }
    }
}

impl<T: MonotonicallyMappableToU128> Column<T> {
    pub fn to_u128_monotonic(self) -> Column<u128> {
        let values = Arc::new(monotonic_map_column(
            self.values,
            StrictlyMonotonicMappingToInternal::<T>::new(),
        ));
        Column {
            index: self.index,
            values,
        }
    }
}

impl<T> Column<T> {
    pub fn to_cached(self) -> CachedColumn<T> {
        CachedColumn::new(self)
    }
}

impl<T: PartialOrd + Copy + Debug + Send + Sync + 'static> Column<T> {
    pub fn num_docs(&self) -> RowId {
        match &self.index {
            ColumnIndex::Full => self.values.num_vals(),
        }
    }

    pub fn min_value(&self) -> T {
        self.values.min_value()
    }

    pub fn max_value(&self) -> T {
        self.values.max_value()
    }

    #[inline]
    pub fn first(&self, row_id: RowId) -> Option<T> {
        Some(self.values.get_val(row_id))
    }

    /// Load the first value for each docid in the provided slice.
    #[inline]
    pub fn first_vals(&self, docids: &[DocId], output: &mut [Option<T>]) {
        match &self.index {
            ColumnIndex::Full => self.values.get_vals_opt(docids, output),
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
    pub fn row_ids_for_docs(
        &self,
        doc_ids: &[DocId],
        doc_ids_out: &mut Vec<DocId>,
        row_ids: &mut Vec<RowId>,
    ) {
        self.index.docids_to_rowids(doc_ids, doc_ids_out, row_ids)
    }

    /// Get the docids of values which are in the provided value range.
    #[inline]
    pub fn get_docids_for_value_range(
        &self,
        value_range: RangeInclusive<T>,
        selected_docid_range: Range<u32>,
        doc_ids: &mut Vec<u32>,
    ) {
        // convert passed docid range to row id range
        let rowid_range = self
            .index
            .docid_range_to_rowids(selected_docid_range.clone());

        // Load rows
        self.values
            .get_row_ids_for_value_range(value_range, rowid_range, doc_ids);
    }
}

impl BinarySerializable for Cardinality {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> std::io::Result<()> {
        self.to_code().serialize(writer)
    }

    fn deserialize<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let cardinality_code = u8::deserialize(reader)?;
        let cardinality = Cardinality::try_from_code(cardinality_code)?;
        Ok(cardinality)
    }
}
