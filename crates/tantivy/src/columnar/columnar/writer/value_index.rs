use super::RowId;

/// The `IndexBuilder` interprets a sequence of
/// calls of the form:
/// (record_doc,record_value+)*
/// and can then serialize the results into an index to associate docids with their value[s].
///
/// It has different implementation depending on whether the
/// cardinality is required, optional, or multivalued.
pub(crate) trait IndexBuilder {
    fn record_row(&mut self, doc: RowId);
    #[inline]
    fn record_value(&mut self) {}
}

/// The FullIndexBuilder does nothing.
#[derive(Default)]
pub struct FullIndexBuilder;

impl IndexBuilder for FullIndexBuilder {
    #[inline(always)]
    fn record_row(&mut self, _doc: RowId) {}
}

/// The `SpareIndexBuilders` is there to avoid allocating a
/// new index builder for every single column.
#[derive(Default)]
pub struct PreallocatedIndexBuilders {
    required_index_builder: FullIndexBuilder,
}

impl PreallocatedIndexBuilders {
    pub fn borrow_required_index_builder(&mut self) -> &mut FullIndexBuilder {
        &mut self.required_index_builder
    }
}
