use super::{Column, DocId};

#[derive(Debug, Default, Clone)]
pub struct ColumnBlockAccessor<T> {
    val_cache: Vec<T>,
}

impl<T: PartialOrd + Copy + std::fmt::Debug + Send + Sync + 'static + Default>
    ColumnBlockAccessor<T>
{
    #[inline]
    pub fn fetch_block<'a>(&'a mut self, docs: &'a [u32], accessor: &Column<T>) {
        self.val_cache.resize(docs.len(), T::default());
        accessor.values.get_vals(docs, &mut self.val_cache);
    }
    #[inline]
    pub fn fetch_block_with_missing(&mut self, docs: &[u32], accessor: &Column<T>) {
        self.fetch_block(docs, accessor);
    }

    #[inline]
    pub fn iter_vals(&self) -> impl Iterator<Item = T> + '_ {
        self.val_cache.iter().cloned()
    }

    #[inline]
    /// Returns an iterator over the docids and values
    /// The passed in `docs` slice needs to be the same slice that was passed to `fetch_block` or
    /// `fetch_block_with_missing`.
    ///
    /// The docs is used if the column is full (each docs has exactly one value), otherwise the
    /// internal docid vec is used for the iterator, which e.g. may contain duplicate docs.
    pub fn iter_docid_vals<'a>(&'a self, docs: &'a [u32]) -> impl Iterator<Item = (DocId, T)> + 'a {
        docs.iter().cloned().zip(self.val_cache.iter().cloned())
    }
}
