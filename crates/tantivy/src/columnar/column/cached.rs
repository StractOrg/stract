use std::cell::Cell;
use std::fmt::Debug;

use crate::columnar::RowId;

use super::Column;

#[derive(Clone, Copy)]
struct CachedValue<T> {
    value: T,
    row_id: RowId,
}

/// A column that caches the last accessed value to avoid re-reading from the underlying column.
pub struct CachedColumn<T> {
    column: Column<T>,
    cache: Cell<Option<CachedValue<T>>>,
}

impl<T> CachedColumn<T> {
    pub fn new(column: Column<T>) -> Self {
        Self {
            column,
            cache: Cell::new(None),
        }
    }
}

impl<T> CachedColumn<T>
where
    T: PartialOrd + Copy + Debug + Send + Sync + 'static,
{
    pub fn num_docs(&self) -> RowId {
        self.column.num_docs()
    }

    pub fn min_value(&self) -> T {
        self.column.min_value()
    }

    pub fn max_value(&self) -> T {
        self.column.max_value()
    }

    #[inline]
    pub fn first(&self, row_id: RowId) -> Option<T> {
        if let Some(cached_value) = self.cache.get() {
            if cached_value.row_id == row_id {
                return Some(cached_value.value);
            }
        }

        let value = self.column.first(row_id);
        if let Some(value) = value {
            self.cache.set(Some(CachedValue { value, row_id }));
        }
        value
    }
}
