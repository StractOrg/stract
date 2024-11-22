use crate::columnar::RowId;

use super::ColumnStatsCollector;

/// A collector for the number of rows in a column.
#[derive(Clone, Default)]
pub struct NumRowsCollector {
    num_rows: RowId,
}

impl NumRowsCollector {
    pub fn as_u64(&self) -> &dyn ColumnStatsCollector<u64, Stats = RowId> {
        self
    }

    pub fn as_u128(&self) -> &dyn ColumnStatsCollector<u128, Stats = RowId> {
        self
    }
}

impl<T> ColumnStatsCollector<T> for NumRowsCollector {
    type Stats = RowId;

    fn collect(&mut self, _value: T) {
        self.num_rows += 1;
    }

    fn finalize(&self) -> Self::Stats {
        self.num_rows
    }

    fn num_bytes(&self) -> u64 {
        8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compute_stats(vals: impl Iterator<Item = u64>) -> RowId {
        let mut stats_collector = NumRowsCollector::default();
        for val in vals {
            stats_collector.collect(val);
        }
        stats_collector.as_u64().finalize()
    }

    #[test]
    fn test_compute_num_rows() {
        assert_eq!(compute_stats([].into_iter()), 0);
        assert_eq!(compute_stats([1, 2, 3].into_iter()), 3);
        assert_eq!(compute_stats([1, 1, 1].into_iter()), 3);
    }
}
