use super::ColumnStatsCollector;

/// A collector for the minimum and maximum values in a column.
#[derive(Clone, Default)]
pub struct MinMaxCollector<T = u64> {
    min_max_opt: Option<(T, T)>,
}

impl ColumnStatsCollector<u64> for MinMaxCollector<u64> {
    type Stats = (u64, u64);

    fn collect(&mut self, value: u64) {
        self.min_max_opt = Some(if let Some((min, max)) = self.min_max_opt {
            (min.min(value), max.max(value))
        } else {
            (value, value)
        });
    }

    fn finalize(&self) -> Self::Stats {
        self.min_max_opt.unwrap_or((0, 0))
    }

    fn num_bytes(&self) -> u64 {
        8 + 8
    }
}

impl ColumnStatsCollector<u128> for MinMaxCollector<u128> {
    type Stats = (u128, u128);

    fn collect(&mut self, value: u128) {
        self.min_max_opt = Some(if let Some((min, max)) = self.min_max_opt {
            (min.min(value), max.max(value))
        } else {
            (value, value)
        });
    }

    fn finalize(&self) -> Self::Stats {
        self.min_max_opt.unwrap_or((0, 0))
    }

    fn num_bytes(&self) -> u64 {
        16 + 16
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compute_stats(vals: impl Iterator<Item = u64>) -> (u64, u64) {
        let mut stats_collector = MinMaxCollector::default();
        for val in vals {
            stats_collector.collect(val);
        }
        stats_collector.finalize()
    }

    #[test]
    fn test_compute_min_max() {
        assert_eq!(compute_stats([].into_iter()), (0, 0));
        assert_eq!(compute_stats([1, 2, 3].into_iter()), (1, 3));
        assert_eq!(compute_stats([3, 2, 1].into_iter()), (1, 3));
        assert_eq!(compute_stats([1, 1, 1].into_iter()), (1, 1));
        assert_eq!(compute_stats([1, 2, 5, 4, 3].into_iter()), (1, 5));
    }
}
