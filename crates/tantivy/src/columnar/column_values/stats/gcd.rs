use std::num::NonZeroU64;

use fastdivide::DividerU64;

use super::ColumnStatsCollector;

/// Compute the gcd of two non null numbers.
///
/// It is recommended, but not required, to feed values such that `large >= small`.
fn compute_gcd(mut large: NonZeroU64, mut small: NonZeroU64) -> NonZeroU64 {
    loop {
        let rem: u64 = large.get() % small;
        if let Some(new_small) = NonZeroU64::new(rem) {
            (large, small) = (small, new_small);
        } else {
            return small;
        }
    }
}

/// A collector for the GCD of the differences between the values and the minimal value.
#[derive(Default)]
pub struct GcdCollector {
    // We measure the GCD of the difference between the values and the minimal value.
    // This is the same as computing the difference between the values and the first value.
    //
    // This way, we can compress i64-converted-to-u64 (e.g. timestamp that were supplied in
    // seconds, only to be converted in nanoseconds).
    increment_gcd_opt: Option<(NonZeroU64, DividerU64)>,
    first_value_opt: Option<u64>,
}

impl GcdCollector {
    #[inline]
    fn update_increment_gcd(&mut self, value: u64) {
        let Some(first_value) = self.first_value_opt else {
            // We set the first value and just quit.
            self.first_value_opt = Some(value);
            return;
        };
        let Some(non_zero_value) = NonZeroU64::new(value.abs_diff(first_value)) else {
            // We can simply skip 0 values.
            return;
        };
        let Some((gcd, gcd_divider)) = self.increment_gcd_opt else {
            self.set_increment_gcd(non_zero_value);
            return;
        };
        if gcd.get() == 1 {
            // It won't see any update now.
            return;
        }
        let remainder =
            non_zero_value.get() - (gcd_divider.divide(non_zero_value.get())) * gcd.get();
        if remainder == 0 {
            return;
        }
        let new_gcd = compute_gcd(non_zero_value, gcd);
        self.set_increment_gcd(new_gcd);
    }

    fn set_increment_gcd(&mut self, gcd: NonZeroU64) {
        let new_divider = DividerU64::divide_by(gcd.get());
        self.increment_gcd_opt = Some((gcd, new_divider));
    }
}

impl ColumnStatsCollector<u64> for GcdCollector {
    type Stats = NonZeroU64;

    fn collect(&mut self, value: u64) {
        self.update_increment_gcd(value);
    }

    fn finalize(&self) -> Self::Stats {
        if let Some((gcd, _)) = self.increment_gcd_opt {
            gcd
        } else {
            NonZeroU64::new(1).unwrap()
        }
    }

    fn num_bytes(&self) -> u64 {
        8
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use crate::columnar::column_values::stats::{
        gcd::compute_gcd, ColumnStatsCollector, GcdCollector,
    };

    fn compute_stats(vals: impl Iterator<Item = u64>) -> NonZeroU64 {
        let mut stats_collector = GcdCollector::default();
        for val in vals {
            stats_collector.collect(val);
        }
        stats_collector.finalize()
    }

    fn find_gcd(vals: impl Iterator<Item = u64>) -> u64 {
        compute_stats(vals).get()
    }

    #[test]
    fn test_compute_gcd() {
        let test_compute_gcd_aux = |large, small, expected| {
            let large = NonZeroU64::new(large).unwrap();
            let small = NonZeroU64::new(small).unwrap();
            let expected = NonZeroU64::new(expected).unwrap();
            assert_eq!(compute_gcd(small, large), expected);
            assert_eq!(compute_gcd(large, small), expected);
        };
        test_compute_gcd_aux(1, 4, 1);
        test_compute_gcd_aux(2, 4, 2);
        test_compute_gcd_aux(10, 25, 5);
        test_compute_gcd_aux(25, 25, 25);
    }

    #[test]
    fn test_gcd() {
        assert_eq!(find_gcd([0].into_iter()), 1);
        assert_eq!(find_gcd([0, 10].into_iter()), 10);
        assert_eq!(find_gcd([10, 0].into_iter()), 10);
        assert_eq!(find_gcd([].into_iter()), 1);
        assert_eq!(find_gcd([15, 30, 5, 10].into_iter()), 5);
        assert_eq!(find_gcd([15, 16, 10].into_iter()), 1);
        assert_eq!(find_gcd([0, 5, 5, 5].into_iter()), 5);
        assert_eq!(find_gcd([0, 0].into_iter()), 1);
        assert_eq!(find_gcd([1, 10, 4, 1, 7, 10].into_iter()), 3);
        assert_eq!(find_gcd([1, 10, 0, 4, 1, 7, 10].into_iter()), 1);
    }

    #[test]
    fn test_stats() {
        assert_eq!(compute_stats([].into_iter()), NonZeroU64::new(1).unwrap());
        assert_eq!(
            compute_stats([0, 1].into_iter()),
            NonZeroU64::new(1).unwrap()
        );
        assert_eq!(
            compute_stats([10, 20, 30].into_iter()),
            NonZeroU64::new(10).unwrap()
        );
        assert_eq!(
            compute_stats([10, 50, 10, 30].into_iter()),
            NonZeroU64::new(20).unwrap()
        );
        assert_eq!(
            compute_stats([10, 0, 30].into_iter()),
            NonZeroU64::new(10).unwrap()
        );
    }
}
