mod min_max;
pub use min_max::MinMaxCollector;

mod gcd;
pub use gcd::GcdCollector;

mod num_rows;
pub use num_rows::NumRowsCollector;

use crate::common::BinarySerializable;

/// Trait for collecting statistics about a column of values.
///
/// This trait is used to gather metadata about a column during its construction,
/// such as minimum/maximum values, GCD, and number of rows. The statistics are
/// collected incrementally as values are added to the column.
///
/// The generic parameter T represents the type of values in the column, while
/// the associated type Stats represents the final statistics produced.
///
/// Statistics are typically used to:
/// - Optimize storage and compression of column values
/// - Enable efficient range queries and filtering
/// - Provide metadata about the column contents
pub trait ColumnStatsCollector<T> {
    type Stats: BinarySerializable;

    /// Record a value from the column.
    fn collect(&mut self, value: T);

    /// Finalize the collection process and return the statistics.
    fn finalize(&self) -> Self::Stats;

    /// Number of bytes that will be written to the columnar file.
    fn num_bytes(&self) -> u64;
}
