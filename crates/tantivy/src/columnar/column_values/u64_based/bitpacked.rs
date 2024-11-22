use std::io::{self, Write};
use std::num::NonZeroU64;
use std::ops::{Range, RangeInclusive};

use crate::bitpacker::{compute_num_bits, BitPacker, BitUnpacker};
use crate::columnar::column_values::stats::{
    ColumnStatsCollector, GcdCollector, MinMaxCollector, NumRowsCollector,
};
use crate::common::{BinarySerializable, OwnedBytes};
use fastdivide::DividerU64;

use crate::columnar::column_values::u64_based::{ColumnCodec, ColumnCodecEstimator};
use crate::columnar::{ColumnValues, RowId};

/// Depending on the field type, a different
/// columnar field is required.
#[derive(Clone)]
pub struct BitpackedReader {
    data: OwnedBytes,
    bit_unpacker: BitUnpacker,
    num_rows: RowId,
    min_value: u64,
    max_value: u64,
    gcd: NonZeroU64,
}

#[inline(always)]
const fn div_ceil(n: u64, q: NonZeroU64) -> u64 {
    // copied from unstable rust standard library.
    let d = n / q.get();
    let r = n % q.get();
    if r > 0 {
        d + 1
    } else {
        d
    }
}

// The bitpacked codec applies a linear transformation `f` over data that are bitpacked.
// f is defined by:
// f: bitpacked -> stats.min_value + stats.gcd * bitpacked
//
// In order to run range queries, we invert the transformation.
// `transform_range_before_linear_transformation` returns the range of values
// [min_bipacked_value..max_bitpacked_value] such that
// f(bitpacked) ∈ [min_value, max_value] <=> bitpacked ∈ [min_bitpacked_value, max_bitpacked_value]
fn transform_range_before_linear_transformation(
    min_value: u64,
    max_value: u64,
    gcd: NonZeroU64,
    range: RangeInclusive<u64>,
) -> Option<RangeInclusive<u64>> {
    if range.is_empty() {
        return None;
    }
    if min_value > *range.end() {
        return None;
    }
    if max_value < *range.start() {
        return None;
    }
    let shifted_range =
        range.start().saturating_sub(min_value)..=range.end().saturating_sub(min_value);
    let start_before_gcd_multiplication: u64 = div_ceil(*shifted_range.start(), gcd);
    let end_before_gcd_multiplication: u64 = *shifted_range.end() / gcd;
    Some(start_before_gcd_multiplication..=end_before_gcd_multiplication)
}

impl ColumnValues for BitpackedReader {
    #[inline(always)]
    fn get_val(&self, doc: u32) -> u64 {
        self.min_value + self.gcd.get() * self.bit_unpacker.get(doc, &self.data)
    }
    #[inline]
    fn min_value(&self) -> u64 {
        self.min_value
    }
    #[inline]
    fn max_value(&self) -> u64 {
        self.max_value
    }
    #[inline]
    fn num_vals(&self) -> RowId {
        self.num_rows
    }

    fn get_row_ids_for_value_range(
        &self,
        range: RangeInclusive<u64>,
        mut doc_id_range: Range<u32>,
        positions: &mut Vec<u32>,
    ) {
        let Some(transformed_range) = transform_range_before_linear_transformation(
            self.min_value,
            self.max_value,
            self.gcd,
            range,
        ) else {
            positions.clear();
            return;
        };
        doc_id_range.end = doc_id_range.end.min(self.num_vals());

        self.bit_unpacker.get_ids_for_value_range(
            transformed_range,
            doc_id_range,
            &self.data,
            positions,
        );
    }
}

fn num_bits(min_value: u64, max_value: u64, gcd: NonZeroU64) -> u8 {
    compute_num_bits((max_value - min_value) / gcd)
}

#[derive(Default)]
pub struct BitpackedCodecEstimator {
    num_rows_collector: NumRowsCollector,
    min_max_collector: MinMaxCollector,
    gcd_collector: GcdCollector,
}

impl ColumnCodecEstimator for BitpackedCodecEstimator {
    fn collect(&mut self, value: u64) {
        self.num_rows_collector.collect(value);
        self.min_max_collector.collect(value);
        self.gcd_collector.collect(value);
    }

    fn estimate(&self) -> Option<u64> {
        let (min_value, max_value) = self.min_max_collector.finalize();
        let gcd = self.gcd_collector.finalize();
        let num_bits_per_value = num_bits(min_value, max_value, gcd);

        Some(
            (self.num_rows_collector.as_u64().num_bytes()
                + self.min_max_collector.num_bytes()
                + self.gcd_collector.num_bytes())
                + (self.num_rows_collector.as_u64().finalize() as u64
                    * (num_bits_per_value as u64)
                    + 7)
                    / 8,
        )
    }

    fn serialize(
        &self,
        vals: &mut dyn Iterator<Item = u64>,
        wrt: &mut dyn Write,
    ) -> io::Result<()> {
        let (min_value, max_value) = self.min_max_collector.finalize();
        let gcd = self.gcd_collector.finalize();
        let num_rows = self.num_rows_collector.as_u64().finalize();

        num_rows.serialize(wrt)?;
        min_value.serialize(wrt)?;
        max_value.serialize(wrt)?;
        gcd.serialize(wrt)?;

        let num_bits = num_bits(min_value, max_value, gcd);
        let mut bit_packer = BitPacker::new();
        let divider = DividerU64::divide_by(gcd.get());
        for val in vals {
            bit_packer.write(divider.divide(val - min_value), num_bits, wrt)?;
        }
        bit_packer.close(wrt)?;
        Ok(())
    }
}

pub struct BitpackedCodec;

impl ColumnCodec for BitpackedCodec {
    type ColumnValues = BitpackedReader;
    type Estimator = BitpackedCodecEstimator;

    /// Opens a columnar field given a file.
    fn load(mut data: OwnedBytes) -> io::Result<Self::ColumnValues> {
        let num_rows = RowId::deserialize(&mut data)?;
        let min_value = u64::deserialize(&mut data)?;
        let max_value = u64::deserialize(&mut data)?;
        let gcd = NonZeroU64::deserialize(&mut data)?;

        let num_bits = num_bits(min_value, max_value, gcd);
        let bit_unpacker = BitUnpacker::new(num_bits);
        Ok(BitpackedReader {
            data,
            bit_unpacker,
            num_rows,
            min_value,
            max_value,
            gcd,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::column_values::u64_based::tests::create_and_validate;

    #[test]
    fn test_with_codec_data_sets_simple() {
        create_and_validate::<BitpackedCodec>(&[4, 3, 12], "name");
    }

    #[test]
    fn test_with_codec_data_sets_simple_gcd() {
        create_and_validate::<BitpackedCodec>(&[1000, 2000, 3000], "name");
    }

    #[test]
    fn test_with_codec_data_sets() {
        let data_sets = crate::columnar::column_values::u64_based::tests::get_codec_test_datasets();
        for (mut data, name) in data_sets {
            create_and_validate::<BitpackedCodec>(&data, name);
            data.reverse();
            create_and_validate::<BitpackedCodec>(&data, name);
        }
    }

    #[test]
    fn bitpacked_column_field_rand() {
        for _ in 0..500 {
            let mut data = (0..1 + rand::random::<u8>() as usize)
                .map(|_| rand::random::<i64>() as u64 / 2)
                .collect::<Vec<_>>();
            create_and_validate::<BitpackedCodec>(&data, "rand");
            data.reverse();
            create_and_validate::<BitpackedCodec>(&data, "rand");
        }
    }
}
