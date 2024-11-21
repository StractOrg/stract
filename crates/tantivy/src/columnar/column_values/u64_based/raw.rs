use std::io::Write;

use ownedbytes::OwnedBytes;

use crate::{
    columnar::{column_values::ColumnStats, ColumnValues},
    common::BinarySerializable,
};

use super::{ColumnCodec, ColumnCodecEstimator};

pub struct RawCodec;

impl ColumnCodec for RawCodec {
    type ColumnValues = RawReader;
    type Estimator = RawCodecEstimator;

    fn load(bytes: OwnedBytes) -> std::io::Result<Self::ColumnValues> {
        RawReader::load(bytes)
    }
}

pub struct RawReader {
    min_value: u64,
    max_value: u64,
    num_rows: u32,
    data: OwnedBytes,
}

impl RawReader {
    fn load(bytes: OwnedBytes) -> std::io::Result<Self> {
        let mut bytes = bytes;

        let num_rows = u32::deserialize(&mut bytes)?;
        let min_value = u64::deserialize(&mut bytes)?;
        let max_value = u64::deserialize(&mut bytes)?;

        let data = bytes;

        Ok(Self {
            min_value,
            max_value,
            num_rows,
            data,
        })
    }
}

impl ColumnValues for RawReader {
    fn get_val(&self, idx: u32) -> u64 {
        let idx = idx as usize;
        let mut bytes = &self.data[idx * 8..(idx + 1) * 8];
        u64::deserialize(&mut bytes).unwrap()
    }

    fn min_value(&self) -> u64 {
        self.min_value
    }

    fn max_value(&self) -> u64 {
        self.max_value
    }

    fn num_vals(&self) -> u32 {
        self.num_rows
    }
}

#[derive(Default)]
pub struct RawCodecEstimator;

impl ColumnCodecEstimator for RawCodecEstimator {
    fn collect(&mut self, _: u64) {}

    fn estimate(&self, stats: &ColumnStats) -> Option<u64> {
        Some(stats.num_rows as u64 * 8)
    }

    fn serialize(
        &self,
        stats: &ColumnStats,
        vals: &mut dyn Iterator<Item = u64>,
        wrt: &mut dyn Write,
    ) -> std::io::Result<()> {
        stats.num_rows.serialize(wrt)?;
        stats.min_value.serialize(wrt)?;
        stats.max_value.serialize(wrt)?;

        for val in vals {
            val.serialize(wrt)?;
        }

        wrt.flush()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::column_values::u64_based::tests::create_and_validate;

    #[test]
    fn test_with_codec_data_sets_simple() {
        create_and_validate::<RawCodec>(&[4, 3, 12], "name");
    }

    #[test]
    fn test_with_codec_data_sets() {
        let data_sets = crate::columnar::column_values::u64_based::tests::get_codec_test_datasets();
        for (mut data, name) in data_sets {
            create_and_validate::<RawCodec>(&data, name);
            data.reverse();
            create_and_validate::<RawCodec>(&data, name);
        }
    }

    #[test]
    fn test_column_field_rand() {
        for _ in 0..500 {
            let mut data = (0..1 + rand::random::<u8>() as usize)
                .map(|_| rand::random::<i64>() as u64 / 2)
                .collect::<Vec<_>>();
            create_and_validate::<RawCodec>(&data, "rand");
            data.reverse();
            create_and_validate::<RawCodec>(&data, "rand");
        }
    }
}
