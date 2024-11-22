mod bitpacked;
mod blockwise_linear;
mod line;
mod linear;
mod raw;

use std::io;
use std::io::Write;
use std::sync::Arc;

use crate::common::{BinarySerializable, OwnedBytes};

use super::{ColumnCodec, ColumnCodecEstimator, ColumnValues, MonotonicallyMappableToU64};
use crate::columnar::column_values::monotonic_map_column;
use crate::columnar::column_values::monotonic_mapping::{
    StrictlyMonotonicMappingInverter, StrictlyMonotonicMappingToInternal,
};
pub use crate::columnar::column_values::u64_based::bitpacked::BitpackedCodec;
pub use crate::columnar::column_values::u64_based::blockwise_linear::BlockwiseLinearCodec;
pub use crate::columnar::column_values::u64_based::linear::LinearCodec;
pub use crate::columnar::column_values::u64_based::raw::RawCodec;
use crate::columnar::iterable::Iterable;

/// Available codecs to use to encode the u64 (via [`MonotonicallyMappableToU64`]) converted data.
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy)]
#[repr(u8)]
pub enum CodecType {
    /// Bitpack all values in the value range. The number of bits is defined by the amplitude
    /// `column.max_value() - column.min_value()`
    Bitpacked = 0u8,
    /// Linear interpolation puts a line between the first and last value and then bitpacks the
    /// values by the offset from the line. The number of bits is defined by the max deviation from
    /// the line.
    Linear = 1u8,
    /// Same as [`CodecType::Linear`], but encodes in blocks of 512 elements.
    BlockwiseLinear = 2u8,
    /// Store the raw values without any compression.
    Raw = 3u8,
}

/// List of all available u64-base codecs.
pub const ALL_U64_CODEC_TYPES: [CodecType; 4] = [
    CodecType::Bitpacked,
    CodecType::Linear,
    CodecType::BlockwiseLinear,
    CodecType::Raw,
];

impl CodecType {
    fn to_code(self) -> u8 {
        self as u8
    }

    fn try_from_code(code: u8) -> Option<CodecType> {
        match code {
            0u8 => Some(CodecType::Bitpacked),
            1u8 => Some(CodecType::Linear),
            2u8 => Some(CodecType::BlockwiseLinear),
            3u8 => Some(CodecType::Raw),
            _ => None,
        }
    }

    fn load<T: MonotonicallyMappableToU64>(
        &self,
        bytes: OwnedBytes,
    ) -> io::Result<Arc<dyn ColumnValues<T>>> {
        match self {
            CodecType::Bitpacked => load_specific_codec::<BitpackedCodec, T>(bytes),
            CodecType::Linear => load_specific_codec::<LinearCodec, T>(bytes),
            CodecType::BlockwiseLinear => load_specific_codec::<BlockwiseLinearCodec, T>(bytes),
            CodecType::Raw => load_specific_codec::<RawCodec, T>(bytes),
        }
    }
}

fn load_specific_codec<C: ColumnCodec, T: MonotonicallyMappableToU64>(
    bytes: OwnedBytes,
) -> io::Result<Arc<dyn ColumnValues<T>>> {
    let reader = C::load(bytes)?;
    let reader_typed = monotonic_map_column(
        reader,
        StrictlyMonotonicMappingInverter::from(StrictlyMonotonicMappingToInternal::<T>::new()),
    );
    Ok(Arc::new(reader_typed))
}

impl CodecType {
    /// Returns a boxed codec estimator associated to a given `CodecType`.
    pub fn estimator(&self) -> Box<dyn ColumnCodecEstimator> {
        match self {
            CodecType::Bitpacked => BitpackedCodec::boxed_estimator(),
            CodecType::Linear => LinearCodec::boxed_estimator(),
            CodecType::BlockwiseLinear => BlockwiseLinearCodec::boxed_estimator(),
            CodecType::Raw => RawCodec::boxed_estimator(),
        }
    }
}

/// Serializes a given column of u64-mapped values.
pub fn serialize_u64_based_column_values<T: MonotonicallyMappableToU64>(
    vals: &dyn Iterable<T>,
    codec_types: &[CodecType],
    wrt: &mut dyn Write,
) -> io::Result<()> {
    let mut estimators: Vec<(CodecType, Box<dyn ColumnCodecEstimator>)> =
        Vec::with_capacity(codec_types.len());
    for &codec_type in codec_types {
        estimators.push((codec_type, codec_type.estimator()));
    }
    for val in vals.boxed_iter() {
        let val_u64 = val.to_u64();
        for (_, estimator) in &mut estimators {
            estimator.collect(val_u64);
        }
    }
    for (_, estimator) in &mut estimators {
        estimator.finalize();
    }
    let (_, best_codec, best_codec_estimator) = estimators
        .into_iter()
        .flat_map(|(codec_type, estimator)| {
            let num_bytes = estimator.estimate()?;
            Some((num_bytes, codec_type, estimator))
        })
        .min_by_key(|(num_bytes, _, _)| *num_bytes)
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "No available applicable codec.")
        })?;
    best_codec.to_code().serialize(wrt)?;
    best_codec_estimator.serialize(
        &mut vals.boxed_iter().map(MonotonicallyMappableToU64::to_u64),
        wrt,
    )?;
    Ok(())
}

/// Load u64-based column values.
///
/// This method first identifies the codec off the first byte.
pub fn load_u64_based_column_values<T: MonotonicallyMappableToU64>(
    mut bytes: OwnedBytes,
) -> io::Result<Arc<dyn ColumnValues<T>>> {
    let codec_type: CodecType = bytes
        .first()
        .copied()
        .and_then(CodecType::try_from_code)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Failed to read codec type"))?;
    bytes.advance(1);
    codec_type.load(bytes)
}

/// Helper function to serialize a column (autodetect from all codecs) and then open it
pub fn serialize_and_load_u64_based_column_values<T: MonotonicallyMappableToU64>(
    vals: &dyn Iterable,
    codec_types: &[CodecType],
) -> Arc<dyn ColumnValues<T>> {
    let mut buffer = Vec::new();
    serialize_u64_based_column_values(vals, codec_types, &mut buffer).unwrap();
    load_u64_based_column_values::<T>(OwnedBytes::new(buffer)).unwrap()
}

#[cfg(test)]
mod tests;
