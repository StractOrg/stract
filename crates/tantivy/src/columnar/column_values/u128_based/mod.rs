mod raw;

pub use crate::columnar::column_values::u128_based::raw::RawCodec;
use crate::{columnar::iterable::Iterable, common::BinarySerializable};
use std::{
    io::{self, Write},
    sync::Arc,
};

use ownedbytes::OwnedBytes;

use super::{
    monotonic_map_column,
    monotonic_mapping::{StrictlyMonotonicMappingInverter, StrictlyMonotonicMappingToInternal},
    ColumnCodec, ColumnCodecEstimator, ColumnValues, MonotonicallyMappableToU128,
};

/// Available codecs to use to encode the u128 (via [`MonotonicallyMappableToU128`]) converted data.
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy)]
#[repr(u8)]
pub enum CodecType {
    /// Store the raw values without any compression.
    Raw = 0u8,
}

impl CodecType {
    fn to_code(self) -> u8 {
        self as u8
    }

    fn try_from_code(code: u8) -> Option<CodecType> {
        match code {
            0u8 => Some(CodecType::Raw),
            _ => None,
        }
    }

    fn load<T: MonotonicallyMappableToU128>(
        &self,
        bytes: OwnedBytes,
    ) -> io::Result<Arc<dyn ColumnValues<T>>> {
        match self {
            CodecType::Raw => load_specific_codec::<RawCodec, T>(bytes),
        }
    }

    /// Returns a boxed codec estimator associated to a given `CodecType`.
    pub fn estimator(&self) -> Box<dyn ColumnCodecEstimator<u128>> {
        match self {
            CodecType::Raw => RawCodec::boxed_estimator(),
        }
    }
}

/// List of all available u128-base codecs.
pub const ALL_U128_CODEC_TYPES: [CodecType; 1] = [CodecType::Raw];

fn load_specific_codec<C: ColumnCodec<u128>, T: MonotonicallyMappableToU128>(
    bytes: OwnedBytes,
) -> io::Result<Arc<dyn ColumnValues<T>>> {
    let reader = C::load(bytes)?;
    let reader_typed = monotonic_map_column(
        reader,
        StrictlyMonotonicMappingInverter::from(StrictlyMonotonicMappingToInternal::<T>::new()),
    );
    Ok(Arc::new(reader_typed))
}

/// Serializes a given column of u128-mapped values.
pub fn serialize_u128_based_column_values<T: MonotonicallyMappableToU128>(
    vals: &dyn Iterable<T>,
    codec_types: &[CodecType],
    wrt: &mut dyn Write,
) -> io::Result<()> {
    let mut estimators: Vec<(CodecType, Box<dyn ColumnCodecEstimator<u128>>)> =
        Vec::with_capacity(codec_types.len());
    for &codec_type in codec_types {
        estimators.push((codec_type, codec_type.estimator()));
    }
    for val in vals.boxed_iter() {
        let val_u128 = val.to_u128();
        for (_, estimator) in &mut estimators {
            estimator.collect(val_u128);
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
        &mut vals.boxed_iter().map(MonotonicallyMappableToU128::to_u128),
        wrt,
    )?;
    Ok(())
}

/// Load u64-based column values.
///
/// This method first identifies the codec off the first byte.
pub fn load_u128_based_column_values<T: MonotonicallyMappableToU128>(
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
pub fn serialize_and_load_u128_based_column_values<T: MonotonicallyMappableToU128>(
    vals: &dyn Iterable<T>,
    codec_types: &[CodecType],
) -> Arc<dyn ColumnValues<T>> {
    let mut buffer = Vec::new();
    serialize_u128_based_column_values(vals, codec_types, &mut buffer).unwrap();
    load_u128_based_column_values::<T>(OwnedBytes::new(buffer)).unwrap()
}

#[cfg(test)]
mod tests;
