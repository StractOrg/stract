use std::io;
use std::io::Write;
use std::sync::Arc;

use crate::common::OwnedBytes;
use crate::sstable::Dictionary;

use crate::columnar::column::{BytesColumn, Column};
use crate::columnar::column_index::{serialize_column_index, SerializableColumnIndex};
use crate::columnar::column_values::{
    load_u128_based_column_values, load_u64_based_column_values,
    serialize_u128_based_column_values, serialize_u64_based_column_values,
    MonotonicallyMappableToU128, MonotonicallyMappableToU64, U128CodecType, U64CodecType,
};
use crate::columnar::iterable::Iterable;

pub fn serialize_column_mappable_to_u64<T: MonotonicallyMappableToU64>(
    column_index: SerializableColumnIndex,
    column_values: &impl Iterable<T>,
    output: &mut impl Write,
) -> io::Result<()> {
    let column_index_num_bytes = serialize_column_index(column_index, output)?;
    serialize_u64_based_column_values(column_values, &[U64CodecType::Raw], output)?;
    output.write_all(&column_index_num_bytes.to_le_bytes())?;
    Ok(())
}

pub fn open_column_u64<T: MonotonicallyMappableToU64>(bytes: OwnedBytes) -> io::Result<Column<T>> {
    let (body, column_index_num_bytes_payload) = bytes.rsplit(4);
    let column_index_num_bytes = u32::from_le_bytes(
        column_index_num_bytes_payload
            .as_slice()
            .try_into()
            .unwrap(),
    );
    let (column_index_data, column_values_data) = body.split(column_index_num_bytes as usize);
    let column_index = crate::columnar::column_index::open_column_index(column_index_data)?;
    let column_values = load_u64_based_column_values(column_values_data)?;
    Ok(Column {
        index: column_index,
        values: column_values,
    })
}

pub fn serialize_column_mappable_to_u128<T: MonotonicallyMappableToU128>(
    column_index: SerializableColumnIndex,
    column_values: &impl Iterable<T>,
    output: &mut impl Write,
) -> io::Result<()> {
    let column_index_num_bytes = serialize_column_index(column_index, output)?;
    serialize_u128_based_column_values(column_values, &[U128CodecType::Raw], output)?;
    output.write_all(&column_index_num_bytes.to_le_bytes())?;
    Ok(())
}

pub fn open_column_u128<T: MonotonicallyMappableToU128>(
    bytes: OwnedBytes,
) -> io::Result<Column<T>> {
    let (body, column_index_num_bytes_payload) = bytes.rsplit(4);
    let column_index_num_bytes = u32::from_le_bytes(
        column_index_num_bytes_payload
            .as_slice()
            .try_into()
            .unwrap(),
    );
    let (column_index_data, column_values_data) = body.split(column_index_num_bytes as usize);
    let column_index = crate::columnar::column_index::open_column_index(column_index_data)?;
    let column_values = load_u128_based_column_values(column_values_data)?;
    Ok(Column {
        index: column_index,
        values: column_values,
    })
}

pub fn open_column_bytes(data: OwnedBytes) -> io::Result<BytesColumn> {
    let (body, dictionary_len_bytes) = data.rsplit(8);
    let dictionary_len = u64::from_le_bytes(dictionary_len_bytes.as_slice().try_into().unwrap());
    let (dictionary_bytes, column_bytes) = body.split(dictionary_len as usize);
    let dictionary = Arc::new(Dictionary::from_bytes(dictionary_bytes)?);
    let term_ord_column = crate::columnar::column::open_column_u64::<u64>(column_bytes)?;
    Ok(BytesColumn {
        dictionary,
        term_ord_column,
    })
}
