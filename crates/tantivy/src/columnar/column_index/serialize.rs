use std::io;
use std::io::Write;

use crate::common::{CountingWriter, OwnedBytes};

use super::Cardinality;
use crate::columnar::column_index::ColumnIndex;

pub enum SerializableColumnIndex {
    Full,
}

impl SerializableColumnIndex {
    pub fn get_cardinality(&self) -> Cardinality {
        match self {
            SerializableColumnIndex::Full => Cardinality::Full,
        }
    }
}

/// Serialize a column index.
pub fn serialize_column_index(
    column_index: SerializableColumnIndex,
    output: &mut impl Write,
) -> io::Result<u32> {
    let mut output = CountingWriter::wrap(output);
    let cardinality = column_index.get_cardinality().to_code();
    output.write_all(&[cardinality])?;
    match column_index {
        SerializableColumnIndex::Full => {}
    }
    let column_index_num_bytes = output.written_bytes() as u32;
    Ok(column_index_num_bytes)
}

/// Open a serialized column index.
pub fn open_column_index(mut bytes: OwnedBytes) -> io::Result<ColumnIndex> {
    if bytes.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "Failed to deserialize column index. Empty buffer.",
        ));
    }
    let cardinality_code = bytes[0];
    let cardinality = Cardinality::try_from_code(cardinality_code)?;
    bytes.advance(1);
    match cardinality {
        Cardinality::Full => Ok(ColumnIndex::Full),
    }
}

// TODO unit tests
