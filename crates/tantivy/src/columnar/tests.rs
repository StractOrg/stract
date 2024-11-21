use super::dynamic_column::{DynamicColumn, DynamicColumnHandle};
use super::value::NumericalValue;
use super::{ColumnarReader, ColumnarWriter};

#[test]
fn test_dataframe_writer_bytes() {
    let mut dataframe_writer = ColumnarWriter::default();
    dataframe_writer.record_bytes(1u32, "my_string", b"hello");
    dataframe_writer.record_bytes(3u32, "my_string", b"helloeee");
    let mut buffer: Vec<u8> = Vec::new();
    dataframe_writer.serialize(5, None, &mut buffer).unwrap();
    let columnar = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar.num_columns(), 1);
    let cols: Vec<DynamicColumnHandle> = columnar.read_columns("my_string").unwrap();
    assert_eq!(cols.len(), 1);
    assert!(cols[0].num_bytes() > 0);
}

#[test]
fn test_dataframe_sort_by_full() {
    let mut dataframe_writer = ColumnarWriter::default();
    dataframe_writer.record_numerical(0u32, "value", NumericalValue::U64(1));
    dataframe_writer.record_numerical(1u32, "value", NumericalValue::U64(2));
    let data = dataframe_writer.sort_order("value", 2, false);
    assert_eq!(data, vec![0, 1]);
}

#[test]
fn test_dictionary_encoded_bytes() {
    let mut buffer = Vec::new();
    let mut columnar_writer = ColumnarWriter::default();
    columnar_writer.record_bytes(1, "my.column", b"a");
    columnar_writer.record_bytes(3, "my.column", b"c");
    columnar_writer.record_bytes(3, "my.column2", b"different_column!");
    columnar_writer.record_bytes(4, "my.column", b"b");
    columnar_writer.serialize(5, None, &mut buffer).unwrap();
    let columnar_reader = ColumnarReader::open(buffer).unwrap();
    assert_eq!(columnar_reader.num_columns(), 2);
    let col_handles = columnar_reader.read_columns("my.column").unwrap();
    assert_eq!(col_handles.len(), 1);
    let DynamicColumn::Bytes(bytes_col) = col_handles[0].open().unwrap() else {
        panic!();
    };
    let mut term_buffer = Vec::new();
    bytes_col
        .dictionary
        .ord_to_term(0u64, &mut term_buffer)
        .unwrap();
    assert_eq!(term_buffer, b"a");
    bytes_col
        .dictionary
        .ord_to_term(2u64, &mut term_buffer)
        .unwrap();
    assert_eq!(term_buffer, b"c");
    bytes_col
        .dictionary
        .ord_to_term(1u64, &mut term_buffer)
        .unwrap();
    assert_eq!(term_buffer, b"b");
}
