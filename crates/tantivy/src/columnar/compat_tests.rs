use std::path::PathBuf;

use super::{Column, ColumnarReader, DynamicColumn, CURRENT_VERSION};

const NUM_DOCS: u32 = u16::MAX as u32;

fn generate_columnar(num_docs: u32) -> Vec<u8> {
    use super::ColumnarWriter;

    let mut columnar_writer = ColumnarWriter::default();

    for i in 0..num_docs {
        if i % 100 == 0 {
            columnar_writer.record_numerical(i, "sparse", i as u64);
        }
        if i % 2 == 0 {
            columnar_writer.record_numerical(i, "dense", i as u64);
        }
        columnar_writer.record_numerical(i, "full", i as u64);
        columnar_writer.record_numerical(i, "multi", i as u64);
        columnar_writer.record_numerical(i, "multi", i as u64);
    }

    let mut wrt: Vec<u8> = Vec::new();
    columnar_writer.serialize(num_docs, None, &mut wrt).unwrap();

    wrt
}

fn path_for_version(version: &str) -> String {
    format!("./compat_tests_data/{}.columnar", version)
}

fn open_column(reader: &ColumnarReader, name: &str) -> Column<u64> {
    let column = reader.read_columns(name).unwrap()[0]
        .open()
        .unwrap()
        .coerce_numerical(super::NumericalType::U64)
        .unwrap();
    let DynamicColumn::U64(column) = column else {
        panic!();
    };
    column
}
