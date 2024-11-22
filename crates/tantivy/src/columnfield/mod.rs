//! Column oriented field storage for tantivy.
//!
//! It is the equivalent of `Lucene`'s `DocValues`.
//!
//! A columnar field is a column-oriented fashion storage for `tantivy`.
//!
//! It is designed for the fast random access of some document
//! fields given a document id.
//!
//! Columnar fields are useful when a field is required for all or most of
//! the `DocSet`: for instance for scoring, grouping, aggregation or filtering
//!
//!
//! Fields have to be declared as `COLUMN` in the schema.
//! Currently supported fields are: u64, i64, f64, bytes, ip and text.
//!
//! Columnar fields are stored in with [different codecs](columnar). The best codec is detected
//! automatically, when serializing.
//!
//! Read access performance is comparable to that of an array lookup.

pub use crate::columnar::Column;

pub use self::error::{ColumnFieldNotAvailableError, Result};
pub use self::readers::ColumnFieldReaders;
pub use self::writer::ColumnFieldsWriter;
use crate::schema::Type;
use crate::DateTime;

mod error;
mod readers;
mod writer;

/// Trait for types that are allowed for columnar fields:
/// (u64, u128, i64, f64, bool, DateTime).
pub trait ColumnarValue {
    /// Returns the `schema::Type` for this ColumnarValue.
    fn to_type() -> Type;
}

impl ColumnarValue for u64 {
    fn to_type() -> Type {
        Type::U64
    }
}

impl ColumnarValue for i64 {
    fn to_type() -> Type {
        Type::I64
    }
}

impl ColumnarValue for f64 {
    fn to_type() -> Type {
        Type::F64
    }
}

impl ColumnarValue for bool {
    fn to_type() -> Type {
        Type::Bool
    }
}
impl ColumnarValue for DateTime {
    fn to_type() -> Type {
        Type::Date
    }
}
impl ColumnarValue for u128 {
    fn to_type() -> Type {
        Type::U128
    }
}
#[cfg(test)]
mod tests {

    use std::ops::RangeInclusive;
    use std::path::Path;

    use crate::columnar::MonotonicallyMappableToU64;
    use crate::common::{ByteCount, DateTimePrecision, HasLen, TerminatingWrite};
    use rand::prelude::SliceRandom;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::sync::LazyLock;

    use super::*;
    use crate::directory::{Directory, RamDirectory, WritePtr};
    use crate::index::SegmentId;
    use crate::merge_policy::NoMergePolicy;
    use crate::schema::{
        DateOptions, Field, JsonObjectOptions, Schema, SchemaBuilder, TantivyDocument, COLUMN,
        INDEXED,
    };
    use crate::time::OffsetDateTime;
    use crate::{Index, IndexWriter, SegmentReader};

    pub static SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
        let mut schema_builder = Schema::builder();
        schema_builder.add_u64_field("field", COLUMN);
        schema_builder.build()
    });
    pub static FIELD: LazyLock<Field> = LazyLock::new(|| SCHEMA.get_field("field").unwrap());

    #[test]
    pub fn test_convert_i64_u64() {
        let datetime = DateTime::from_utc(OffsetDateTime::UNIX_EPOCH);
        assert_eq!(i64::from_u64(datetime.to_u64()), 0i64);
    }

    #[test]
    fn test_intcolumnfield_small() -> crate::Result<()> {
        let path = Path::new("test");
        let directory: RamDirectory = RamDirectory::create();
        {
            let mut write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut column_field_writers = ColumnFieldsWriter::from_schema(&SCHEMA).unwrap();
            column_field_writers
                .add_document(&doc!(*FIELD=>13u64))
                .unwrap();
            column_field_writers
                .add_document(&doc!(*FIELD=>14u64))
                .unwrap();
            column_field_writers
                .add_document(&doc!(*FIELD=>2u64))
                .unwrap();
            column_field_writers.serialize(&mut write, None).unwrap();
            write.terminate().unwrap();
        }
        let file = directory.open_read(path).unwrap();

        assert!(file.len() > 0);
        let column_field_readers = ColumnFieldReaders::open(file, SCHEMA.clone()).unwrap();
        let column = column_field_readers.u64("field").unwrap().values;
        assert_eq!(column.get_val(0), 13u64);
        assert_eq!(column.get_val(1), 14u64);
        assert_eq!(column.get_val(2), 2u64);
        Ok(())
    }

    #[test]
    fn test_intcolumnfield_large() {
        let path = Path::new("test");
        let directory: RamDirectory = RamDirectory::create();
        {
            let mut write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut column_field_writers = ColumnFieldsWriter::from_schema(&SCHEMA).unwrap();
            column_field_writers
                .add_document(&doc!(*FIELD=>4u64))
                .unwrap();
            column_field_writers
                .add_document(&doc!(*FIELD=>14_082_001u64))
                .unwrap();
            column_field_writers
                .add_document(&doc!(*FIELD=>3_052u64))
                .unwrap();
            column_field_writers
                .add_document(&doc!(*FIELD=>9_002u64))
                .unwrap();
            column_field_writers
                .add_document(&doc!(*FIELD=>15_001u64))
                .unwrap();
            column_field_writers
                .add_document(&doc!(*FIELD=>777u64))
                .unwrap();
            column_field_writers
                .add_document(&doc!(*FIELD=>1_002u64))
                .unwrap();
            column_field_writers
                .add_document(&doc!(*FIELD=>1_501u64))
                .unwrap();
            column_field_writers
                .add_document(&doc!(*FIELD=>215u64))
                .unwrap();
            column_field_writers.serialize(&mut write, None).unwrap();
            write.terminate().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert!(file.len() > 0);
        let column_field_readers = ColumnFieldReaders::open(file, SCHEMA.clone()).unwrap();
        let col = column_field_readers.u64("field").unwrap().values;
        assert_eq!(col.get_val(0), 4u64);
        assert_eq!(col.get_val(1), 14_082_001u64);
        assert_eq!(col.get_val(2), 3_052u64);
        assert_eq!(col.get_val(3), 9002u64);
        assert_eq!(col.get_val(4), 15_001u64);
        assert_eq!(col.get_val(5), 777u64);
        assert_eq!(col.get_val(6), 1_002u64);
        assert_eq!(col.get_val(7), 1_501u64);
        assert_eq!(col.get_val(8), 215u64);
    }

    #[test]
    fn test_intcolumnfield_null_amplitude() {
        let path = Path::new("test");
        let directory: RamDirectory = RamDirectory::create();
        {
            let mut write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut column_field_writers = ColumnFieldsWriter::from_schema(&SCHEMA).unwrap();
            for _ in 0..10_000 {
                column_field_writers
                    .add_document(&doc!(*FIELD=>100_000u64))
                    .unwrap();
            }
            column_field_writers.serialize(&mut write, None).unwrap();
            write.terminate().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert!(file.len() > 0);
        let column_field_readers = ColumnFieldReaders::open(file, SCHEMA.clone()).unwrap();
        let column_field_reader = column_field_readers.u64("field").unwrap().values;
        for doc in 0..10_000 {
            assert_eq!(column_field_reader.get_val(doc), 100_000u64);
        }
    }

    #[test]
    fn test_intcolumnfield_large_numbers() {
        let path = Path::new("test");
        let directory: RamDirectory = RamDirectory::create();

        {
            let mut write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut column_field_writers = ColumnFieldsWriter::from_schema(&SCHEMA).unwrap();
            // forcing the amplitude to be high
            column_field_writers
                .add_document(&doc!(*FIELD=>0u64))
                .unwrap();
            for doc_id in 1u64..10_000u64 {
                column_field_writers
                    .add_document(&doc!(*FIELD=>5_000_000_000_000_000_000u64 + doc_id))
                    .unwrap();
            }
            column_field_writers.serialize(&mut write, None).unwrap();
            write.terminate().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert!(file.len() > 0);
        {
            let column_field_readers = ColumnFieldReaders::open(file, SCHEMA.clone()).unwrap();
            let col = column_field_readers.u64("field").unwrap().values;
            for doc in 1..10_000 {
                assert_eq!(col.get_val(doc), 5_000_000_000_000_000_000u64 + doc as u64);
            }
        }
    }

    #[test]
    fn test_signed_intcolumnfield_normal() -> crate::Result<()> {
        let path = Path::new("test");
        let directory: RamDirectory = RamDirectory::create();
        let mut schema_builder = Schema::builder();

        let i64_field = schema_builder.add_i64_field("field", COLUMN);
        let schema = schema_builder.build();
        {
            let mut write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut column_field_writers = ColumnFieldsWriter::from_schema(&schema).unwrap();
            for i in -100i64..10_000i64 {
                let mut doc = TantivyDocument::default();
                doc.add_i64(i64_field, i);
                column_field_writers.add_document(&doc).unwrap();
            }
            column_field_writers.serialize(&mut write, None).unwrap();
            write.terminate().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert!(file.len() > 0);

        {
            let column_field_readers = ColumnFieldReaders::open(file, schema).unwrap();
            let col = column_field_readers.i64("field").unwrap().values;
            assert_eq!(col.min_value(), -100i64);
            assert_eq!(col.max_value(), 9_999i64);
            for (doc, i) in (-100i64..10_000i64).enumerate() {
                assert_eq!(col.get_val(doc as u32), i);
            }
            let mut buffer = vec![0i64; 100];
            col.get_range(53, &mut buffer[..]);
            for i in 0..100 {
                assert_eq!(buffer[i], -100i64 + 53i64 + i as i64);
            }
        }
        Ok(())
    }

    // Warning: this generates the same permutation at each call
    pub fn generate_permutation() -> Vec<u64> {
        let mut permutation: Vec<u64> = (0u64..100_000u64).collect();
        permutation.shuffle(&mut StdRng::from_seed([1u8; 32]));
        permutation
    }

    // Warning: this generates the same permutation at each call
    pub fn generate_permutation_gcd() -> Vec<u64> {
        let mut permutation: Vec<u64> = (1u64..100_000u64).map(|el| el * 1000).collect();
        permutation.shuffle(&mut StdRng::from_seed([1u8; 32]));
        permutation
    }

    fn test_intcolumnfield_permutation_with_data(permutation: Vec<u64>) {
        let path = Path::new("test");
        let n = permutation.len();
        let directory = RamDirectory::create();
        {
            let mut write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut column_field_writers = ColumnFieldsWriter::from_schema(&SCHEMA).unwrap();
            for &x in &permutation {
                column_field_writers.add_document(&doc!(*FIELD=>x)).unwrap();
            }
            column_field_writers.serialize(&mut write, None).unwrap();
            write.terminate().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        let column_field_readers = ColumnFieldReaders::open(file, SCHEMA.clone()).unwrap();
        let col = column_field_readers.u64("field").unwrap().values;
        for a in 0..n {
            assert_eq!(col.get_val(a as u32), permutation[a]);
        }
    }

    #[test]
    fn test_intcolumnfield_permutation_gcd() {
        let permutation = generate_permutation_gcd();
        test_intcolumnfield_permutation_with_data(permutation);
    }

    #[test]
    fn test_intcolumnfield_permutation() {
        let permutation = generate_permutation();
        test_intcolumnfield_permutation_with_data(permutation);
    }

    #[test]
    fn test_merge_missing_date_column_field() {
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_date_field("date", COLUMN);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        index_writer.set_merge_policy(Box::new(NoMergePolicy));
        index_writer
            .add_document(doc!(date_field => DateTime::from_utc(OffsetDateTime::now_utc())))
            .unwrap();
        index_writer.commit().unwrap();
        index_writer.add_document(doc!()).unwrap();
        index_writer.commit().unwrap();
        let reader = index.reader().unwrap();
        let segment_ids: Vec<SegmentId> = reader
            .searcher()
            .segment_readers()
            .iter()
            .map(SegmentReader::segment_id)
            .collect();
        assert_eq!(segment_ids.len(), 2);
        index_writer.merge(&segment_ids[..]).wait().unwrap();
        reader.reload().unwrap();
        assert_eq!(reader.searcher().segment_readers().len(), 1);
    }

    #[test]
    fn test_datecolumnfield() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_date_field(
            "date",
            DateOptions::from(COLUMN).set_precision(DateTimePrecision::Nanoseconds),
        );
        let multi_date_field = schema_builder.add_date_field(
            "multi_date",
            DateOptions::default()
                .set_precision(DateTimePrecision::Nanoseconds)
                .set_columnar(),
        );
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.set_merge_policy(Box::new(NoMergePolicy));
        index_writer.add_document(doc!(
            date_field => DateTime::from_u64(1i64.to_u64()),
            multi_date_field => DateTime::from_u64(2i64.to_u64()),
            multi_date_field => DateTime::from_u64(3i64.to_u64())
        ))?;
        index_writer.add_document(doc!(
            date_field => DateTime::from_u64(4i64.to_u64())
        ))?;
        index_writer.add_document(doc!(
            date_field => DateTime::from_u64(0i64.to_u64()),
            multi_date_field => DateTime::from_u64(5i64.to_u64()),
            multi_date_field => DateTime::from_u64(6i64.to_u64())
        ))?;
        index_writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let segment_reader = searcher.segment_reader(0);
        let column_fields = segment_reader.column_fields();
        let date_column_field = column_fields
            .column_opt::<DateTime>("date")
            .unwrap()
            .unwrap()
            .values;

        assert_eq!(date_column_field.get_val(0).into_timestamp_nanos(), 1i64);
        assert_eq!(date_column_field.get_val(1).into_timestamp_nanos(), 4i64);
        assert_eq!(date_column_field.get_val(2).into_timestamp_nanos(), 0i64);
        Ok(())
    }

    #[test]
    pub fn test_columnfield_bool_small() {
        let path = Path::new("test_bool");
        let directory: RamDirectory = RamDirectory::create();

        let mut schema_builder = Schema::builder();
        schema_builder.add_bool_field("field_bool", COLUMN);
        let schema = schema_builder.build();
        let field = schema.get_field("field_bool").unwrap();

        {
            let mut write: WritePtr = directory.open_write(path).unwrap();
            let mut column_field_writers = ColumnFieldsWriter::from_schema(&schema).unwrap();
            column_field_writers
                .add_document(&doc!(field=>true))
                .unwrap();
            column_field_writers
                .add_document(&doc!(field=>false))
                .unwrap();
            column_field_writers
                .add_document(&doc!(field=>true))
                .unwrap();
            column_field_writers
                .add_document(&doc!(field=>false))
                .unwrap();
            column_field_writers.serialize(&mut write, None).unwrap();
            write.terminate().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert!(file.len() > 0);
        let column_field_readers = ColumnFieldReaders::open(file, schema).unwrap();
        let bool_col = column_field_readers.bool("field_bool").unwrap();
        assert_eq!(bool_col.first(0), Some(true));
        assert_eq!(bool_col.first(1), Some(false));
        assert_eq!(bool_col.first(2), Some(true));
        assert_eq!(bool_col.first(3), Some(false));
    }

    #[test]
    pub fn test_columnfield_bool_large() {
        let path = Path::new("test_bool");
        let directory: RamDirectory = RamDirectory::create();

        let mut schema_builder = Schema::builder();
        schema_builder.add_bool_field("field_bool", COLUMN);
        let schema = schema_builder.build();
        let field = schema.get_field("field_bool").unwrap();

        {
            let mut write: WritePtr = directory.open_write(path).unwrap();
            let mut column_field_writers = ColumnFieldsWriter::from_schema(&schema).unwrap();
            for _ in 0..50 {
                column_field_writers
                    .add_document(&doc!(field=>true))
                    .unwrap();
                column_field_writers
                    .add_document(&doc!(field=>false))
                    .unwrap();
            }
            column_field_writers.serialize(&mut write, None).unwrap();
            write.terminate().unwrap();
        }
        let file = directory.open_read(path).unwrap();
        assert!(file.len() > 0);
        let readers = ColumnFieldReaders::open(file, schema).unwrap();
        let bool_col = readers.bool("field_bool").unwrap();
        for i in 0..25 {
            assert_eq!(bool_col.first(i * 2), Some(true));
            assert_eq!(bool_col.first(i * 2 + 1), Some(false));
        }
    }

    fn get_index(docs: &[crate::TantivyDocument], schema: &Schema) -> crate::Result<RamDirectory> {
        let directory: RamDirectory = RamDirectory::create();
        {
            let mut write: WritePtr = directory.open_write(Path::new("test")).unwrap();
            let mut column_field_writers = ColumnFieldsWriter::from_schema(schema).unwrap();
            for doc in docs {
                column_field_writers.add_document(doc).unwrap();
            }
            column_field_writers.serialize(&mut write, None).unwrap();
            write.terminate().unwrap();
        }
        Ok(directory)
    }

    #[test]
    #[ignore]
    pub fn test_gcd_date() {
        let size_prec_sec = test_gcd_date_with_codec(DateTimePrecision::Seconds);
        assert!((1000 * 13 / 8..100 + 1000 * 13 / 8).contains(&size_prec_sec.get_bytes())); // 13 bits per val = ceil(log_2(number of seconds in 2hours);
        let size_prec_micros = test_gcd_date_with_codec(DateTimePrecision::Microseconds);
        assert!((1000 * 33 / 8..100 + 1000 * 33 / 8).contains(&size_prec_micros.get_bytes()));
        // 33 bits per
        // val = ceil(log_2(number
        // of microsecsseconds
        // in 2hours);
    }

    fn test_gcd_date_with_codec(precision: DateTimePrecision) -> ByteCount {
        let mut rng = StdRng::seed_from_u64(2u64);
        const T0: i64 = 1_662_345_825_012_529i64;
        const ONE_HOUR_IN_MICROSECS: i64 = 3_600 * 1_000_000;
        let times: Vec<DateTime> = std::iter::repeat_with(|| {
            // +- One hour.
            let t = T0 + rng.gen_range(-ONE_HOUR_IN_MICROSECS..ONE_HOUR_IN_MICROSECS);
            DateTime::from_timestamp_micros(t)
        })
        .take(1_000)
        .collect();
        let date_options = DateOptions::default()
            .set_columnar()
            .set_precision(precision);
        let mut schema_builder = SchemaBuilder::default();
        let field = schema_builder.add_date_field("field", date_options);
        let schema = schema_builder.build();

        let docs: Vec<TantivyDocument> = times.iter().map(|time| doc!(field=>*time)).collect();

        let directory = get_index(&docs[..], &schema).unwrap();
        let path = Path::new("test");
        let file = directory.open_read(path).unwrap();
        let readers = ColumnFieldReaders::open(file, schema).unwrap();
        let col = readers.date("field").unwrap();

        for (i, time) in times.iter().enumerate() {
            let dt: DateTime = col.first(i as u32).unwrap();
            assert_eq!(dt, time.truncate(precision));
        }
        readers.column_num_bytes("field").unwrap()
    }

    #[test]
    fn test_gcd_bug_regression_1757() {
        let mut schema_builder = Schema::builder();
        let num_field = schema_builder.add_u64_field("url_norm_hash", COLUMN | INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut writer = index.writer_for_tests().unwrap();
            writer
                .add_document(doc! {
                    num_field => 100u64,
                })
                .unwrap();
            writer
                .add_document(doc! {
                    num_field => 200u64,
                })
                .unwrap();
            writer
                .add_document(doc! {
                    num_field => 300u64,
                })
                .unwrap();

            writer.commit().unwrap();
        }

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment = &searcher.segment_readers()[0];
        let field = segment.column_fields().u64("url_norm_hash").unwrap().values;

        let numbers = [100, 200, 300];
        let test_range = |range: RangeInclusive<u64>| {
            let expected_count = numbers.iter().filter(|num| range.contains(num)).count();
            let mut vec = vec![];
            field.get_row_ids_for_value_range(range, 0..u32::MAX, &mut vec);
            assert_eq!(vec.len(), expected_count);
        };
        test_range(50..=50);
        test_range(150..=150);
        test_range(350..=350);
        test_range(100..=250);
        test_range(101..=200);
        test_range(101..=199);
        test_range(100..=300);
        test_range(100..=299);
    }

    #[test]
    fn test_mapping_bug_docids_for_value_range() {
        let mut schema_builder = Schema::builder();
        let num_field = schema_builder.add_u64_field("url_norm_hash", COLUMN | INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            // Values without gcd, but with min_value
            let mut writer = index.writer_for_tests().unwrap();
            writer
                .add_document(doc! {
                    num_field => 1000u64,
                })
                .unwrap();
            writer
                .add_document(doc! {
                    num_field => 1001u64,
                })
                .unwrap();
            writer
                .add_document(doc! {
                    num_field => 1003u64,
                })
                .unwrap();
            writer.commit().unwrap();
        }

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment = &searcher.segment_readers()[0];
        let field = segment.column_fields().u64("url_norm_hash").unwrap().values;

        let numbers = [1000, 1001, 1003];
        let test_range = |range: RangeInclusive<u64>| {
            let expexted_count = numbers.iter().filter(|num| range.contains(num)).count();
            let mut vec = vec![];
            field.get_row_ids_for_value_range(range, 0..u32::MAX, &mut vec);
            assert_eq!(vec.len(), expexted_count);
        };
        let test_range_variant = |start, stop| {
            let start_range = start..=stop;
            test_range(start_range);
            let start_range = start..=(stop - 1);
            test_range(start_range);
            let start_range = start..=(stop + 1);
            test_range(start_range);
            let start_range = (start - 1)..=stop;
            test_range(start_range);
            let start_range = (start - 1)..=(stop - 1);
            test_range(start_range);
            let start_range = (start - 1)..=(stop + 1);
            test_range(start_range);
            let start_range = (start + 1)..=stop;
            test_range(start_range);
            let start_range = (start + 1)..=(stop - 1);
            test_range(start_range);
            let start_range = (start + 1)..=(stop + 1);
            test_range(start_range);
        };
        test_range_variant(50, 50);
        test_range_variant(1000, 1000);
        test_range_variant(1000, 1002);
    }

    #[test]
    fn test_column_field_in_json_field_expand_dots_disabled() {
        let mut schema_builder = Schema::builder();
        let json_option = JsonObjectOptions::default().set_columnar(None);
        let json = schema_builder.add_json_field("json", json_option);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        index_writer
            .add_document(doc!(json => json!({"attr.age": 32})))
            .unwrap();
        index_writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let column_field_reader = searcher.segment_reader(0u32).column_fields();
        assert!(column_field_reader
            .column_opt::<i64>("json.attr.age")
            .unwrap()
            .is_none());
        let column = column_field_reader
            .column_opt::<i64>(r"json.attr\.age")
            .unwrap()
            .unwrap();
        assert_eq!(column.first(0u32), Some(32))
    }

    #[test]
    fn test_column_field_in_json_field_expand_dots_enabled() {
        let mut schema_builder = Schema::builder();
        let json_option = JsonObjectOptions::default()
            .set_columnar(None)
            .set_expand_dots_enabled();
        let json = schema_builder.add_json_field("json", json_option);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        index_writer
            .add_document(doc!(json => json!({"attr.age": 32})))
            .unwrap();
        index_writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let column_field_reader = searcher.segment_reader(0u32).column_fields();
        for test_column_name in &["json.attr.age", "json.attr\\.age"] {
            let column = column_field_reader
                .column_opt::<i64>(test_column_name)
                .unwrap()
                .unwrap();
            assert_eq!(column.first(0u32), Some(32))
        }
    }

    #[test]
    fn test_column_field_dot_in_schema_field_name() {
        let mut schema_builder = Schema::builder();
        let field_with_dot = schema_builder.add_i64_field("field.with.dot", COLUMN);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        index_writer
            .add_document(doc!(field_with_dot => 32i64))
            .unwrap();
        index_writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let column_field_reader = searcher.segment_reader(0u32).column_fields();
        let column = column_field_reader
            .column_opt::<i64>("field.with.dot")
            .unwrap()
            .unwrap();
        assert_eq!(column.first(0u32), Some(32));
    }

    #[test]
    fn test_shadowing_column_field() {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("jsonfield", COLUMN);
        let shadowing_json_field = schema_builder.add_json_field("jsonfield.attr", COLUMN);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        index_writer
            .add_document(doc!(json_field=> json!({"attr": {"age": 32}}), shadowing_json_field=>json!({"age": 33})))
            .unwrap();
        index_writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let column_field_reader = searcher.segment_reader(0u32).column_fields();
        let column = column_field_reader
            .column_opt::<i64>("jsonfield.attr.age")
            .unwrap()
            .unwrap();
        assert_eq!(column.first(0u32), Some(33))
    }

    #[test]
    fn test_shadowing_column_field_with_expand_dots() {
        let mut schema_builder = Schema::builder();
        let json_option = JsonObjectOptions::default()
            .set_columnar(None)
            .set_expand_dots_enabled();
        let json_field = schema_builder.add_json_field("jsonfield", json_option.clone());
        let shadowing_json_field = schema_builder.add_json_field("jsonfield.attr", json_option);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        index_writer
            .add_document(doc!(json_field=> json!({"attr.age": 32}), shadowing_json_field=>json!({"age": 33})))
            .unwrap();
        index_writer.commit().unwrap();
        let searcher = index.reader().unwrap().searcher();
        let column_field_reader = searcher.segment_reader(0u32).column_fields();
        // Supported for now, maybe dropped in the future.
        let column = column_field_reader
            .column_opt::<i64>("jsonfield.attr.age")
            .unwrap()
            .unwrap();
        assert_eq!(column.first(0u32), Some(33));
        let column = column_field_reader
            .column_opt::<i64>("jsonfield\\.attr.age")
            .unwrap()
            .unwrap();
        assert_eq!(column.first(0u32), Some(33));
    }
}
