use super::*;
use crate::columnar::{ColumnarWriter, HasAssociatedColumnType, RowId};

fn make_columnar<T: Into<NumericalValue> + HasAssociatedColumnType + Copy>(
    column_name: &str,
    vals: &[T],
) -> ColumnarReader {
    let mut dataframe_writer = ColumnarWriter::default();
    dataframe_writer.record_column_type(column_name, T::column_type(), false);
    for (row_id, val) in vals.iter().copied().enumerate() {
        dataframe_writer.record_numerical(row_id as RowId, column_name, val.into());
    }
    let mut buffer: Vec<u8> = Vec::new();
    dataframe_writer
        .serialize(vals.len() as RowId, None, &mut buffer)
        .unwrap();
    ColumnarReader::open(buffer).unwrap()
}

#[test]
fn test_column_coercion_to_u64() {
    // i64 type
    let columnar1 = make_columnar("numbers", &[1i64]);
    // u64 type
    let columnar2 = make_columnar("numbers", &[u64::MAX]);
    let columnars = &[&columnar1, &columnar2];
    let merge_order = StackMergeOrder::stack(columnars).into();
    let column_map: BTreeMap<(String, ColumnTypeCategory), GroupedColumnsHandle> =
        group_columns_for_merge(columnars, &[], &merge_order).unwrap();
    assert_eq!(column_map.len(), 1);
    assert!(column_map.contains_key(&("numbers".to_string(), ColumnTypeCategory::Numerical)));
}

#[test]
fn test_column_coercion_to_i64() {
    let columnar1 = make_columnar("numbers", &[-1i64]);
    let columnar2 = make_columnar("numbers", &[2u64]);
    let columnars = &[&columnar1, &columnar2];
    let merge_order = StackMergeOrder::stack(columnars).into();
    let column_map: BTreeMap<(String, ColumnTypeCategory), GroupedColumnsHandle> =
        group_columns_for_merge(columnars, &[], &merge_order).unwrap();
    assert_eq!(column_map.len(), 1);
    assert!(column_map.contains_key(&("numbers".to_string(), ColumnTypeCategory::Numerical)));
}

//#[test]
// fn test_impossible_coercion_returns_an_error() {
// let columnar1 = make_columnar("numbers", &[u64::MAX]);
// let merge_order = StackMergeOrder::stack(&[&columnar1]).into();
// let group_error = group_columns_for_merge_iter(
//&[&columnar1],
//&[("numbers".to_string(), ColumnType::I64)],
//&merge_order,
//)
//.unwrap_err();
// assert_eq!(group_error.kind(), io::ErrorKind::InvalidInput);
//}

#[test]
fn test_group_columns_with_required_column() {
    let columnar1 = make_columnar("numbers", &[1i64]);
    let columnar2 = make_columnar("numbers", &[2u64]);
    let columnars = &[&columnar1, &columnar2];
    let merge_order = StackMergeOrder::stack(columnars).into();
    let column_map: BTreeMap<(String, ColumnTypeCategory), GroupedColumnsHandle> =
        group_columns_for_merge(
            &[&columnar1, &columnar2],
            &[("numbers".to_string(), ColumnType::U64)],
            &merge_order,
        )
        .unwrap();
    assert_eq!(column_map.len(), 1);
    assert!(column_map.contains_key(&("numbers".to_string(), ColumnTypeCategory::Numerical)));
}

#[test]
fn test_group_columns_required_column_is_above_all_columns_have_the_same_type_rule() {
    let columnar1 = make_columnar("numbers", &[2i64]);
    let columnar2 = make_columnar("numbers", &[2i64]);
    let columnars = &[&columnar1, &columnar2];
    let merge_order = StackMergeOrder::stack(columnars).into();
    let column_map: BTreeMap<(String, ColumnTypeCategory), GroupedColumnsHandle> =
        group_columns_for_merge(
            columnars,
            &[("numbers".to_string(), ColumnType::U64)],
            &merge_order,
        )
        .unwrap();
    assert_eq!(column_map.len(), 1);
    assert!(column_map.contains_key(&("numbers".to_string(), ColumnTypeCategory::Numerical)));
}

#[test]
fn test_missing_column() {
    let columnar1 = make_columnar("numbers", &[-1i64]);
    let columnar2 = make_columnar("numbers2", &[2u64]);
    let columnars = &[&columnar1, &columnar2];
    let merge_order = StackMergeOrder::stack(columnars).into();
    let column_map: BTreeMap<(String, ColumnTypeCategory), GroupedColumnsHandle> =
        group_columns_for_merge(columnars, &[], &merge_order).unwrap();
    assert_eq!(column_map.len(), 2);
    assert!(column_map.contains_key(&("numbers".to_string(), ColumnTypeCategory::Numerical)));
    {
        let columns = &column_map
            .get(&("numbers".to_string(), ColumnTypeCategory::Numerical))
            .unwrap()
            .columns;
        assert!(columns[0].is_some());
        assert!(columns[1].is_none());
    }
    {
        let columns = &column_map
            .get(&("numbers2".to_string(), ColumnTypeCategory::Numerical))
            .unwrap()
            .columns;
        assert!(columns[0].is_none());
        assert!(columns[1].is_some());
    }
}
