use proptest::prelude::*;
use proptest::{prop_oneof, proptest};

#[test]
fn test_serialize_and_load_simple() {
    let mut buffer = Vec::new();
    let vals = &[1u128, 2u128, 5u128];
    serialize_u128_based_column_values(&&vals[..], &[CodecType::Raw], &mut buffer).unwrap();
    let col = load_u128_based_column_values::<u128>(OwnedBytes::new(buffer)).unwrap();
    assert_eq!(col.num_vals(), 3);
    assert_eq!(col.get_val(0), 1);
    assert_eq!(col.get_val(1), 2);
    assert_eq!(col.get_val(2), 5);
}

#[test]
fn test_empty_column_u128() {
    let vals: [u128; 0] = [];
    let mut num_acceptable_codecs = 0;
    for codec in ALL_U128_CODEC_TYPES {
        let mut buffer = Vec::new();
        if serialize_u128_based_column_values(&&vals[..], &[codec], &mut buffer).is_err() {
            continue;
        }
        num_acceptable_codecs += 1;
        let col = load_u128_based_column_values::<u128>(OwnedBytes::new(buffer)).unwrap();
        assert_eq!(col.num_vals(), 0);
        assert_eq!(col.min_value(), u128::MIN);
        assert_eq!(col.max_value(), u128::MIN);
    }
    assert!(num_acceptable_codecs > 0);
}

pub(crate) fn create_and_validate<TColumnCodec: ColumnCodec<u128>>(
    vals: &[u128],
    name: &str,
) -> Option<(f32, f32)> {
    let num_rows = vals.len() as u32;
    let mut codec_estimator: TColumnCodec::Estimator = Default::default();

    for val in vals.boxed_iter() {
        codec_estimator.collect(val);
    }
    codec_estimator.finalize();
    let estimation = codec_estimator.estimate()?;

    let mut buffer = Vec::new();
    codec_estimator
        .serialize(vals.boxed_iter().as_mut(), &mut buffer)
        .unwrap();

    let actual_compression = buffer.len() as u64;

    let reader = TColumnCodec::load(OwnedBytes::new(buffer)).unwrap();
    assert_eq!(reader.num_vals(), vals.len() as u32);
    let mut buffer = Vec::new();
    for (doc, orig_val) in vals.iter().copied().enumerate() {
        let val = reader.get_val(doc as u32);
        assert_eq!(
            val, orig_val,
            "val `{val}` does not match orig_val {orig_val:?}, in data set {name}, data `{vals:?}`",
        );

        buffer.resize(1, 0);
        reader.get_vals(&[doc as u32], &mut buffer);
        let val = buffer[0];
        assert_eq!(
            val, orig_val,
            "val `{val}` does not match orig_val {orig_val:?}, in data set {name}, data `{vals:?}`",
        );
    }

    let all_docs: Vec<u32> = (0..vals.len() as u32).collect();
    buffer.resize(all_docs.len(), 0);
    reader.get_vals(&all_docs, &mut buffer);
    assert_eq!(vals, buffer);

    if !vals.is_empty() {
        let test_rand_idx = rand::thread_rng().gen_range(0..=vals.len() - 1);
        let expected_positions: Vec<u32> = vals
            .iter()
            .enumerate()
            .filter(|(_, el)| **el == vals[test_rand_idx])
            .map(|(pos, _)| pos as u32)
            .collect();
        let mut positions = Vec::new();
        reader.get_row_ids_for_value_range(
            vals[test_rand_idx]..=vals[test_rand_idx],
            0..vals.len() as u32,
            &mut positions,
        );
        assert_eq!(expected_positions, positions);
    }
    if actual_compression > 1000 {
        assert!(relative_difference(estimation, actual_compression) < 0.10f32);
    }
    Some((
        compression_rate(estimation, num_rows),
        compression_rate(actual_compression, num_rows),
    ))
}

fn compression_rate(num_bytes: u64, num_values: u32) -> f32 {
    num_bytes as f32 / (num_values as f32 * 8.0)
}

fn relative_difference(left: u64, right: u64) -> f32 {
    let left = left as f32;
    let right = right as f32;
    2.0f32 * (left - right).abs() / (left + right)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn test_proptest_small_raw(data in proptest::collection::vec(num_strategy(), 1..10)) {
        create_and_validate::<RawCodec>(&data, "proptest raw");
    }
}

#[test]
fn test_small_raw_example() {
    create_and_validate::<RawCodec>(&[9223372036854775808, 9223370937344622593], "proptest raw");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    #[test]
    fn test_proptest_large_raw(data in proptest::collection::vec(num_strategy(), 1..6000)) {
        create_and_validate::<RawCodec>(&data, "proptest raw");
    }

}

fn num_strategy() -> impl Strategy<Value = u128> {
    prop_oneof![
        1 => prop::num::u128::ANY.prop_map(|num| u128::MAX - (num % 10) ),
        1 => prop::num::u128::ANY.prop_map(|num| num % 10 ),
        20 => prop::num::u128::ANY,
    ]
}

pub fn get_codec_test_datasets() -> Vec<(Vec<u128>, &'static str)> {
    let mut data_and_names = vec![];

    let data = (10..=10_000_u128).collect::<Vec<_>>();
    data_and_names.push((data, "simple monotonically increasing"));

    data_and_names.push((
        vec![5, 6, 7, 8, 9, 10, 99, 100],
        "offset in linear interpol",
    ));
    data_and_names.push((vec![5, 50, 3, 13, 1, 1000, 35], "rand small"));
    data_and_names.push((vec![10], "single value"));

    data_and_names.push((
        vec![1572656989877777, 1170935903116329, 720575940379279, 0],
        "overflow error",
    ));

    data_and_names
}

fn test_codec<C: ColumnCodec<u128>>() {
    let codec_name = std::any::type_name::<C>();
    for (data, dataset_name) in get_codec_test_datasets() {
        let estimate_actual_opt: Option<(f32, f32)> =
            tests::create_and_validate::<C>(&data, dataset_name);
        let result = if let Some((estimate, actual)) = estimate_actual_opt {
            format!("Estimate `{estimate}` Actual `{actual}`")
        } else {
            "Disabled".to_string()
        };
        println!("Codec {codec_name}, DataSet {dataset_name}, {result}");
    }
}
#[test]
fn test_codec_raw() {
    test_codec::<RawCodec>();
}

use super::*;

#[test]
fn test_column_field_codec_type_to_code() {
    let mut count_codec = 0;
    for code in 0..=255 {
        if let Some(codec_type) = CodecType::try_from_code(code) {
            assert_eq!(codec_type.to_code(), code);
            count_codec += 1;
        }
    }
    assert_eq!(count_codec, 1);
}

#[test]
pub fn test_columnfield2() {
    let test_columnfield =
        crate::columnar::column_values::serialize_and_load_u128_based_column_values::<u128>(
            &&[100u128, 200u128, 300u128][..],
            &ALL_U128_CODEC_TYPES,
        );
    assert_eq!(test_columnfield.get_val(0), 100);
    assert_eq!(test_columnfield.get_val(1), 200);
    assert_eq!(test_columnfield.get_val(2), 300);
}
