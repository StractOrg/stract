use std::ops::Bound;

use crate::schema::Type;

mod column_field_range_query;
mod range_query;
mod range_query_u64_columnfield;

pub use self::range_query::RangeQuery;
pub use self::range_query_u64_columnfield::ColumnFieldRangeWeight;

// TODO is this correct?
pub(crate) fn is_type_valid_for_columnfield_range_query(typ: Type) -> bool {
    match typ {
        Type::U64 | Type::I64 | Type::F64 | Type::Bool | Type::Date => true,
        Type::IpAddr => true,
        Type::Str | Type::Bytes | Type::Json | Type::U128 => false,
    }
}

fn map_bound<TFrom, TTo>(bound: &Bound<TFrom>, transform: impl Fn(&TFrom) -> TTo) -> Bound<TTo> {
    use self::Bound::*;
    match bound {
        Excluded(ref from_val) => Excluded(transform(from_val)),
        Included(ref from_val) => Included(transform(from_val)),
        Unbounded => Unbounded,
    }
}
