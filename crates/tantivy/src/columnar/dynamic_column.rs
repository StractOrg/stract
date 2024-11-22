use std::sync::Arc;
use std::{fmt, io};

use crate::common::file_slice::FileSlice;
use crate::common::{ByteCount, DateTime, HasLen, OwnedBytes};

use super::column::{BytesColumn, Column};
use super::column_values::{monotonic_map_column, StrictlyMonotonicFn};
use super::columnar::ColumnType;
use super::{ColumnIndex, ColumnValues, NumericalType};

#[derive(Clone)]
pub enum DynamicColumn {
    Bool(Column<bool>),
    I64(Column<i64>),
    U64(Column<u64>),
    F64(Column<f64>),
    U128(Column<u128>),
    DateTime(Column<DateTime>),
    Bytes(BytesColumn),
}

impl fmt::Debug for DynamicColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[{} |", self.column_type())?;
        match self {
            DynamicColumn::Bool(col) => write!(f, " {col:?}")?,
            DynamicColumn::I64(col) => write!(f, " {col:?}")?,
            DynamicColumn::U64(col) => write!(f, " {col:?}")?,
            DynamicColumn::F64(col) => write!(f, "{col:?}")?,
            DynamicColumn::DateTime(col) => write!(f, "{col:?}")?,
            DynamicColumn::Bytes(col) => write!(f, "{col:?}")?,
            DynamicColumn::U128(col) => write!(f, "{col:?}")?,
        }
        write!(f, "]")
    }
}

impl DynamicColumn {
    pub fn column_index(&self) -> &ColumnIndex {
        match self {
            DynamicColumn::Bool(c) => &c.index,
            DynamicColumn::I64(c) => &c.index,
            DynamicColumn::U64(c) => &c.index,
            DynamicColumn::F64(c) => &c.index,
            DynamicColumn::DateTime(c) => &c.index,
            DynamicColumn::Bytes(c) => &c.ords().index,
            DynamicColumn::U128(c) => &c.index,
        }
    }

    pub fn num_values(&self) -> u32 {
        match self {
            DynamicColumn::Bool(c) => c.values.num_vals(),
            DynamicColumn::I64(c) => c.values.num_vals(),
            DynamicColumn::U64(c) => c.values.num_vals(),
            DynamicColumn::F64(c) => c.values.num_vals(),
            DynamicColumn::DateTime(c) => c.values.num_vals(),
            DynamicColumn::Bytes(c) => c.ords().values.num_vals(),
            DynamicColumn::U128(c) => c.values.num_vals(),
        }
    }

    pub fn column_type(&self) -> ColumnType {
        match self {
            DynamicColumn::Bool(_) => ColumnType::Bool,
            DynamicColumn::I64(_) => ColumnType::I64,
            DynamicColumn::U64(_) => ColumnType::U64,
            DynamicColumn::F64(_) => ColumnType::F64,
            DynamicColumn::DateTime(_) => ColumnType::DateTime,
            DynamicColumn::Bytes(_) => ColumnType::Bytes,
            DynamicColumn::U128(_) => ColumnType::U128,
        }
    }

    pub fn coerce_numerical(self, target_numerical_type: NumericalType) -> Option<Self> {
        match target_numerical_type {
            NumericalType::I64 => self.coerce_to_i64(),
            NumericalType::U64 => self.coerce_to_u64(),
            NumericalType::F64 => self.coerce_to_f64(),
            NumericalType::U128 => self.coerce_to_u128(),
        }
    }

    pub fn is_numerical(&self) -> bool {
        self.column_type().numerical_type().is_some()
    }

    pub fn is_f64(&self) -> bool {
        self.column_type().numerical_type() == Some(NumericalType::F64)
    }
    pub fn is_i64(&self) -> bool {
        self.column_type().numerical_type() == Some(NumericalType::I64)
    }
    pub fn is_u64(&self) -> bool {
        self.column_type().numerical_type() == Some(NumericalType::U64)
    }

    fn coerce_to_f64(self) -> Option<DynamicColumn> {
        match self {
            DynamicColumn::I64(column) => Some(DynamicColumn::F64(Column {
                index: column.index,
                values: Arc::new(monotonic_map_column(column.values, MapI64ToF64)),
            })),
            DynamicColumn::U64(column) => Some(DynamicColumn::F64(Column {
                index: column.index,
                values: Arc::new(monotonic_map_column(column.values, MapU64ToF64)),
            })),
            DynamicColumn::F64(_) => Some(self),
            _ => None,
        }
    }
    fn coerce_to_i64(self) -> Option<DynamicColumn> {
        match self {
            DynamicColumn::U64(column) => {
                if column.max_value() > i64::MAX as u64 {
                    return None;
                }
                Some(DynamicColumn::I64(Column {
                    index: column.index,
                    values: Arc::new(monotonic_map_column(column.values, MapU64ToI64)),
                }))
            }
            DynamicColumn::I64(_) => Some(self),
            _ => None,
        }
    }
    fn coerce_to_u64(self) -> Option<DynamicColumn> {
        match self {
            DynamicColumn::I64(column) => {
                if column.min_value() < 0 {
                    return None;
                }
                Some(DynamicColumn::U64(Column {
                    index: column.index,
                    values: Arc::new(monotonic_map_column(column.values, MapI64ToU64)),
                }))
            }
            DynamicColumn::U64(_) => Some(self),
            _ => None,
        }
    }
    fn coerce_to_u128(self) -> Option<DynamicColumn> {
        match self {
            DynamicColumn::U128(_) => Some(self),
            _ => None,
        }
    }
}

struct MapI64ToF64;
impl StrictlyMonotonicFn<i64, f64> for MapI64ToF64 {
    #[inline(always)]
    fn mapping(&self, inp: i64) -> f64 {
        inp as f64
    }
    #[inline(always)]
    fn inverse(&self, out: f64) -> i64 {
        out as i64
    }
}

struct MapU64ToF64;
impl StrictlyMonotonicFn<u64, f64> for MapU64ToF64 {
    #[inline(always)]
    fn mapping(&self, inp: u64) -> f64 {
        inp as f64
    }
    #[inline(always)]
    fn inverse(&self, out: f64) -> u64 {
        out as u64
    }
}

struct MapU64ToI64;
impl StrictlyMonotonicFn<u64, i64> for MapU64ToI64 {
    #[inline(always)]
    fn mapping(&self, inp: u64) -> i64 {
        inp as i64
    }
    #[inline(always)]
    fn inverse(&self, out: i64) -> u64 {
        out as u64
    }
}

struct MapI64ToU64;
impl StrictlyMonotonicFn<i64, u64> for MapI64ToU64 {
    #[inline(always)]
    fn mapping(&self, inp: i64) -> u64 {
        inp as u64
    }
    #[inline(always)]
    fn inverse(&self, out: u64) -> i64 {
        out as i64
    }
}

macro_rules! static_dynamic_conversions {
    ($typ:ty, $enum_name:ident) => {
        impl From<DynamicColumn> for Option<$typ> {
            fn from(dynamic_column: DynamicColumn) -> Option<$typ> {
                if let DynamicColumn::$enum_name(col) = dynamic_column {
                    Some(col)
                } else {
                    None
                }
            }
        }

        impl From<$typ> for DynamicColumn {
            fn from(typed_column: $typ) -> Self {
                DynamicColumn::$enum_name(typed_column)
            }
        }
    };
}

static_dynamic_conversions!(Column<bool>, Bool);
static_dynamic_conversions!(Column<u64>, U64);
static_dynamic_conversions!(Column<i64>, I64);
static_dynamic_conversions!(Column<f64>, F64);
static_dynamic_conversions!(Column<DateTime>, DateTime);
static_dynamic_conversions!(BytesColumn, Bytes);
static_dynamic_conversions!(Column<u128>, U128);

#[derive(Clone, Debug)]
pub struct DynamicColumnHandle {
    pub(crate) file_slice: FileSlice,
    pub(crate) column_type: ColumnType,
}

impl DynamicColumnHandle {
    // TODO rename load
    pub fn open(&self) -> io::Result<DynamicColumn> {
        let column_bytes: OwnedBytes = self.file_slice.read_bytes()?;
        self.open_internal(column_bytes)
    }

    #[doc(hidden)]
    pub fn file_slice(&self) -> &FileSlice {
        &self.file_slice
    }

    /// Returns the `u64` columnar field reader reader associated with `fields` of types
    /// Str, u64, i64, f64, bool, or datetime.
    pub fn open_u64_lenient(&self) -> io::Result<Option<Column<u64>>> {
        let column_bytes = self.file_slice.read_bytes()?;
        match self.column_type {
            ColumnType::Bytes => {
                let column: BytesColumn = super::column::open_column_bytes(column_bytes)?;
                Ok(Some(column.term_ord_column))
            }
            ColumnType::Bool
            | ColumnType::I64
            | ColumnType::U64
            | ColumnType::F64
            | ColumnType::DateTime => {
                let column = super::column::open_column_u64::<u64>(column_bytes)?;
                Ok(Some(column))
            }
            ColumnType::U128 => Ok(None),
        }
    }

    fn open_internal(&self, column_bytes: OwnedBytes) -> io::Result<DynamicColumn> {
        let dynamic_column: DynamicColumn = match self.column_type {
            ColumnType::Bytes => super::column::open_column_bytes(column_bytes)?.into(),
            ColumnType::I64 => super::column::open_column_u64::<i64>(column_bytes)?.into(),
            ColumnType::U64 => super::column::open_column_u64::<u64>(column_bytes)?.into(),
            ColumnType::F64 => super::column::open_column_u64::<f64>(column_bytes)?.into(),
            ColumnType::Bool => super::column::open_column_u64::<bool>(column_bytes)?.into(),
            ColumnType::U128 => super::column::open_column_u128::<u128>(column_bytes)?.into(),
            ColumnType::DateTime => {
                super::column::open_column_u64::<DateTime>(column_bytes)?.into()
            }
        };
        Ok(dynamic_column)
    }

    pub fn num_bytes(&self) -> ByteCount {
        self.file_slice.len().into()
    }

    pub fn column_type(&self) -> ColumnType {
        self.column_type
    }
}
