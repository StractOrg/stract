// Stract is an open source web search engine.
// Copyright (C) 2024 Stract ApS
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use enum_dispatch::enum_dispatch;

use super::Value;

#[derive(
    Debug, Clone, Copy, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
)]
pub enum UpsertAction {
    Merged,
    NoChange,
    Inserted,
}

impl UpsertAction {
    pub fn is_changed(&self) -> bool {
        matches!(self, UpsertAction::Merged | UpsertAction::Inserted)
    }
}

#[enum_dispatch]
pub trait UpsertFn {
    fn upsert(&self, old: Value, new: Value) -> Value;
}

#[enum_dispatch(UpsertFn)]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub enum UpsertEnum {
    HyperLogLog8Upsert,
    HyperLogLog16Upsert,
    HyperLogLog32Upsert,
    HyperLogLog64Upsert,
    HyperLogLog128Upsert,
    U64Add,
    U64Min,
    F32Add,
    F64Add,
    KahanSumAdd,
}

macro_rules! unwrap_value {
    ($value:expr, $variant:ident) => {
        if let Value::$variant(value) = $value {
            value
        } else {
            panic!("Expected {}", stringify!($variant));
        }
    };
}

macro_rules! hyperloglog_upsert {
    ($name:ident, $variant:ident) => {
        #[derive(
            Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
        )]
        pub struct $name;

        impl UpsertFn for $name {
            fn upsert(&self, old: Value, new: Value) -> Value {
                let mut old = unwrap_value!(old, $variant);
                let new = unwrap_value!(new, $variant);

                old.merge(&new);

                Value::$variant(old)
            }
        }
    };
}

hyperloglog_upsert!(HyperLogLog8Upsert, HyperLogLog8);
hyperloglog_upsert!(HyperLogLog16Upsert, HyperLogLog16);
hyperloglog_upsert!(HyperLogLog32Upsert, HyperLogLog32);
hyperloglog_upsert!(HyperLogLog64Upsert, HyperLogLog64);
hyperloglog_upsert!(HyperLogLog128Upsert, HyperLogLog128);

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct U64Add;

impl UpsertFn for U64Add {
    fn upsert(&self, old: Value, new: Value) -> Value {
        let old = unwrap_value!(old, U64);
        let new = unwrap_value!(new, U64);

        Value::U64(old + new)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct U64Min;

impl UpsertFn for U64Min {
    fn upsert(&self, old: Value, new: Value) -> Value {
        let old = unwrap_value!(old, U64);
        let new = unwrap_value!(new, U64);

        Value::U64(old.min(new))
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct F32Add;

impl UpsertFn for F32Add {
    fn upsert(&self, old: Value, new: Value) -> Value {
        let old = unwrap_value!(old, F32);
        let new = unwrap_value!(new, F32);

        Value::F32(old + new)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct F64Add;

impl UpsertFn for F64Add {
    fn upsert(&self, old: Value, new: Value) -> Value {
        let old = unwrap_value!(old, F64);
        let new = unwrap_value!(new, F64);

        Value::F64(old + new)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct KahanSumAdd;

impl UpsertFn for KahanSumAdd {
    fn upsert(&self, old: Value, new: Value) -> Value {
        let old = unwrap_value!(old, KahanSum);
        let new = unwrap_value!(new, KahanSum);

        let new: f64 = new.into();

        Value::KahanSum(old + new)
    }
}
