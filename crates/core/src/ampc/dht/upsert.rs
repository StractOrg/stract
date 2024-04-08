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

use crate::hyperloglog::HyperLogLog;

use super::Value;

#[enum_dispatch]
pub trait UpsertFn {
    fn upsert(&self, old: Value, new: Value) -> Value;
}

#[enum_dispatch(UpsertFn)]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum UpsertEnum {
    HyperLogLog64Upsert,
    U64Add,
    F64Add,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HyperLogLog64Upsert;

impl UpsertFn for HyperLogLog64Upsert {
    fn upsert(&self, old: Value, new: Value) -> Value {
        let mut old: HyperLogLog<64> = bincode::deserialize(old.as_bytes()).unwrap();
        let new: HyperLogLog<64> = bincode::deserialize(new.as_bytes()).unwrap();

        old.merge(&new);

        bincode::serialize(&old).unwrap().into()
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct U64Add;

impl UpsertFn for U64Add {
    fn upsert(&self, old: Value, new: Value) -> Value {
        let old: u64 = bincode::deserialize(old.as_bytes()).unwrap();
        let new: u64 = bincode::deserialize(new.as_bytes()).unwrap();

        bincode::serialize(&(old + new)).unwrap().into()
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct F64Add;

impl UpsertFn for F64Add {
    fn upsert(&self, old: Value, new: Value) -> Value {
        let old: f64 = bincode::deserialize(old.as_bytes()).unwrap();
        let new: f64 = bincode::deserialize(new.as_bytes()).unwrap();

        bincode::serialize(&(old + new)).unwrap().into()
    }
}
