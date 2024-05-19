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

#[derive(
    Debug, Clone, Copy, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
)]
pub enum UpsertAction {
    Merged,
    NoChange,
    Inserted,
}

#[enum_dispatch]
pub trait UpsertFn {
    fn upsert(&self, old: Value, new: Value) -> Value;
}

#[enum_dispatch(UpsertFn)]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub enum UpsertEnum {
    HyperLogLog2Upsert,
    HyperLogLog4Upsert,
    HyperLogLog8Upsert,
    HyperLogLog16Upsert,
    HyperLogLog32Upsert,
    HyperLogLog64Upsert,
    HyperLogLog128Upsert,
    U64Add,
    F64Add,
}

macro_rules! hyperloglog_upsert {
    ($name:ident => $n:expr) => {
        #[derive(
            Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
        )]
        pub struct $name;

        impl UpsertFn for $name {
            fn upsert(&self, old: Value, new: Value) -> Value {
                let (mut old, _) = bincode::decode_from_slice::<HyperLogLog<$n>, _>(
                    old.as_bytes(),
                    bincode::config::standard(),
                )
                .unwrap();
                let (new, _) = bincode::decode_from_slice::<HyperLogLog<$n>, _>(
                    new.as_bytes(),
                    bincode::config::standard(),
                )
                .unwrap();

                old.merge(&new);

                bincode::encode_to_vec(&old, bincode::config::standard())
                    .unwrap()
                    .into()
            }
        }
    };
}

hyperloglog_upsert!(HyperLogLog2Upsert => 2);
hyperloglog_upsert!(HyperLogLog4Upsert => 4);
hyperloglog_upsert!(HyperLogLog8Upsert => 8);
hyperloglog_upsert!(HyperLogLog16Upsert => 16);
hyperloglog_upsert!(HyperLogLog32Upsert => 32);
hyperloglog_upsert!(HyperLogLog64Upsert => 64);
hyperloglog_upsert!(HyperLogLog128Upsert => 128);

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct U64Add;

impl UpsertFn for U64Add {
    fn upsert(&self, old: Value, new: Value) -> Value {
        let (old, _) =
            bincode::decode_from_slice::<u64, _>(old.as_bytes(), bincode::config::standard())
                .unwrap();
        let (new, _) =
            bincode::decode_from_slice::<u64, _>(new.as_bytes(), bincode::config::standard())
                .unwrap();

        bincode::encode_to_vec(old + new, bincode::config::standard())
            .unwrap()
            .into()
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct F64Add;

impl UpsertFn for F64Add {
    fn upsert(&self, old: Value, new: Value) -> Value {
        let (old, _) =
            bincode::decode_from_slice::<f64, _>(old.as_bytes(), bincode::config::standard())
                .unwrap();
        let (new, _) =
            bincode::decode_from_slice::<f64, _>(new.as_bytes(), bincode::config::standard())
                .unwrap();

        bincode::encode_to_vec(old + new, bincode::config::standard())
            .unwrap()
            .into()
    }
}
