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

use bloom::U64BloomFilter;

use crate::kahan_sum::KahanSum;

pub trait ValueTrait: TryFrom<Value> + Into<Value> {}

impl ValueTrait for String {}

type ApproxHarmonicMeta = crate::entrypoint::ampc::approximated_harmonic_centrality::Meta;
impl ValueTrait for ApproxHarmonicMeta {}

type F32 = f32;
impl ValueTrait for F32 {}

type F64 = f64;
impl ValueTrait for F64 {}

type U64 = u64;
impl ValueTrait for U64 {}

impl ValueTrait for KahanSum {}

type HyperLogLog8 = crate::hyperloglog::HyperLogLog<8>;
impl ValueTrait for HyperLogLog8 {}
type HyperLogLog16 = crate::hyperloglog::HyperLogLog<16>;
impl ValueTrait for HyperLogLog16 {}
type HyperLogLog32 = crate::hyperloglog::HyperLogLog<32>;
impl ValueTrait for HyperLogLog32 {}
type HyperLogLog64 = crate::hyperloglog::HyperLogLog<64>;
impl ValueTrait for HyperLogLog64 {}
type HyperLogLog128 = crate::hyperloglog::HyperLogLog<128>;
impl ValueTrait for HyperLogLog128 {}

type HarmonicMeta = crate::entrypoint::ampc::harmonic_centrality::Meta;
impl ValueTrait for HarmonicMeta {}

impl ValueTrait for U64BloomFilter {}

type Unit = ();
impl ValueTrait for Unit {}

#[derive(
    serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug, Clone, PartialEq,
)]
pub enum Value {
    String(String),
    ApproxHarmonicMeta(ApproxHarmonicMeta),
    F32(F32),
    F64(F64),
    U64(U64),
    KahanSum(KahanSum),
    HyperLogLog8(HyperLogLog8),
    HyperLogLog16(HyperLogLog16),
    HyperLogLog32(HyperLogLog32),
    HyperLogLog64(HyperLogLog64),
    HyperLogLog128(HyperLogLog128),
    HarmonicMeta(HarmonicMeta),
    U64BloomFilter(U64BloomFilter),
    Unit(Unit),
}

macro_rules! impl_from_to_value {
    ($val:ty, $variant:ident) => {
        impl From<$val> for Value {
            fn from(val: $val) -> Self {
                Value::$variant(val)
            }
        }

        impl TryFrom<Value> for $val {
            type Error = anyhow::Error;

            fn try_from(val: Value) -> Result<Self, Self::Error> {
                match val {
                    Value::$variant(val) => Ok(val),
                    _ => anyhow::bail!("Value is not of type {}", stringify!($val)),
                }
            }
        }
    };
}

impl_from_to_value!(String, String);
impl_from_to_value!(ApproxHarmonicMeta, ApproxHarmonicMeta);
impl_from_to_value!(F32, F32);
impl_from_to_value!(F64, F64);
impl_from_to_value!(U64, U64);
impl_from_to_value!(KahanSum, KahanSum);
impl_from_to_value!(HyperLogLog8, HyperLogLog8);
impl_from_to_value!(HyperLogLog16, HyperLogLog16);
impl_from_to_value!(HyperLogLog32, HyperLogLog32);
impl_from_to_value!(HyperLogLog64, HyperLogLog64);
impl_from_to_value!(HyperLogLog128, HyperLogLog128);
impl_from_to_value!(HarmonicMeta, HarmonicMeta);
impl_from_to_value!(U64BloomFilter, U64BloomFilter);
impl_from_to_value!(Unit, Unit);
