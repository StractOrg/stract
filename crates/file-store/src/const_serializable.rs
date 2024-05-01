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
// along with this program.  If not, see <https://www.gnu.org/licenses/

// change ConstSerializable to something like the following
// once generic_const_exprs is stable. See https://github.com/rust-lang/rust/issues/60551
//
// pub trait ConstSerializable {
//     const BYTES: usize;
//     fn serialize(&self) -> [u8; Self::BYTES];
//     fn deserialize(bytes: [u8; Self::BYTES]) -> Self;
// }

pub trait ConstSerializable {
    const BYTES: usize;

    fn serialize(&self, buf: &mut Vec<u8>);
    fn deserialize(buf: &[u8]) -> Self;
}

macro_rules! impl_const_serializable_num {
    ($t:ty, $n:expr) => {
        impl ConstSerializable for $t {
            const BYTES: usize = $n;

            fn serialize(&self, buf: &mut Vec<u8>) {
                buf.extend_from_slice(&self.to_le_bytes());
            }

            fn deserialize(buf: &[u8]) -> Self {
                let mut bytes = [0; $n];
                bytes.copy_from_slice(&buf[..$n]);
                <$t>::from_le_bytes(bytes)
            }
        }
    };
}

impl_const_serializable_num!(u8, 1);
impl_const_serializable_num!(u16, 2);
impl_const_serializable_num!(u32, 4);
impl_const_serializable_num!(u64, 8);
impl_const_serializable_num!(u128, 16);

impl_const_serializable_num!(i8, 1);
impl_const_serializable_num!(i16, 2);
impl_const_serializable_num!(i32, 4);
impl_const_serializable_num!(i64, 8);
impl_const_serializable_num!(i128, 16);

impl_const_serializable_num!(f32, 4);
impl_const_serializable_num!(f64, 8);
