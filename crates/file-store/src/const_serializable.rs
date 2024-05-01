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

// change ConstSerializable to something like this
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
