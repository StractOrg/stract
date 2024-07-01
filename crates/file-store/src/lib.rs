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
// along with this program.  If not, see <https://www.gnu.org/licenses/>

//! A collection of simple disk-based data structures.

pub type Result<T> = std::result::Result<T, anyhow::Error>;

pub mod const_serializable;
pub mod iterable;
pub mod peekable;
pub mod random_lookup;

pub use const_serializable::ConstSerializable;
pub use peekable::Peekable;
