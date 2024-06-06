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

use crate::distributed::member::ShardId;
use crate::webgraph::NodeID;

pub trait KeyTrait: TryFrom<Key> + Into<Key> {
    fn as_bytes(&self) -> Vec<u8>;
}

impl KeyTrait for String {
    fn as_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

impl KeyTrait for NodeID {
    fn as_bytes(&self) -> Vec<u8> {
        self.as_u64().to_le_bytes().to_vec()
    }
}

type Unit = ();
impl KeyTrait for Unit {
    fn as_bytes(&self) -> Vec<u8> {
        vec![]
    }
}

impl KeyTrait for ShardId {
    fn as_bytes(&self) -> Vec<u8> {
        self.as_u64().to_le_bytes().to_vec()
    }
}

type U64 = u64;
impl KeyTrait for U64 {
    fn as_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

#[derive(
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    Debug,
    Clone,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
)]
pub enum Key {
    String(String),
    NodeID(NodeID),
    Unit(Unit),
    ShardId(ShardId),
    U64(U64),
}

impl KeyTrait for Key {
    fn as_bytes(&self) -> Vec<u8> {
        match self {
            Key::String(key) => KeyTrait::as_bytes(key),
            Key::NodeID(key) => KeyTrait::as_bytes(key),
            Key::Unit(key) => KeyTrait::as_bytes(key),
            Key::ShardId(key) => KeyTrait::as_bytes(key),
            Key::U64(key) => KeyTrait::as_bytes(key),
        }
    }
}

macro_rules! impl_from_to_key {
    ($key:ty, $variant:ident) => {
        impl From<$key> for Key {
            fn from(key: $key) -> Self {
                Key::$variant(key)
            }
        }

        impl TryFrom<Key> for $key {
            type Error = anyhow::Error;

            fn try_from(key: Key) -> Result<Self, Self::Error> {
                match key {
                    Key::$variant(key) => Ok(key),
                    _ => anyhow::bail!("Key is not of type {}", stringify!($key)),
                }
            }
        }
    };
}

impl_from_to_key!(String, String);
impl_from_to_key!(NodeID, NodeID);
impl_from_to_key!(Unit, Unit);
impl_from_to_key!(ShardId, ShardId);
impl_from_to_key!(U64, U64);
