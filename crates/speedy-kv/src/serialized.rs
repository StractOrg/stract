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

use crate::Result;
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub struct Serialized<T> {
    bytes: Vec<u8>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> bincode::Encode for Serialized<T> {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        self.bytes.encode(encoder)
    }
}

impl<T> bincode::Decode for Serialized<T> {
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let bytes = Vec::<u8>::decode(decoder)?;
        Ok(Self {
            bytes,
            _marker: std::marker::PhantomData,
        })
    }
}

impl<T> AsRef<[u8]> for Serialized<T> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<T> From<Vec<u8>> for Serialized<T> {
    fn from(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T> PartialOrd for Serialized<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> PartialEq for Serialized<T> {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl<T> Eq for Serialized<T> {}

impl<T> Ord for Serialized<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.bytes.cmp(&other.bytes)
    }
}

impl<T> Serialized<T>
where
    T: bincode::Encode,
{
    pub fn new(value: &T) -> Result<Self> {
        let bytes = bincode::encode_to_vec(value, common::bincode_config())?;
        Ok(Self {
            bytes,
            _marker: std::marker::PhantomData,
        })
    }
}

impl<T> Serialized<T> {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

pub struct SerializedRef<'a, T> {
    bytes: &'a [u8],
    _marker: std::marker::PhantomData<T>,
}

impl<'a, T> std::fmt::Debug for SerializedRef<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let truncated = self.bytes.len() > 16;

        let truncated_bytes = if truncated {
            &self.bytes[..16]
        } else {
            self.bytes
        };

        let field = if truncated {
            "bytes (truncated)"
        } else {
            "bytes"
        };

        f.debug_struct("Serialized")
            .field(field, &truncated_bytes)
            .finish()
    }
}

impl<'a, T> Clone for SerializedRef<'a, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, T> Copy for SerializedRef<'a, T> {}

impl<'a, T> AsRef<[u8]> for SerializedRef<'a, T> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl<'a, T> SerializedRef<'a, T> {
    pub fn as_bytes(&self) -> &[u8] {
        self.bytes
    }
}

impl<'a, T> From<&'a [u8]> for SerializedRef<'a, T> {
    fn from(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'a, T, const N: usize> From<&'a [u8; N]> for SerializedRef<'a, T> {
    fn from(bytes: &'a [u8; N]) -> Self {
        Self {
            bytes,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'a, T> PartialOrd for SerializedRef<'a, T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a, T> PartialEq for SerializedRef<'a, T> {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl<'a, T> Eq for SerializedRef<'a, T> {}

impl<'a, T> Ord for SerializedRef<'a, T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.bytes.cmp(other.bytes)
    }
}

impl<'a, T> From<&'a Serialized<T>> for SerializedRef<'a, T> {
    fn from(serialized: &'a Serialized<T>) -> Self {
        Self {
            bytes: serialized.as_bytes(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T> From<Serialized<T>> for Vec<u8> {
    fn from(serialized: Serialized<T>) -> Vec<u8> {
        serialized.bytes
    }
}
