// Stract is an open source web search engine.
// Copyright (C) 2023 Stract ApS
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

use std::hash::Hash;

#[derive(
    Debug, Eq, Clone, Copy, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
)]
pub struct Prehashed(pub u128);

impl From<u128> for Prehashed {
    fn from(val: u128) -> Self {
        Self(val)
    }
}

impl Hash for Prehashed {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_u128(self.0);
    }
}

impl PartialEq for Prehashed {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

pub fn hash<T: AsRef<[u8]>>(val: T) -> Prehashed {
    let digest = md5::compute(val);
    Prehashed(u128::from_le_bytes(*digest))
}
