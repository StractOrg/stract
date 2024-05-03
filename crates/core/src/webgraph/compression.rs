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

#[derive(Default, Debug, Clone, Copy, bincode::Encode, bincode::Decode)]
pub enum Compression {
    None,
    #[default]
    Lz4,
}

impl Compression {
    pub fn compress(&self, bytes: &[u8]) -> Vec<u8> {
        match self {
            Compression::None => bytes.to_vec(),
            Compression::Lz4 => lz4_flex::compress_prepend_size(bytes),
        }
    }

    pub fn decompress(&self, bytes: &[u8]) -> Vec<u8> {
        match self {
            Compression::None => bytes.to_vec(),
            Compression::Lz4 => lz4_flex::decompress_size_prepended(bytes).unwrap(),
        }
    }
}
