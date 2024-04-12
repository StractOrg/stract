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

use itertools::Itertools;

use tantivy::schema::Facet;

use crate::Result;
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufReader, Write},
    path::Path,
};

#[derive(Debug, Hash, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize)]
pub struct Topic<const N: usize = 2> {
    detailed_topics: Vec<String>,
}

/// bincode seems to not like default const generics, therefore manual impl
impl<const N: usize> bincode::Encode for Topic<N> {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        self.detailed_topics.encode(encoder)
    }
}

impl<const N: usize> bincode::Decode for Topic<N> {
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        Ok(Self {
            detailed_topics: Vec::decode(decoder)?,
        })
    }
}

impl<'de, const N: usize> bincode::BorrowDecode<'de> for Topic<N> {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        Ok(Self {
            detailed_topics: Vec::borrow_decode(decoder)?,
        })
    }
}

impl<const N: usize> From<Facet> for Topic<N> {
    fn from(facet: Facet) -> Self {
        Self {
            detailed_topics: facet
                .to_path()
                .into_iter()
                .take(N)
                .map(String::from)
                .collect(),
        }
    }
}

impl<const N: usize> Topic<N> {
    pub fn from_string(s: String) -> Self {
        Self {
            detailed_topics: s
                .split('/')
                .skip(2)
                .take(N)
                .map(String::from)
                .collect::<Vec<_>>(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct Info {
    pub description: String,
    pub topic: Topic,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct Mapper(HashMap<String, Info>);

impl From<HashMap<String, Info>> for Mapper {
    fn from(map: HashMap<String, Info>) -> Self {
        Self(map)
    }
}

impl Mapper {
    pub fn save<P: AsRef<Path>>(self, path: P) -> Result<()> {
        let mut file = File::options()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;

        let bytes = bincode::encode_to_vec(self, bincode::config::standard())?;
        file.write_all(&bytes)?;

        Ok(())
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut reader = BufReader::new(File::open(path)?);

        Ok(bincode::decode_from_std_read(
            &mut reader,
            bincode::config::standard(),
        )?)
    }

    pub fn get(&self, host: &String) -> Option<&Info> {
        self.0.get(host)
    }

    pub fn all_topics(&self) -> HashSet<Topic> {
        self.0.values().map(|info| info.topic.clone()).collect()
    }

    pub fn top_topics(&self, top_n: usize) -> Vec<Topic> {
        let mut topics: HashMap<Topic, usize> = HashMap::new();

        for topic in self.0.values().map(|info| info.topic.clone()) {
            *topics.entry(topic).or_default() += 1;
        }

        topics
            .into_iter()
            .sorted_by_key(|(_, count)| *count)
            .rev()
            .map(|(topic, _)| topic)
            .take(top_n)
            .collect()
    }
}
