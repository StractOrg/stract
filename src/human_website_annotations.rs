// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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

use serde::{Deserialize, Serialize};

use crate::Result;
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufReader, Read, Write},
    path::Path,
};

#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Topic<const N: usize = 3> {
    detailed_topics: Vec<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Info {
    pub description: String,
    pub topic: Topic,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

        let bytes = bincode::serialize(&self)?;
        file.write_all(&bytes)?;

        Ok(())
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut reader = BufReader::new(File::open(path)?);

        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;

        Ok(bincode::deserialize(&bytes)?)
    }

    pub fn all_topics(&self) -> HashSet<Topic> {
        self.0.iter().map(|(_, info)| info.topic.clone()).collect()
    }
}
