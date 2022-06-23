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

use std::path::Path;

pub struct CentralityStore {
    inner: sled::Tree,
}

impl CentralityStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let db = sled::Config::default()
            .path(path)
            .use_compression(true)
            .mode(sled::Mode::LowSpace)
            .open()
            .expect("Failed to open database");

        Self {
            inner: db
                .open_tree("centrality_store")
                .expect("Failed to open tree"),
        }
    }

    pub fn insert(&mut self, key: String, centrality: f64) {
        let key_bytes = bincode::serialize(&key).expect("Failed to serialize key");
        let centrality_bytes =
            bincode::serialize(&centrality).expect("Failed to serialize centrality");

        self.inner
            .insert(key_bytes, centrality_bytes)
            .expect("Failed to insert into tree");
    }

    pub fn get(&self, key: &str) -> Option<f64> {
        let key_bytes = bincode::serialize(key).expect("Failed to serialize key");
        self.inner
            .get(key_bytes)
            .expect("Failed to retrieve value from key")
            .map(|bytes| bincode::deserialize(&bytes).expect("Failed to deserialize value"))
    }

    pub fn append(&mut self, it: impl Iterator<Item = (String, f64)>) {
        it.filter(|(_, value)| *value != 0.0)
            .for_each(|(key, value)| {
                self.insert(key, value);
            });
        self.inner.flush().unwrap();
    }
}
