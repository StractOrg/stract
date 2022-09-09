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

use crate::kv::{rocksdb_store::RocksDbStore, Kv};

pub struct CentralityStore {
    inner: Box<dyn Kv<String, f64>>,
}

impl CentralityStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            inner: RocksDbStore::open(path),
        }
    }

    pub fn insert(&mut self, key: String, centrality: f64) {
        self.inner.insert(key, centrality);
    }

    pub fn get(&self, key: &str) -> Option<f64> {
        self.inner.get(&key.to_string())
    }

    pub fn append(&mut self, it: impl Iterator<Item = (String, f64)>) {
        it.filter(|(_, value)| *value != 0.0)
            .for_each(|(key, value)| {
                self.insert(key, value);
            });

        self.flush();
    }

    pub fn flush(&self) {
        self.inner.flush();
    }
}
