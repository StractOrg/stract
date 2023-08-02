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

use crate::{
    ceil_char_boundary,
    kv::{rocksdb_store::RocksDbStore, Kv},
    prehashed::{hash, Prehashed},
};
use std::{collections::HashSet, path::Path};
use url::Url;

pub struct SubdomainCounter {
    inner: Box<dyn Kv<Prehashed, HashSet<String>>>,
}

impl SubdomainCounter {
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        Self {
            inner: RocksDbStore::open(path),
        }
    }

    pub fn increment(&mut self, url: Url) {
        let scheme = url.scheme();
        let domain = url.domain().unwrap_or_default();

        let mut url = url.as_str().strip_prefix(scheme).unwrap_or_default();

        if let Some(slash) = url.find('/') {
            url = &url[..ceil_char_boundary(url, slash)];
        }

        if let Some(subdomain) = url.strip_suffix(domain) {
            let domain = hash(domain);
            let subdomain = subdomain.to_string();

            let mut set = self.inner.get(&domain).unwrap_or_default();
            set.insert(subdomain);

            self.inner.insert(domain, set);
        }
    }

    pub fn commit(&self) {
        self.inner.flush();
    }

    pub fn merge(&mut self, other: Self) {
        for (key, val) in other.inner.iter() {
            let mut current = self.inner.get(&key).unwrap_or_default();
            current.extend(val);
            self.inner.insert(key, current);
        }
    }
}
