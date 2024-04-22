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

//! Index used to find the canonical URL for a given URL.
//! (https://en.wikipedia.org/wiki/Canonical_link_element)

use std::path::Path;

use url::Url;

use crate::{webpage::url_ext::UrlExt, Result};

#[derive(bincode::Decode, bincode::Encode)]
struct StoredUrl(#[bincode(with_serde)] Url);

pub struct CanonicalIndex {
    inner: speedy_kv::Db<StoredUrl, StoredUrl>,
}

impl CanonicalIndex {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Self {
            inner: speedy_kv::Db::open_or_create(path)?,
        })
    }

    pub fn insert(&mut self, original: Url, canonical: Url) -> Result<()> {
        if original == canonical || original.root_domain() != canonical.root_domain() {
            return Ok(());
        }

        let original = StoredUrl(original);
        let canonical = StoredUrl(canonical);

        self.inner.insert(original, canonical)
    }

    pub fn get(&self, url: &Url) -> Result<Option<Url>> {
        Ok(self
            .inner
            .get(&StoredUrl(url.clone()))?
            .map(|stored_url| stored_url.0))
    }

    pub fn commit(&mut self) -> Result<()> {
        self.inner.commit()
    }

    pub fn optimize_read(&mut self) -> Result<()> {
        self.inner.merge_all_segments()
    }

    pub fn merge(&mut self, other: CanonicalIndex) -> Result<()> {
        self.inner.merge(other.inner)?;
        self.optimize_read()
    }
}
