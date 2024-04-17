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
// along with this program.  If not, see <https://www.gnu.org/licenses/

use url::Url;

use crate::{speedy_kv, Result};
use std::path::Path;

pub struct DownloadedDb {
    db: std::sync::Mutex<speedy_kv::Db<Url, ()>>,
}

impl DownloadedDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = speedy_kv::Db::open_or_create(path)?;

        Ok(Self {
            db: std::sync::Mutex::new(db),
        })
    }

    pub fn has_downloaded(&self, url: &Url) -> bool {
        let key = url.as_str().as_bytes();
        self.db.lock().unwrap().get_raw_with_live(key).is_some()
    }

    pub fn insert(&self, url: &Url) -> Result<()> {
        let key = url.as_str().as_bytes().to_vec();
        let mut db = self.db.lock().unwrap();

        db.insert_raw(key, vec![]);

        if db.uncommitted_inserts() > 1_000_000 {
            db.commit()?;

            // TODO: Truncate the database using a ttl
        }

        Ok(())
    }
}
