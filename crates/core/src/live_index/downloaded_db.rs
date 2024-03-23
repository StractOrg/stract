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

use crate::Result;
use std::path::Path;

use super::TTL;

pub struct DownloadedDb {
    db: rocksdb::DB,
}

impl DownloadedDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);

        // some recommended settings (https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning)
        options.set_level_compaction_dynamic_level_bytes(true);
        options.set_bytes_per_sync(1048576);
        let mut block_options = rocksdb::BlockBasedOptions::default();
        block_options.set_block_size(16 * 1024);
        block_options.set_format_version(5);
        block_options.set_cache_index_and_filter_blocks(true);
        block_options.set_pin_l0_filter_and_index_blocks_in_cache(true);

        block_options.set_ribbon_filter(10.0);

        options.set_block_based_table_factory(&block_options);
        options.optimize_for_point_lookup(512); // 512 mb

        let db = rocksdb::DB::open_with_ttl(&options, path, TTL)?;
        Ok(Self { db })
    }

    pub fn has_downloaded(&self, url: &Url) -> Result<bool> {
        let key = url.as_str().as_bytes();

        if self.db.key_may_exist(key) {
            self.db.get(key).map(|v| v.is_some()).map_err(Into::into)
        } else {
            Ok(false)
        }
    }

    pub fn insert(&self, url: &Url) -> Result<()> {
        let key = url.as_str().as_bytes();
        self.db.put(key, b"")?;
        Ok(())
    }
}
