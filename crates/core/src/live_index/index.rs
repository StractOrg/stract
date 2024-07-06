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

use std::{
    path::Path,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use crate::Result;

pub struct Index {
    search_index: Arc<RwLock<crate::index::Index>>,
}

impl Index {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut search_index = crate::index::Index::open(path.as_ref().join("index"))?;
        search_index.prepare_writer()?;
        search_index.set_auto_merge_policy();

        let search_index = Arc::new(RwLock::new(search_index));

        Ok(Self { search_index })
    }

    pub fn commit(&self) {
        self.search_index
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .commit()
            .ok();
    }

    pub fn prune(&self) {
        todo!("delete index files older than TTL")
    }

    pub fn clone_inner_index(&self) -> Arc<RwLock<crate::index::Index>> {
        self.search_index.clone()
    }

    pub fn read(&self) -> RwLockReadGuard<'_, crate::index::Index> {
        self.search_index.read().unwrap_or_else(|e| e.into_inner())
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, crate::index::Index> {
        self.search_index.write().unwrap_or_else(|e| e.into_inner())
    }
}
