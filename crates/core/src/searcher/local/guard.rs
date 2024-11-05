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
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::sync::Arc;
use tokio::sync::OwnedRwLockReadGuard;

use crate::index::Index;
use crate::inverted_index::InvertedIndex;
use crate::live_index;

pub trait SearchGuard: Send + Sync {
    fn search_index(&self) -> &Index;
    fn inverted_index(&self) -> &InvertedIndex {
        &self.search_index().inverted_index
    }
}

pub struct NormalIndexSearchGuard {
    search_index: Arc<Index>,
}

impl NormalIndexSearchGuard {
    pub fn new(search_index: Arc<Index>) -> Self {
        Self { search_index }
    }
}

impl SearchGuard for NormalIndexSearchGuard {
    fn search_index(&self) -> &Index {
        self.search_index.as_ref()
    }
}

pub struct LiveIndexSearchGuard {
    lock_guard: OwnedRwLockReadGuard<live_index::index::InnerIndex>,
}

impl LiveIndexSearchGuard {
    pub fn new(lock_guard: OwnedRwLockReadGuard<live_index::index::InnerIndex>) -> Self {
        Self { lock_guard }
    }
}

impl SearchGuard for LiveIndexSearchGuard {
    fn search_index(&self) -> &Index {
        self.lock_guard.index()
    }
}
