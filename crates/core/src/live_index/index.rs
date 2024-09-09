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
    io::{BufReader, BufWriter},
    path::{Path, PathBuf},
    sync::{Arc, RwLock, RwLockReadGuard},
};

use chrono::{DateTime, Utc};
use simple_wal::Wal;
use tantivy::index::SegmentId;

use crate::{
    config::{LiveIndexConfig, SnippetConfig},
    entrypoint::feed_indexer::IndexingWorker,
    searcher::SearchableIndex,
    Result,
};

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct Segment {
    id: SegmentId,
    created: DateTime<Utc>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
struct Meta {
    segments: Vec<Segment>,
}

impl Meta {
    pub fn open_or_create<P: AsRef<Path>>(path: P) -> Self {
        if !path.as_ref().exists() {
            let meta = Meta::default();
            meta.save(path);

            meta
        } else {
            let file = std::fs::File::open(path).unwrap();
            let reader = BufReader::new(file);
            serde_json::from_reader(reader).unwrap()
        }
    }

    pub fn save<P: AsRef<Path>>(&self, path: P) {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)
            .unwrap();
        let writer = BufWriter::new(file);

        serde_json::to_writer_pretty(writer, self).unwrap();
    }
}

pub struct InnerIndex {
    index: crate::index::Index,
    write_ahead_log: Wal<crate::entrypoint::indexer::IndexableWebpage>,
    has_inserts: bool,
    worker: IndexingWorker,
    path: PathBuf,
    meta: Meta,
}

impl InnerIndex {
    pub fn new(config: LiveIndexConfig) -> Result<Self> {
        let path = Path::new(&config.index_path);
        let mut index = crate::index::Index::open(path.join("index"))?;
        index.prepare_writer()?;

        let write_ahead_log = Wal::open(path.join("wal"))?;
        let wal_count = write_ahead_log.iter()?.count();

        let worker = IndexingWorker;

        let meta = Meta::open_or_create(path.join("meta.json"));

        Ok(Self {
            index,
            write_ahead_log,
            worker,
            has_inserts: wal_count > 0,
            meta,
            path: path.to_path_buf(),
        })
    }

    pub fn prune_segments(&mut self) {
        todo!("delete index segments older than TTL")
    }

    pub fn compact_todays_segments(&mut self) {
        todo!("compact all segments from today")
    }

    pub fn index(&self) -> &crate::index::Index {
        &self.index
    }

    pub fn insert(&mut self, webpages: &[crate::entrypoint::indexer::IndexableWebpage]) {
        self.has_inserts = true;
        todo!("insert into wal")
    }

    pub fn commit(&mut self) {
        todo!("index all wal entries");
        todo!("commit inner index");
        self.has_inserts = false;
    }

    pub fn has_inserts(&self) -> bool {
        self.has_inserts
    }
}

pub struct LiveIndex {
    inner: Arc<RwLock<InnerIndex>>,
}

impl LiveIndex {
    pub fn new(config: LiveIndexConfig) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(RwLock::new(InnerIndex::new(config)?)),
        })
    }

    pub fn commit(&self) {
        self.inner
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .commit();
    }

    pub fn prune_segments(&self) {
        self.inner
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .prune_segments()
    }

    pub fn has_inserts(&self) -> bool {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .has_inserts()
    }

    pub fn compact_todays_segments(&self) {
        self.inner
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .compact_todays_segments()
    }

    pub fn read(&self) -> RwLockReadGuard<'_, InnerIndex> {
        self.inner.read().unwrap_or_else(|e| e.into_inner())
    }

    pub fn set_snippet_config(&self, config: SnippetConfig) {
        self.inner
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .index
            .set_snippet_config(config)
    }
}
