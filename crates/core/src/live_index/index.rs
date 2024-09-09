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

use chrono::{DateTime, NaiveDate, Utc};
use itertools::Itertools;
use simple_wal::Wal;
use tantivy::index::SegmentId;

use std::collections::{HashMap, HashSet};

use crate::{
    config::{LiveIndexConfig, SnippetConfig},
    entrypoint::indexer::{IndexableWebpage, IndexingWorker},
    live_index::{BATCH_SIZE, TTL},
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
    indexing_worker: IndexingWorker,
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

        let worker = IndexingWorker::new(config.clone());

        let meta = Meta::open_or_create(path.join("meta.json"));

        Ok(Self {
            index,
            write_ahead_log,
            indexing_worker: worker,
            has_inserts: wal_count > 0,
            meta,
            path: path.to_path_buf(),
        })
    }

    pub fn prune_segments(&mut self) {
        let old_segments: Vec<_> = self
            .meta
            .segments
            .iter()
            .filter_map(|segment| {
                if segment.created + TTL < Utc::now() {
                    Some(segment.id.clone())
                } else {
                    None
                }
            })
            .collect();

        self.index
            .inverted_index
            .delete_segments_by_id(&old_segments)
            .unwrap();

        self.update_meta();
    }

    pub fn compact_segments_by_date(&mut self) {
        let mut segments_by_date: HashMap<NaiveDate, Vec<SegmentId>> = HashMap::new();

        for segment in self.meta.segments.clone() {
            segments_by_date
                .entry(segment.created.date_naive())
                .or_default()
                .push(segment.id.clone());
        }

        for (_, segments) in segments_by_date {
            if segments.len() <= 1 {
                continue;
            }

            self.index
                .inverted_index
                .merge_segments_by_id(&segments)
                .unwrap();
        }

        self.update_meta();
    }

    fn update_meta(&mut self) {
        let segments_in_index: HashSet<_> = self
            .index
            .inverted_index
            .segment_ids()
            .into_iter()
            .collect();

        let segments_in_meta: HashSet<_> = self
            .meta
            .segments
            .clone()
            .into_iter()
            .map(|segment| segment.id)
            .collect();

        // remove all segments from meta that is not present in the index
        let to_remove: HashSet<_> = segments_in_meta
            .iter()
            .filter(|segment| !segments_in_index.contains(segment))
            .collect();

        self.meta.segments = self
            .meta
            .segments
            .clone()
            .into_iter()
            .filter(|segment| !to_remove.contains(&segment.id))
            .collect();

        // insert all segments from index that is not already in meta
        for id in segments_in_index
            .into_iter()
            .filter(|segment| !segments_in_meta.contains(&segment))
        {
            self.meta.segments.push(Segment {
                id,
                created: Utc::now(),
            })
        }

        self.meta.save(self.path.join("meta.json"));
    }

    pub fn index(&self) -> &crate::index::Index {
        &self.index
    }

    pub fn insert(&mut self, pages: &[IndexableWebpage]) {
        self.write_ahead_log.batch_write(pages.iter()).unwrap();
        self.has_inserts = true;
    }

    pub fn commit(&mut self) {
        for batch in self
            .write_ahead_log
            .iter()
            .unwrap()
            .chunks(BATCH_SIZE)
            .into_iter()
        {
            let batch: Vec<_> = batch.collect();
            for webpage in self.indexing_worker.prepare_webpages(&batch) {
                self.index.insert(&webpage).unwrap();
            }
        }
        self.index.commit().unwrap();
        self.update_meta();
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

    pub fn compact_segments_by_date(&self) {
        self.inner
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .compact_segments_by_date()
    }

    pub fn insert(&self, pages: &[IndexableWebpage]) {
        self.inner
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(pages)
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
