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
    sync::Arc,
};

use tokio::sync::{OwnedRwLockReadGuard, RwLock};

use chrono::{DateTime, NaiveDate, Utc};
use itertools::Itertools;
use simple_wal::Wal;
use tantivy::{
    index::SegmentId,
    indexer::{MergeOperation, SegmentEntry},
};

use std::collections::{HashMap, HashSet};

use crate::{
    config::SnippetConfig,
    entrypoint::indexer::{self, IndexableWebpage, IndexingWorker},
    inverted_index::InvertedIndex,
    live_index::{BATCH_SIZE, TTL},
    Result,
};

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct Segment {
    id: SegmentId,
    created: DateTime<Utc>,
}

impl Segment {
    pub fn id(&self) -> SegmentId {
        self.id
    }

    pub fn created(&self) -> DateTime<Utc> {
        self.created
    }
}

pub struct CompactOperation {
    segments: Vec<Segment>,
    entry: Option<SegmentEntry>,
    merge_op: MergeOperation,
}

impl CompactOperation {
    pub fn end(self, index: &mut InvertedIndex) -> Result<Option<SegmentId>> {
        let res = index.end_merge_segments_by_id(self.merge_op, self.entry)?;
        Ok(res)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
pub struct Meta {
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
            .truncate(true)
            .open(path)
            .unwrap();
        let writer = BufWriter::new(file);

        serde_json::to_writer_pretty(writer, self).unwrap();
    }

    pub fn segments(&self) -> &[Segment] {
        &self.segments
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
    pub async fn new<P: AsRef<Path>>(
        path: P,
        indexer_worker_config: indexer::worker::Config,
    ) -> Result<Self> {
        let mut index = crate::index::Index::open(path.as_ref())?;
        index.prepare_writer()?;

        let write_ahead_log = Wal::open(path.as_ref().join("wal"))?;
        let wal_count = write_ahead_log.iter()?.count();

        let worker = IndexingWorker::new(indexer_worker_config).await;

        let meta = Meta::open_or_create(path.as_ref().join("meta.json"));

        Ok(Self {
            index,
            write_ahead_log,
            indexing_worker: worker,
            has_inserts: wal_count > 0,
            meta,
            path: path.as_ref().to_path_buf(),
        })
    }

    pub fn prune_segments(&mut self) {
        let old_segments: Vec<_> = self
            .meta
            .segments
            .iter()
            .filter_map(|segment| {
                if segment.created + TTL < Utc::now() {
                    Some(segment.id)
                } else {
                    None
                }
            })
            .collect();

        self.index
            .inverted_index
            .delete_segments_by_id(&old_segments)
            .unwrap();

        self.sync_meta_with_index();
        self.re_open();
    }

    pub async fn start_compact_segments_by_date(&self) -> Result<Vec<CompactOperation>> {
        let segments_to_compact = self.prepare_segments_for_compaction();

        let mut operations = Vec::new();

        for (_, segments) in segments_to_compact {
            if segments.len() <= 1 {
                continue;
            }

            let segment_ids: Vec<SegmentId> = segments.iter().map(|s| s.id).collect();

            let (entry, merge_op) = self
                .index
                .inverted_index
                .start_merge_segments_by_id(&segment_ids)
                .await?;

            operations.push(CompactOperation {
                segments,
                entry,
                merge_op,
            });
        }

        Ok(operations)
    }

    fn end_compact_segments_by_date(&mut self, operations: Vec<CompactOperation>) -> Result<()> {
        for op in operations {
            let newest_creation_date = op.segments.iter().map(|s| s.created).max().unwrap();
            let segment_ids: Vec<SegmentId> = op.segments.iter().map(|s| s.id).collect();

            if let Ok(Some(new_segment_id)) = op.end(&mut self.index.inverted_index) {
                self.update_meta_after_compaction(
                    segment_ids,
                    new_segment_id,
                    newest_creation_date,
                );
            }
        }

        self.save_meta();
        self.re_open();

        Ok(())
    }

    fn prepare_segments_for_compaction(&self) -> HashMap<NaiveDate, Vec<Segment>> {
        let mut segments_by_date: HashMap<NaiveDate, Vec<Segment>> = HashMap::new();

        for segment in self.meta.segments.clone() {
            segments_by_date
                .entry(segment.created.date_naive())
                .or_default()
                .push(segment);
        }

        segments_by_date
    }

    fn update_meta_after_compaction(
        &mut self,
        old_segment_ids: Vec<SegmentId>,
        new_segment_id: SegmentId,
        newest_creation_date: DateTime<Utc>,
    ) {
        self.meta
            .segments
            .retain(|s| !old_segment_ids.contains(&s.id));
        self.meta.segments.push(Segment {
            id: new_segment_id,
            created: newest_creation_date,
        });
    }

    fn re_open(&mut self) {
        self.index.inverted_index.re_open().unwrap();
        self.index.prepare_writer().unwrap();
    }

    fn sync_meta_with_index(&mut self) {
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
            .filter(|segment| !segments_in_meta.contains(segment))
        {
            self.meta.segments.push(Segment {
                id,
                created: Utc::now(),
            })
        }

        self.save_meta();
    }

    fn save_meta(&self) {
        self.meta.save(self.path.join("meta.json"));
    }

    pub fn index(&self) -> &crate::index::Index {
        &self.index
    }

    pub fn delete_all_pages(&mut self) {
        let segments = self.index.inverted_index.segment_ids();
        self.index
            .inverted_index
            .delete_segments_by_id(&segments)
            .unwrap();

        self.meta = Meta::default();
        self.save_meta();
        self.re_open();
    }

    pub fn insert(&mut self, pages: &[IndexableWebpage]) {
        self.write_ahead_log.batch_write(pages.iter()).unwrap();
        self.has_inserts = true;
    }

    pub async fn commit(&mut self) {
        for batch in self
            .write_ahead_log
            .iter()
            .unwrap()
            .unique_by(|page| page.url.clone())
            .chunks(BATCH_SIZE)
            .into_iter()
        {
            let batch: Vec<_> = batch.collect();
            for webpage in self.indexing_worker.prepare_webpages(&batch).await {
                self.index.insert(&webpage).unwrap();
            }
        }
        self.index.commit().unwrap();
        self.write_ahead_log.clear().unwrap();
        self.sync_meta_with_index();
        self.has_inserts = false;
        self.re_open();
    }

    pub fn has_inserts(&self) -> bool {
        self.has_inserts
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn meta(&self) -> &Meta {
        &self.meta
    }
}

pub struct LiveIndex {
    inner: Arc<RwLock<InnerIndex>>,
}

impl LiveIndex {
    pub async fn new<P: AsRef<Path>>(
        path: P,
        indexer_worker_config: indexer::worker::Config,
    ) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(RwLock::new(
                InnerIndex::new(path, indexer_worker_config).await?,
            )),
        })
    }

    pub async fn commit(&self) {
        tracing::debug!("committing index");
        self.inner.write().await.commit().await;
    }

    pub async fn prune_segments(&self) {
        tracing::debug!("pruning segments");
        self.inner.write().await.prune_segments()
    }

    pub async fn has_inserts(&self) -> bool {
        self.inner.read().await.has_inserts()
    }

    pub async fn compact_segments_by_date(&self) -> Result<()> {
        tracing::debug!("compacting segments by date");
        let operations = self
            .inner
            .read()
            .await
            .start_compact_segments_by_date()
            .await?;

        self.inner
            .write()
            .await
            .end_compact_segments_by_date(operations)?;

        Ok(())
    }

    pub async fn insert(&self, pages: &[IndexableWebpage]) {
        tracing::debug!("inserting {} pages into index", pages.len());
        self.inner.write().await.insert(pages)
    }

    pub async fn read(&self) -> OwnedRwLockReadGuard<InnerIndex> {
        RwLock::read_owned(self.inner.clone()).await
    }

    pub async fn set_snippet_config(&self, config: SnippetConfig) {
        self.inner
            .write()
            .await
            .index
            .inverted_index
            .set_snippet_config(config)
    }

    pub async fn path(&self) -> PathBuf {
        self.inner.read().await.path.to_path_buf()
    }

    pub async fn delete_all_pages(&self) {
        self.inner.write().await.delete_all_pages();
    }

    pub async fn re_open(&self) -> Result<()> {
        self.inner.write().await.re_open();

        Ok(())
    }

    pub async fn meta(&self) -> Meta {
        self.inner.read().await.meta.clone()
    }
}
