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

use std::sync::{Arc, Mutex, RwLock};

use crate::{
    config::LiveIndexConfig,
    crawler::{self, CrawlDatum, DatumStream},
    entrypoint::indexer::IndexingWorker,
    Result,
};

use super::BATCH_SIZE;

pub struct Indexer {
    search_index: Arc<RwLock<crate::index::Index>>,
    worker: IndexingWorker,
    write_batch: Arc<Mutex<Vec<crate::entrypoint::indexer::IndexableWebpage>>>,
}

impl Indexer {
    pub fn new(search_index: Arc<RwLock<crate::index::Index>>, config: LiveIndexConfig) -> Self {
        Self {
            search_index,
            worker: IndexingWorker::new(config),
            write_batch: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn maybe_write_batch_to_index(&self) {
        let batch = self.write_batch.lock().unwrap_or_else(|e| e.into_inner());

        if batch.len() < BATCH_SIZE {
            return;
        }

        let prepared = self.worker.prepare_webpages(&batch);

        let search_index = self.search_index.write().unwrap_or_else(|e| e.into_inner());
        for webpage in &prepared {
            search_index.insert(webpage).ok();
        }
    }

    pub fn write_batch_to_index(&self) {
        let batch = self.write_batch.lock().unwrap_or_else(|e| e.into_inner());

        if batch.is_empty() {
            return;
        }

        let prepared = self.worker.prepare_webpages(&batch);

        let search_index = self.search_index.write().unwrap_or_else(|e| e.into_inner());
        for webpage in &prepared {
            search_index.insert(webpage).ok();
        }
    }
}

impl DatumStream for Indexer {
    async fn write(&self, crawl_datum: CrawlDatum) -> Result<(), crawler::Error> {
        self.write_batch
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(crawl_datum.into());

        self.maybe_write_batch_to_index();

        Ok(())
    }

    async fn finish(&self) -> Result<(), crawler::Error> {
        self.write_batch_to_index();

        self.search_index
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .commit()?;

        Ok(())
    }
}
