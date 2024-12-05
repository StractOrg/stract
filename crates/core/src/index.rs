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

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::inverted_index::{self, InvertedIndex, ShardId};
use crate::query::Query;
use crate::webpage::region::{Region, RegionCount};
use crate::webpage::Webpage;
use crate::Result;

const INVERTED_INDEX_SUBFOLDER_NAME: &str = "inverted_index";
const REGION_COUNT_FILE_NAME: &str = "region_count.json";

pub struct Index {
    pub inverted_index: InvertedIndex,
    pub region_count: Mutex<RegionCount>,
    path: String,
}

impl Index {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        if !path.as_ref().exists() {
            fs::create_dir_all(path.as_ref())?;
        }

        let inverted_index =
            InvertedIndex::open(path.as_ref().join(INVERTED_INDEX_SUBFOLDER_NAME))?;

        let region_count = RegionCount::open(path.as_ref().join(REGION_COUNT_FILE_NAME));

        Ok(Self {
            inverted_index,
            region_count: Mutex::new(region_count),
            path: path.as_ref().to_str().unwrap().to_string(),
        })
    }

    pub fn inverted_index(&self) -> &InvertedIndex {
        &self.inverted_index
    }

    pub fn region_count(&self) -> &Mutex<RegionCount> {
        &self.region_count
    }

    pub fn path(&self) -> PathBuf {
        PathBuf::from(&self.path)
    }

    pub fn set_auto_merge_policy(&mut self) {
        self.inverted_index.set_auto_merge_policy();
    }

    pub fn set_shard_id(&mut self, shard_id: ShardId) {
        self.inverted_index.set_shard_id(shard_id);
    }

    pub fn shard_id(&self) -> Option<ShardId> {
        self.inverted_index.shard_id()
    }

    #[cfg(test)]
    pub fn temporary() -> Result<(Self, file_store::temp::TempDir)> {
        let dir = crate::gen_temp_dir()?;
        let mut s = Self::open(&dir)?;
        s.set_shard_id(ShardId::Backbone(0));

        s.prepare_writer()?;

        Ok((s, dir))
    }

    pub fn insert(&self, webpage: &Webpage) -> Result<()> {
        if let Ok(region) = Region::guess_from(webpage) {
            let mut reg = self.region_count.lock().unwrap_or_else(|e| e.into_inner());
            reg.increment(&region);
        }

        self.inverted_index.insert(webpage)
    }

    pub fn commit(&mut self) -> Result<()> {
        self.inverted_index.commit()?;

        let mut reg = self.region_count.lock().unwrap_or_else(|e| e.into_inner());
        reg.commit();

        Ok(())
    }

    pub fn retrieve_websites(
        &self,
        websites: &[inverted_index::WebpagePointer],
        query: &Query,
    ) -> Result<Vec<inverted_index::RetrievedWebpage>> {
        self.inverted_index.retrieve_websites(websites, query)
    }

    pub fn merge(self, other: Self) -> Self {
        let _ = self.inverted_index.merge(other.inverted_index);

        let mut self_region_count = self
            .region_count
            .into_inner()
            .unwrap_or_else(|e| e.into_inner());
        let other_region_count = other
            .region_count
            .into_inner()
            .unwrap_or_else(|e| e.into_inner());

        self_region_count.merge(other_region_count);

        let mut res = Self::open(&self.path).expect("failed to open index");
        res.prepare_writer().expect("failed to prepare writer");
        res
    }

    pub(crate) fn prepare_writer(&mut self) -> Result<()> {
        self.inverted_index.prepare_writer()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::RwLock;

    use crate::{
        ranking,
        searcher::{LocalSearcher, SearchQuery},
    };

    use super::*;

    const CONTENT: &str = "this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever";

    #[test]
    fn bm25_all_docs() {
        let (mut index, _dir) = Index::temporary().expect("Unable to open index");

        index
            .insert(
                &Webpage::test_parse(
                    &format!(
                        r#"
            <html>
                <head>
                    <title>Test website</title>
                </head>
                <body>
                    {CONTENT} {}
                </body>
            </html>
            "#,
                        crate::rand_words(100)
                    ),
                    "https://www.first.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index
            .insert(
                &Webpage::test_parse(
                    &format!(
                        r#"
            <html>
                <head>
                    <title>Test test website</title>
                </head>
                <body>
                    {CONTENT} {}
                </body>
            </html>
            "#,
                        crate::rand_words(100)
                    ),
                    "https://www.second.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        index
            .insert(
                &Webpage::test_parse(
                    &format!(
                        r#"
            <html>
                <head>
                    <title>Test test test website</title>
                </head>
                <body>
                    {CONTENT} {}
                </body>
            </html>
            "#,
                        crate::rand_words(100)
                    ),
                    "https://www.third.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");

        index.commit().unwrap();

        let searcher = LocalSearcher::builder(Arc::new(RwLock::new(index))).build();
        let res = searcher
            .search_sync(&SearchQuery {
                query: "test".to_string(),
                return_ranking_signals: true,
                ..Default::default()
            })
            .unwrap();

        assert!(res
            .webpages
            .iter()
            .map(|d| d
                .ranking_signals
                .as_ref()
                .unwrap()
                .get(&crate::ranking::SignalEnum::from(ranking::signals::Bm25Title).into())
                .unwrap())
            .all(|&v| v.value > 0.0));
    }
}
