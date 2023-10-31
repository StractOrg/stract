// Stract is an open source web search engine.
// Copyright (C) 2023 Stract ApS
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
use std::path::Path;
use std::sync::Arc;
use std::{collections::HashSet, path::PathBuf};

use serde::{Deserialize, Serialize};
use stdx::directory::{self, DirEntry};
use tantivy::schema::Schema;
use tantivy::tokenizer::TokenizerManager;
use url::Url;
use webgraph::NodeID;
use webpage::{
    region::{Region, RegionCount},
    Webpage,
};

use crate::inverted_index::{self, InvertedIndex};
use crate::query::Query;
use crate::search_ctx::Ctx;
use crate::subdomain_count::SubdomainCounter;
use crate::MainCollector;
use crate::Result;

const INVERTED_INDEX_SUBFOLDER_NAME: &str = "inverted_index";
const REGION_COUNT_FILE_NAME: &str = "region_count.json";
const SUBDOMAIN_COUNT_SUBFOLDER_NAME: &str = "subdomain_count";

pub struct Index {
    pub inverted_index: InvertedIndex,
    pub region_count: RegionCount,
    pub subdomain_counter: SubdomainCounter,
    pub path: PathBuf,
}

impl Index {
    pub fn open(path: &Path) -> Result<Self> {
        if !path.exists() {
            fs::create_dir_all(path)?;
        }

        let inverted_index = InvertedIndex::open(&path.join(INVERTED_INDEX_SUBFOLDER_NAME))?;

        let region_count = RegionCount::open(&path.join(REGION_COUNT_FILE_NAME));

        Ok(Self {
            inverted_index,
            region_count,
            subdomain_counter: SubdomainCounter::open(&path.join(SUBDOMAIN_COUNT_SUBFOLDER_NAME)),
            path: path.to_owned(),
        })
    }

    pub fn optimize_for_search(&mut self) -> Result<()> {
        self.inverted_index.optimize_for_search()?;

        Ok(())
    }

    pub fn tokenizers(&self) -> &TokenizerManager {
        self.inverted_index.tokenizers()
    }

    #[cfg(test)]
    pub fn temporary() -> Result<Self> {
        let path = stdx::gen_temp_path();
        Self::open(&path)
    }

    pub fn insert(&mut self, webpage: Webpage) -> Result<()> {
        self.subdomain_counter.increment(webpage.html.url().clone());

        if let Ok(region) = Region::guess_from(&webpage) {
            self.region_count.increment(&region);
        }

        self.inverted_index.insert(webpage)
    }

    pub fn commit(&mut self) -> Result<()> {
        self.inverted_index.commit()?;
        self.region_count.commit();
        self.subdomain_counter.commit();
        Ok(())
    }

    pub fn top_nodes(
        &self,
        query: &Query,
        ctx: &Ctx,
        collector: MainCollector,
    ) -> Result<Vec<NodeID>> {
        let websites = self
            .inverted_index
            .search_initial(query, ctx, collector)?
            .top_websites;

        let mut hosts = HashSet::with_capacity(websites.len());
        for website in &websites {
            if let Some(id) = self.inverted_index.website_host_node(website)? {
                hosts.insert(id);
            }
        }
        Ok(hosts.into_iter().collect())
    }

    pub fn retrieve_websites(
        &self,
        websites: &[collector::WebsitePointer],
        query: &Query,
    ) -> Result<Vec<inverted_index::RetrievedWebpage>> {
        self.inverted_index.retrieve_websites(websites, query)
    }

    pub fn merge(mut self, other: Self) -> Self {
        self.inverted_index.merge(other.inverted_index);

        self.region_count.merge(other.region_count);

        self.subdomain_counter.merge(other.subdomain_counter);
        drop(self.subdomain_counter);

        Self::open(self.path.as_ref()).expect("failed to open index")
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.inverted_index.schema()
    }

    pub fn num_segments(&self) -> usize {
        self.inverted_index.num_segments()
    }

    pub(crate) fn get_webpage(&self, url: &str) -> Option<inverted_index::RetrievedWebpage> {
        self.inverted_index.get_webpage(url)
    }

    pub fn get_homepage(&self, url: &Url) -> Option<inverted_index::RetrievedWebpage> {
        self.inverted_index.get_homepage(url)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FrozenIndex {
    pub root: DirEntry,
}

impl From<FrozenIndex> for Index {
    fn from(frozen: FrozenIndex) -> Self {
        let path = match &frozen.root {
            DirEntry::Folder { name, entries: _ } => name.clone(),
            DirEntry::File {
                name: _,
                content: _,
            } => {
                panic!("Cannot open index from a file - must be directory.")
            }
        };

        if Path::new(&path).exists() {
            fs::remove_dir_all(&path).unwrap();
        }

        directory::recreate_folder(&frozen.root).unwrap();
        Index::open(path.as_ref()).expect("failed to open index")
    }
}

impl From<Index> for FrozenIndex {
    fn from(mut index: Index) -> Self {
        index.commit().expect("failed to commit index");
        let path = index.path.clone();
        index.inverted_index.stop();
        let root = directory::scan_folder(path.to_str().unwrap().to_string()).unwrap();

        Self { root }
    }
}

#[cfg(test)]
mod tests {
    use crate::searcher::{LocalSearcher, SearchQuery};

    use super::*;

    const CONTENT: &str = "this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever";

    #[test]
    fn serialize_deserialize_bincode() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(
                Webpage::new(
                    &format!(
                        r#"
            <html>
                <head>
                    <title>Test website</title>
                </head>
                <body>
                    {CONTENT}
                </body>
            </html>
            "#
                    ),
                    "https://www.example.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");

        index.commit().unwrap();

        let path = index.path.clone();
        let frozen: FrozenIndex = index.into();
        let bytes = bincode::serialize(&frozen).unwrap();

        std::fs::remove_dir_all(path).unwrap();

        let deserialized_frozen: FrozenIndex = bincode::deserialize(&bytes).unwrap();
        let index: Index = deserialized_frozen.into();
        let searcher = LocalSearcher::from(index);

        let result = searcher
            .search(&SearchQuery {
                query: "website".to_string(),
                ..Default::default()
            })
            .expect("Search failed");

        assert_eq!(result.webpages.len(), 1);
        assert_eq!(result.webpages[0].url, "https://www.example.com/");
    }

    #[test]
    fn bm25_all_docs() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(
                Webpage::new(
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
                Webpage::new(
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
                Webpage::new(
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

        let searcher = LocalSearcher::from(index);
        let res = searcher
            .search(&SearchQuery {
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
                .get(&crate::ranking::Signal::Bm25Title)
                .unwrap())
            .all(|&v| v.value > 0.0));
    }
}
