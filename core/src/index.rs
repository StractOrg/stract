// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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

use std::cmp::Ordering;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tantivy::collector::Collector;
use tantivy::schema::Schema;
use tantivy::tokenizer::TokenizerManager;
use uuid::Uuid;

use crate::directory::{self, DirEntry};
use crate::image_downloader::{ImageDownloadJob, ImageDownloader};
use crate::image_store::{FaviconStore, Image, ImageStore, PrimaryImageStore};
use crate::inverted_index::{self, InitialSearchResult, InvertedIndex};
use crate::query::Query;
use crate::spell::{
    Correction, CorrectionTerm, Dictionary, LogarithmicEdit, SpellChecker, TermSplitter,
};
use crate::subdomain_count::SubdomainCounter;
use crate::webgraph::NodeID;
use crate::webpage::region::{Region, RegionCount};
use crate::webpage::{Url, Webpage};
use crate::Result;

const INVERTED_INDEX_SUBFOLDER_NAME: &str = "inverted_index";
const FAVICON_STORE_SUBFOLDER_NAME: &str = "favicon_store";
const PRIMARY_IMAGE_STORE_SUBFOLDER_NAME: &str = "primary_image_store";
const SPELL_SUBFOLDER_NAME: &str = "primary_image_store";
const REGION_COUNT_FILE_NAME: &str = "region_count.json";
const SUBDOMAIN_COUNT_SUBFOLDER_NAME: &str = "subdomain_count";
const IMAGE_WEBPAGE_CENTRALITY_THRESHOLD: f64 = 0.0;

pub struct Index {
    pub inverted_index: InvertedIndex,
    favicon_store: FaviconStore,
    primary_image_store: PrimaryImageStore,
    favicon_downloader: ImageDownloader<String>,
    primary_image_downloader: ImageDownloader<Uuid>,
    spell_dictionary: Dictionary<100_000>,
    pub region_count: RegionCount,
    pub subdomain_counter: SubdomainCounter,
    pub path: String,
}

impl Index {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        if !path.as_ref().exists() {
            fs::create_dir_all(path.as_ref())?;
        }

        let favicon_store = FaviconStore::open(path.as_ref().join(FAVICON_STORE_SUBFOLDER_NAME));
        let primary_image_store =
            PrimaryImageStore::open(path.as_ref().join(PRIMARY_IMAGE_STORE_SUBFOLDER_NAME));

        let inverted_index =
            InvertedIndex::open(path.as_ref().join(INVERTED_INDEX_SUBFOLDER_NAME))?;

        let region_count = RegionCount::open(path.as_ref().join(REGION_COUNT_FILE_NAME));

        Ok(Self {
            inverted_index,
            favicon_store,
            primary_image_store,
            region_count,
            primary_image_downloader: ImageDownloader::new(),
            favicon_downloader: ImageDownloader::new(),
            spell_dictionary: Dictionary::open(Some(path.as_ref().join(SPELL_SUBFOLDER_NAME)))?,
            subdomain_counter: SubdomainCounter::open(
                path.as_ref().join(SUBDOMAIN_COUNT_SUBFOLDER_NAME),
            ),
            path: path.as_ref().to_str().unwrap().to_string(),
        })
    }

    pub fn tokenizers(&self) -> &TokenizerManager {
        self.inverted_index.tokenizers()
    }

    #[cfg(test)]
    pub fn temporary() -> Result<Self> {
        let path = crate::gen_temp_path();
        Self::open(path)
    }

    pub fn insert(&mut self, mut webpage: Webpage) -> Result<()> {
        self.maybe_insert_favicon(&webpage);
        self.maybe_insert_primary_image(&mut webpage);
        self.spell_dictionary.insert_page(&webpage);
        self.subdomain_counter.increment(webpage.html.url().clone());

        if let Ok(region) = Region::guess_from(&webpage) {
            self.region_count.increment(&region);
        }

        self.inverted_index.insert(webpage)
    }

    pub fn commit(&mut self) -> Result<()> {
        self.spell_dictionary.commit()?;
        self.inverted_index.commit()?;
        self.region_count.commit();
        self.subdomain_counter.commit();
        Ok(())
    }

    pub fn search_initial<C>(&self, query: &Query, collector: C) -> Result<InitialSearchResult>
    where
        C: Collector<Fruit = Vec<inverted_index::WebsitePointer>>,
    {
        self.inverted_index.search_initial(query, collector)
    }

    pub fn top_nodes<C>(&self, query: &Query, collector: C) -> Result<Vec<NodeID>>
    where
        C: Collector<Fruit = Vec<inverted_index::WebsitePointer>>,
    {
        let websites = self
            .inverted_index
            .search_initial(query, collector)?
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
        websites: &[inverted_index::WebsitePointer],
        query: &Query,
    ) -> Result<Vec<inverted_index::RetrievedWebpage>> {
        self.inverted_index.retrieve_websites(websites, query)
    }

    pub fn merge(mut self, other: Self) -> Self {
        self.inverted_index.merge(other.inverted_index);

        self.favicon_store.merge(other.favicon_store);
        drop(self.favicon_store);

        self.primary_image_store.merge(other.primary_image_store);
        drop(self.primary_image_store);

        self.spell_dictionary.merge(other.spell_dictionary);

        self.region_count.merge(other.region_count);

        self.subdomain_counter.merge(other.subdomain_counter);
        drop(self.subdomain_counter);

        Self::open(&self.path).expect("failed to open index")
    }

    fn maybe_insert_favicon(&mut self, webpage: &Webpage) {
        if !webpage.html.url().is_homepage()
            || self
                .favicon_store
                .contains(&webpage.html.url().domain().to_string())
        {
            return;
        }

        if let Some(favicon) = webpage.html.favicon() {
            if favicon.link.is_valid_uri() {
                self.favicon_downloader.schedule(ImageDownloadJob {
                    key: favicon.link.domain().to_string(),
                    urls: vec![favicon.link],
                    timeout: Some(Duration::from_secs(1)),
                });
            }
        }
    }

    fn maybe_insert_primary_image(&mut self, webpage: &mut Webpage) {
        match webpage
            .host_centrality
            .partial_cmp(&IMAGE_WEBPAGE_CENTRALITY_THRESHOLD)
        {
            None | Some(Ordering::Greater) => {}
            _ => return,
        }

        if let Some(image) = webpage.html.primary_image() {
            let url = image.url.clone();
            if url.is_valid_uri() {
                let uuid = self.primary_image_store.generate_uuid();
                webpage.set_primary_image(uuid, image);

                self.primary_image_downloader.schedule(ImageDownloadJob {
                    key: uuid,
                    urls: vec![url],
                    timeout: Some(Duration::from_secs(5)),
                });
            }
        }
    }

    pub fn retrieve_favicon(&self, url: &Url) -> Option<Image> {
        self.favicon_store.get(&url.domain().to_string())
    }

    pub fn retrieve_primary_image(&self, uuid: &Uuid) -> Option<Image> {
        self.primary_image_store.get(uuid)
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.inverted_index.schema()
    }

    pub(crate) fn download_pending_images(&mut self) {
        self.favicon_downloader.download(&mut self.favicon_store);
        self.primary_image_downloader
            .download(&mut self.primary_image_store);
    }

    fn spell_check(&self, terms: &[String]) -> Option<Correction> {
        let spellchecker = SpellChecker::new(&self.spell_dictionary, LogarithmicEdit::new(4));

        let mut original = String::new();

        for term in terms {
            original.push_str(term);
            original.push(' ');
        }
        original = original.trim_end().to_string();

        let mut possible_correction = Correction::empty(original);

        for term in terms {
            match spellchecker.correct(term.to_ascii_lowercase().as_str()) {
                Some(correction) => possible_correction
                    .push(CorrectionTerm::Corrected(correction.to_ascii_lowercase())),
                None => possible_correction
                    .push(CorrectionTerm::NotCorrected(term.to_ascii_lowercase())),
            }
        }

        if possible_correction.is_all_orig() {
            None
        } else {
            Some(possible_correction)
        }
    }

    fn split_words(&self, terms: &[String]) -> Option<Correction> {
        let splitter = TermSplitter::new(&self.spell_dictionary);

        let mut original = String::new();

        for term in terms {
            original.push_str(term);
            original.push(' ');
        }
        original = original.trim_end().to_string();

        let mut possible_correction = Correction::empty(original);

        for term in terms {
            let t = term.to_ascii_lowercase();
            let split = splitter.split(t.as_str());
            if split.is_empty() {
                possible_correction.push(CorrectionTerm::NotCorrected(t));
            } else {
                for s in split {
                    possible_correction.push(CorrectionTerm::Corrected(s.to_string()))
                }
            }
        }

        if possible_correction.is_all_orig() {
            None
        } else {
            Some(possible_correction)
        }
    }

    pub fn spell_correction(&self, terms: &[String]) -> Option<Correction> {
        self.spell_check(terms).or_else(|| self.split_words(terms))
    }

    pub fn num_segments(&self) -> usize {
        self.inverted_index.num_segments()
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
        Index::open(path).expect("failed to open index")
    }
}

impl From<Index> for FrozenIndex {
    fn from(mut index: Index) -> Self {
        index.commit().expect("failed to commit index");
        let path = index.path.clone();
        index.inverted_index.stop();
        let root = directory::scan_folder(path).unwrap();

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
            .insert(Webpage::new(
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
            ))
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
                original: "website".to_string(),
                ..Default::default()
            })
            .expect("Search failed")
            .into_websites()
            .unwrap()
            .webpages;

        assert_eq!(result.num_docs, 1);
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com");
    }

    #[test]
    fn sentence_spell_correction() {
        let mut index = Index::temporary().expect("Unable to open index");

        index
            .insert(Webpage::new(
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
            ))
            .expect("failed to insert webpage");

        index.commit().unwrap();

        assert_eq!(
            String::from(
                index
                    .spell_correction(&["th".to_string(), "best".to_string()])
                    .unwrap()
            ),
            "the best".to_string()
        );
        assert_eq!(
            index.spell_correction(&["the".to_string(), "best".to_string()]),
            None
        );
    }
}
