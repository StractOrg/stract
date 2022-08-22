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
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use itertools::intersperse;
use serde::{Deserialize, Serialize};
use tantivy::collector::Collector;
use tantivy::schema::Schema;
use tantivy::tokenizer::TokenizerManager;
use uuid::Uuid;

use crate::directory::{self, DirEntry};
use crate::image_downloader::{ImageDownloadJob, ImageDownloader};
use crate::image_store::{FaviconStore, Image, ImageStore, PrimaryImageStore};
use crate::inverted_index::{InvertedIndex, InvertedIndexSearchResult};
use crate::query::Query;
use crate::spell::{Dictionary, LogarithmicEdit, SpellChecker, TermSplitter};
use crate::webpage::{Url, Webpage};
use crate::Result;

const INVERTED_INDEX_SUBFOLDER_NAME: &str = "inverted_index";
const FAVICON_STORE_SUBFOLDER_NAME: &str = "favicon_store";
const PRIMARY_IMAGE_STORE_SUBFOLDER_NAME: &str = "primary_image_store";
const SPELL_SUBFOLDER_NAME: &str = "primary_image_store";
const IMAGE_WEBPAGE_CENTRALITY_THRESHOLD: f64 = 0.0;

pub struct Index {
    inverted_index: InvertedIndex,
    favicon_store: FaviconStore,
    primary_image_store: PrimaryImageStore,
    favicon_downloader: ImageDownloader<String>,
    primary_image_downloader: ImageDownloader<Uuid>,
    spell_dictionary: Dictionary<1_000_000>,
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

        Ok(Self {
            inverted_index,
            favicon_store,
            primary_image_store,
            primary_image_downloader: ImageDownloader::new(),
            favicon_downloader: ImageDownloader::new(),
            spell_dictionary: Dictionary::open(Some(path.as_ref().join(SPELL_SUBFOLDER_NAME)))?,
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
        self.inverted_index.insert(webpage)
    }

    pub fn commit(&mut self) -> Result<()> {
        self.inverted_index.merge_all_segments()?;
        self.spell_dictionary.commit()?;
        self.inverted_index.commit()
    }

    pub fn search<C>(&self, query: &Query, collector: C) -> Result<InvertedIndexSearchResult>
    where
        C: Collector<Fruit = Vec<(f64, tantivy::DocAddress)>>,
    {
        self.inverted_index.search(query, collector)
    }

    pub fn merge(mut self, other: Self) -> Self {
        self.inverted_index.merge(other.inverted_index);

        self.favicon_store.merge(other.favicon_store);
        drop(self.favicon_store);

        self.primary_image_store.merge(other.primary_image_store);
        drop(self.primary_image_store);

        self.spell_dictionary.merge(other.spell_dictionary);

        Self::open(&self.path).expect("failed to open index")
    }

    fn maybe_insert_favicon(&mut self, webpage: &Webpage) {
        if !webpage.html.is_homepage()
            || self
                .favicon_store
                .contains(&webpage.html.domain().to_string())
        {
            return;
        }

        if let Some(favicon) = webpage.html.favicon() {
            self.favicon_downloader.schedule(ImageDownloadJob {
                key: favicon.link.domain().to_string(),
                urls: vec![favicon.link],
                timeout: Some(Duration::from_secs(1)),
            });
        }
    }

    fn maybe_insert_primary_image(&mut self, webpage: &mut Webpage) {
        match webpage
            .centrality
            .partial_cmp(&IMAGE_WEBPAGE_CENTRALITY_THRESHOLD)
        {
            None | Some(Ordering::Greater) => {}
            _ => return,
        }

        if let Some(url) = webpage.html.primary_image() {
            let uuid = self.primary_image_store.generate_uuid();
            webpage.set_primary_image_uuid(uuid);

            self.primary_image_downloader.schedule(ImageDownloadJob {
                key: uuid,
                urls: vec![url],
                timeout: Some(Duration::from_secs(5)),
            });
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

    fn spell_check(&self, terms: &[String]) -> Option<String> {
        let spellchecker = SpellChecker::new(&self.spell_dictionary, LogarithmicEdit::new(4));
        let mut corrections: Vec<String> = Vec::new();

        for term in terms {
            match spellchecker.correct(term.to_ascii_lowercase().as_str()) {
                Some(correction) => corrections.push(correction.to_ascii_lowercase()),
                None => corrections.push(term.to_ascii_lowercase()),
            }
        }

        if corrections
            .iter()
            .cloned()
            .zip(terms.iter().map(|term| term.to_ascii_lowercase()))
            .all(|(correction, term)| correction == term)
        {
            None
        } else {
            Some(intersperse(corrections.into_iter(), " ".to_string()).collect())
        }
    }

    fn split_words(&self, terms: &[String]) -> Option<String> {
        let splitter = TermSplitter::new(&self.spell_dictionary);
        let mut corrections: Vec<String> = Vec::new();

        for term in terms {
            let t = term.to_ascii_lowercase();
            let split = splitter.split(t.as_str());
            if split.is_empty() {
                corrections.push(t);
            } else {
                for s in split {
                    corrections.push(s.to_string())
                }
            }
        }

        if corrections
            .iter()
            .map(|s| s.to_ascii_lowercase())
            .zip(terms.iter().map(|term| term.to_ascii_lowercase()))
            .all(|(correction, term)| correction == term)
        {
            None
        } else {
            Some(intersperse(corrections.into_iter(), " ".to_string()).collect())
        }
    }

    pub fn spell_correction(&self, terms: &[String]) -> Option<String> {
        self.spell_check(terms).or_else(|| self.split_words(terms))
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
    use crate::ranking::Ranker;

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
                vec![],
                1.0,
                500,
            ))
            .expect("failed to parse webpage");

        index.commit().unwrap();

        let path = index.path.clone();
        let frozen: FrozenIndex = index.into();
        let bytes = bincode::serialize(&frozen).unwrap();

        std::fs::remove_dir_all(path).unwrap();

        let deserialized_frozen: FrozenIndex = bincode::deserialize(&bytes).unwrap();
        let index: Index = deserialized_frozen.into();
        let query = Query::parse("website", index.schema(), index.tokenizers())
            .expect("Failed to parse query");
        let ranker = Ranker::new(query.clone());

        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.num_docs, 1);
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com");

        assert_eq!(
            index.spell_correction(&["thiss".to_string()]),
            Some("this".to_string())
        );
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
                vec![],
                1.0,
                500,
            ))
            .expect("failed to parse webpage");

        index.commit().unwrap();

        assert_eq!(
            index.spell_correction(&["th".to_string(), "best".to_string()]),
            Some("the best".to_string())
        );
        assert_eq!(
            index.spell_correction(&["the".to_string(), "best".to_string()]),
            None
        );
    }
}
