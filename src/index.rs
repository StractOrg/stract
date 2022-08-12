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

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tantivy::collector::Collector;
use tantivy::schema::Schema;
use uuid::Uuid;

use crate::directory::{self, DirEntry};
use crate::image_store::{FaviconStore, Image, ImageStore, PrimaryImageStore};
use crate::inverted_index::{InvertedIndex, InvertedIndexSearchResult};
use crate::query::Query;
use crate::webpage::{Url, Webpage};
use crate::Result;

const INVERTED_INDEX_SUBFOLDER_NAME: &str = "inverted_index";
const FAVICON_STORE_SUBFOLDER_NAME: &str = "favicon_store";
const PRIMARY_IMAGE_STORE_SUBFOLDER_NAME: &str = "primary_image_store";
const IMAGE_WEBPAGE_CENTRALITY_THRESHOLD: f64 = 0.0;

#[derive(Clone, Hash, PartialEq, Eq, Debug)]
enum ImageDownloadJob {
    Favicon(Url),
    PrimaryImage { key: Uuid, url: Url },
}

#[derive(Debug)]
struct DownloadedImage {
    image: Image,
    key: String,
    original_job: ImageDownloadJob,
}

impl ImageDownloadJob {
    pub async fn download(&self) -> Option<DownloadedImage> {
        match self {
            ImageDownloadJob::Favicon(url) => url
                .download_bytes(Duration::from_secs(1))
                .await
                .and_then(|bytes| Image::from_bytes(bytes).ok())
                .map(|image| DownloadedImage {
                    image,
                    key: url.domain().to_string(),
                    original_job: self.clone(),
                }),
            ImageDownloadJob::PrimaryImage { key, url } => url
                .download_bytes(Duration::from_secs(5))
                .await
                .and_then(|bytes| Image::from_bytes(bytes).ok())
                .map(|image| DownloadedImage {
                    image,
                    key: key.to_string(),
                    original_job: self.clone(),
                }),
        }
    }
}

pub struct Index {
    inverted_index: InvertedIndex,
    favicon_store: FaviconStore,
    primary_image_store: PrimaryImageStore,
    image_download_jobs: HashSet<ImageDownloadJob>,
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
            path: path.as_ref().to_str().unwrap().to_string(),
            image_download_jobs: HashSet::new(),
        })
    }

    #[cfg(test)]
    pub fn temporary() -> Result<Self> {
        let path = crate::gen_temp_path();
        Self::open(path)
    }

    pub fn insert(&mut self, mut webpage: Webpage) -> Result<()> {
        self.maybe_insert_favicon(&webpage);
        self.maybe_insert_primary_image(&mut webpage);
        self.inverted_index.insert(webpage)
    }

    pub fn commit(&mut self) -> Result<()> {
        self.inverted_index.merge_all_segments()?;
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
            self.image_download_jobs
                .insert(ImageDownloadJob::Favicon(favicon.link));
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

            self.image_download_jobs
                .insert(ImageDownloadJob::PrimaryImage { key: uuid, url });
        }
    }

    pub fn download_pending_images(&mut self) {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async { self.download_pending_images_async().await });

        self.image_download_jobs.clear();
    }

    async fn download_pending_images_async(&mut self) {
        let results = futures::stream::iter(
            self.image_download_jobs
                .iter()
                .map(|job| async move { job.download().await }),
        )
        .buffer_unordered(20)
        .collect::<Vec<Option<DownloadedImage>>>()
        .await;

        for result in results.into_iter().flatten() {
            match result.original_job {
                ImageDownloadJob::Favicon(_) => self.favicon_store.insert(result.key, result.image),
                ImageDownloadJob::PrimaryImage { key, url: _ } => {
                    self.primary_image_store.insert(key, result.image)
                }
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

        let path = index.path.clone();
        let frozen: FrozenIndex = index.into();
        let bytes = bincode::serialize(&frozen).unwrap();

        std::fs::remove_dir_all(path).unwrap();

        let deserialized_frozen: FrozenIndex = bincode::deserialize(&bytes).unwrap();
        let index: Index = deserialized_frozen.into();
        let query = Query::parse("website", index.schema()).expect("Failed to parse query");
        let ranker = Ranker::new(query.clone());

        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.num_docs, 1);
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com");
    }
}
