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

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use tantivy::collector::{Collector, Count};
use tantivy::directory::MmapDirectory;
use tantivy::merge_policy::NoMergePolicy;
use tantivy::schema::Schema;
use tantivy::tokenizer::TokenizerManager;
use tantivy::{Document, IndexReader, IndexWriter, SegmentMeta};

use crate::collector::Hashes;
use crate::fastfield_cache::FastFieldCache;
use crate::human_website_annotations::Topic;
use crate::image_store::Image;
use crate::query::Query;
use crate::schema::{FastField, Field, TextField, ALL_FIELDS};
use crate::snippet;
use crate::tokenizer::Identity;
use crate::webpage::region::Region;
use crate::webpage::{StoredPrimaryImage, Webpage};
use crate::Result;
use crate::{schema::create_schema, tokenizer::Tokenizer};
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Weak};

#[derive(Debug, Serialize, Deserialize)]
pub struct InitialSearchResult {
    pub num_websites: usize,
    pub top_websites: Vec<WebsitePointer>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WebsitePointer {
    pub score: f64,
    pub hashes: Hashes,
    pub address: DocAddress,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct DocAddress {
    pub segment: u32,
    pub doc_id: u32,
}

impl From<tantivy::DocAddress> for DocAddress {
    fn from(address: tantivy::DocAddress) -> Self {
        Self {
            segment: address.segment_ord,
            doc_id: address.doc_id,
        }
    }
}

impl From<DocAddress> for tantivy::DocAddress {
    fn from(address: DocAddress) -> Self {
        Self {
            segment_ord: address.segment,
            doc_id: address.doc_id,
        }
    }
}

struct SegmentMergeCandidate {
    num_docs: u32,
    segments: Vec<SegmentMeta>,
}

pub struct InvertedIndex {
    pub path: String,
    tantivy_index: tantivy::Index,
    writer: IndexWriter,
    reader: IndexReader,
    fastfield_cache: Arc<FastFieldCache>,
    schema: Arc<Schema>,
}

impl InvertedIndex {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let schema = create_schema();

        let mut tantivy_index = if path.as_ref().exists() {
            let mmap_directory = MmapDirectory::open(&path)?;
            tantivy::Index::open(mmap_directory)?
        } else {
            let index_settings = tantivy::IndexSettings {
                sort_by_field: Some(tantivy::IndexSortByField {
                    field: Field::Fast(FastField::PreComputedScore).name().to_string(),
                    order: tantivy::Order::Desc,
                }),
                ..Default::default()
            };

            fs::create_dir_all(&path)?;
            let mmap_directory = MmapDirectory::open(&path)?;
            tantivy::Index::create(mmap_directory, schema.clone(), index_settings)?
        };

        tantivy_index.set_default_multithread_executor()?;

        let tokenizer = Tokenizer::default();
        tantivy_index
            .tokenizers()
            .register(tokenizer.as_str(), tokenizer);

        let tokenizer = Tokenizer::new_stemmed();
        tantivy_index
            .tokenizers()
            .register(tokenizer.as_str(), tokenizer);

        let tokenizer = Tokenizer::Identity(Identity::default());
        tantivy_index
            .tokenizers()
            .register(tokenizer.as_str(), tokenizer);

        let writer = tantivy_index.writer_with_num_threads(1, 1_000_000_000)?;

        let merge_policy = NoMergePolicy::default();
        writer.set_merge_policy(Box::new(merge_policy));

        let fastfield_cache = Arc::new(FastFieldCache::default());

        let warmers: Vec<Weak<dyn tantivy::Warmer>> = vec![Arc::downgrade(
            &(Arc::clone(&fastfield_cache) as Arc<dyn tantivy::Warmer>),
        )];
        let reader = tantivy_index.reader_builder().warmers(warmers).try_into()?;

        Ok(InvertedIndex {
            writer,
            reader,
            schema: Arc::new(schema),
            path: path.as_ref().to_str().unwrap().to_string(),
            fastfield_cache,
            tantivy_index,
        })
    }

    pub fn fastfield_cache(&self) -> Arc<FastFieldCache> {
        Arc::clone(&self.fastfield_cache)
    }

    pub fn tokenizers(&self) -> &TokenizerManager {
        self.tantivy_index.tokenizers()
    }

    #[cfg(test)]
    pub fn temporary() -> Result<Self> {
        let path = crate::gen_temp_path();
        Self::open(path)
    }

    pub fn insert(&mut self, webpage: Webpage) -> Result<()> {
        self.writer
            .add_document(webpage.into_tantivy(&self.schema)?)?;
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        self.writer.commit()?;
        self.reader.reload()?;

        Ok(())
    }

    pub fn search_initial<C>(&self, query: &Query, collector: C) -> Result<InitialSearchResult>
    where
        C: Collector<Fruit = Vec<WebsitePointer>>,
    {
        let searcher = self.reader.searcher();

        let (count, docs) = searcher.search(query, &(Count, collector))?;
        Ok(InitialSearchResult {
            num_websites: count,
            top_websites: docs,
        })
    }

    pub fn retrieve_websites(
        &self,
        websites: &[WebsitePointer],
        query: &Query,
    ) -> Result<Vec<RetrievedWebpage>> {
        let searcher = self.reader.searcher();
        let mut webpages: Vec<RetrievedWebpage> = websites
            .iter()
            .map(|website| self.retrieve_doc(website.address, &searcher))
            .filter_map(|page| page.ok())
            .map(|mut doc| {
                if let Some(image) = doc.primary_image.as_ref() {
                    if !query.simple_terms().into_iter().all(|term| {
                        image
                            .title_terms
                            .contains(term.to_ascii_lowercase().as_str())
                            || image
                                .description_terms
                                .contains(term.to_ascii_lowercase().as_str())
                    }) {
                        doc.primary_image = None;
                    }
                }

                doc
            })
            .collect();

        for page in &mut webpages {
            page.snippet = snippet::generate(
                query,
                &page.body,
                &page.dirty_body,
                &page.description,
                &page.region,
                &searcher,
            )?;
        }

        Ok(webpages)
    }

    pub fn search<C>(&self, query: &Query, collector: C) -> Result<SearchResult>
    where
        C: Collector<Fruit = Vec<WebsitePointer>>,
    {
        let initial_result = self.search_initial(query, collector)?;
        let websites = self.retrieve_websites(&initial_result.top_websites, query)?;

        Ok(SearchResult {
            num_docs: initial_result.num_websites,
            documents: websites,
        })
    }

    pub fn merge_into_segments(&mut self, num_segments: u32) -> Result<()> {
        assert!(num_segments > 0);

        let mut merge_segments = Vec::new();

        for _ in 0..num_segments {
            merge_segments.push(SegmentMergeCandidate {
                num_docs: 0,
                segments: Vec::new(),
            });
        }

        let mut segments: Vec<_> = self
            .tantivy_index
            .load_metas()?
            .segments
            .into_iter()
            .collect();

        if segments.len() > num_segments as usize {
            segments.sort_by_key(|b| std::cmp::Reverse(b.num_docs()));

            for segment in segments {
                let best_candidate = merge_segments
                    .iter_mut()
                    .min_by(|a, b| a.num_docs.cmp(&b.num_docs))
                    .unwrap();

                best_candidate.num_docs += segment.num_docs();
                best_candidate.segments.push(segment);
            }

            for merge in merge_segments
                .into_iter()
                .filter(|merge| !merge.segments.is_empty())
            {
                let segment_ids: Vec<_> =
                    merge.segments.iter().map(|segment| segment.id()).collect();
                self.writer.merge(&segment_ids[..]).wait()?;

                let path = Path::new(&self.path);
                for segment in merge.segments {
                    for file in segment.list_files() {
                        std::fs::remove_file(path.join(file)).ok();
                    }
                }
            }
        }

        Ok(())
    }

    fn retrieve_doc(
        &self,
        doc_address: DocAddress,
        searcher: &tantivy::Searcher,
    ) -> Result<RetrievedWebpage> {
        let doc = searcher.doc(doc_address.into())?;
        Ok(RetrievedWebpage::from(doc))
    }

    pub fn merge(mut self, mut other: InvertedIndex) -> Self {
        let path = self.path.clone();

        {
            other.commit().expect("failed to commit index");
            self.commit().expect("failed to commit index");

            let other_meta = other
                .tantivy_index
                .load_metas()
                .expect("failed to load tantivy metadata for index");

            let mut meta = self
                .tantivy_index
                .load_metas()
                .expect("failed to load tantivy metadata for index");

            let x = other.path.clone();
            let other_path = Path::new(x.as_str());
            other.writer.wait_merging_threads().unwrap();

            let path = self.path.clone();
            let self_path = Path::new(path.as_str());
            self.writer.wait_merging_threads().unwrap();

            let ids: HashSet<_> = meta.segments.iter().map(|segment| segment.id()).collect();

            for segment in other_meta.segments {
                if ids.contains(&segment.id()) {
                    continue;
                }

                // TODO: handle case where current index has segment with same name
                for file in segment.list_files() {
                    let p = other_path.join(&file);
                    if p.exists() {
                        fs::rename(p, self_path.join(&file)).unwrap();
                    }
                }
                meta.segments.push(segment);
            }

            meta.segments
                .sort_by_key(|a| std::cmp::Reverse(a.max_doc()));

            fs::remove_dir_all(other_path).unwrap();

            let self_path = Path::new(&path);

            std::fs::write(
                self_path.join("meta.json"),
                serde_json::to_string_pretty(&meta).unwrap(),
            )
            .unwrap();
        }

        Self::open(path).expect("failed to open index")
    }

    pub fn stop(self) {
        self.writer.wait_merging_threads().unwrap()
    }

    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    pub fn num_segments(&self) -> usize {
        self.tantivy_index.searchable_segments().unwrap().len()
    }
}

#[derive(Debug, Serialize)]
pub struct SearchResult {
    pub num_docs: usize,
    pub documents: Vec<RetrievedWebpage>,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct RetrievedWebpage {
    pub title: String,
    pub url: String,
    pub snippet: String,
    pub body: String,
    pub dirty_body: String,
    pub description: Option<String>,
    pub favicon: Option<Image>,
    pub primary_image: Option<StoredPrimaryImage>,
    pub updated_time: Option<NaiveDateTime>,
    pub host_topic: Option<Topic>,
    pub region: Region,
}

impl From<Document> for RetrievedWebpage {
    fn from(doc: Document) -> Self {
        let mut webpage = RetrievedWebpage::default();

        for value in doc.field_values() {
            match ALL_FIELDS[value.field.field_id() as usize] {
                Field::Text(TextField::Title) => {
                    webpage.title = value
                        .value
                        .as_text()
                        .expect("Title field should be text")
                        .to_string()
                }
                Field::Text(TextField::StemmedCleanBody) => {
                    webpage.body = value
                        .value
                        .as_text()
                        .expect("Body field should be text")
                        .to_string()
                }
                Field::Text(TextField::Description) => {
                    let desc = value
                        .value
                        .as_text()
                        .expect("Description field should be text")
                        .to_string();

                    webpage.description = if desc.is_empty() { None } else { Some(desc) }
                }
                Field::Text(TextField::Url) => {
                    webpage.url = value
                        .value
                        .as_text()
                        .expect("Url field should be text")
                        .to_string()
                }
                Field::Text(TextField::PrimaryImage) => {
                    webpage.primary_image = {
                        let bytes = value
                            .value
                            .as_bytes()
                            .expect("Primary image field should be bytes");

                        bincode::deserialize(bytes).unwrap()
                    }
                }
                Field::Fast(FastField::LastUpdated) => {
                    webpage.updated_time = {
                        let timestamp = value.value.as_u64().unwrap() as i64;
                        if timestamp == 0 {
                            None
                        } else {
                            Some(NaiveDateTime::from_timestamp(timestamp, 0))
                        }
                    }
                }
                Field::Text(TextField::AllBody) => {
                    webpage.dirty_body = value
                        .value
                        .as_text()
                        .expect("All body field should be text")
                        .to_string()
                }
                Field::Fast(FastField::Region) => {
                    webpage.region = {
                        let id = value.value.as_u64().unwrap();
                        Region::from_id(id)
                    }
                }
                Field::Text(TextField::HostTopic) => {
                    let facet = value.value.as_facet().unwrap();

                    if !facet.is_root() {
                        webpage.host_topic = Some(facet.clone().into())
                    }
                }
                _ => {}
            }
        }

        webpage
    }
}

#[cfg(test)]
mod tests {
    use maplit::hashset;

    use crate::{
        ranking::{Ranker, SignalAggregator},
        webpage::{region::RegionCount, Html, Link},
    };

    use super::*;

    const CONTENT: &str = "this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever";

    #[test]
    fn simple_search() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");
        let query = Query::parse(
            "website",
            index.schema(),
            index.tokenizers(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(
            RegionCount::default(),
            SignalAggregator::default(),
            index.fastfield_cache(),
        );

        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.documents.len(), 0);
        assert_eq!(result.num_docs, 0);

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
        index.commit().expect("failed to commit index");

        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.num_docs, 1);
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com");
    }

    #[test]
    fn document_not_matching() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");
        let query = Query::parse(
            "this query should not match",
            index.schema(),
            index.tokenizers(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(
            RegionCount::default(),
            SignalAggregator::default(),
            index.fastfield_cache(),
        );

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
        index.commit().expect("failed to commit index");

        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.documents.len(), 0);
        assert_eq!(result.num_docs, 0);
    }

    #[test]
    fn english_stemming() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");
        let query = Query::parse(
            "runner",
            index.schema(),
            index.tokenizers(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(
            RegionCount::default(),
            SignalAggregator::default(),
            index.fastfield_cache(),
        );

        index
            .insert(Webpage::new(
                &format!(
                    r#"
            <html>
                <head>
                    <title>Website for runners</title>
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
        index.commit().expect("failed to commit index");

        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com");
    }

    #[test]
    fn stemmed_query_english() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");
        let query = Query::parse(
            "runners",
            index.schema(),
            index.tokenizers(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(
            RegionCount::default(),
            SignalAggregator::default(),
            index.fastfield_cache(),
        );

        index
            .insert(Webpage::new(
                &format!(
                    r#"
            <html>
                <head>
                    <title>Fast runner</title>
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
        index.commit().expect("failed to commit index");

        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com");
    }

    #[test]
    fn not_searchable_backlinks() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");
        let query = Query::parse(
            "great site",
            index.schema(),
            index.tokenizers(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(
            RegionCount::default(),
            SignalAggregator::default(),
            index.fastfield_cache(),
        );

        index
            .insert(Webpage::new(
                &format!(
                    r#"
            <html>
                <head>
                    <title>Website A</title>
                </head>
                <a href="https://www.b.com">B site is great</a>
                {CONTENT}
            </html>
            "#
                ),
                "https://www.a.com",
            ))
            .expect("failed to insert webpage");
        index
            .insert(Webpage {
                html: Html::parse(
                    &format!(
                        r#"
            <html>
                <head>
                    <title>Website B</title>
                </head>
                <body>
                    {CONTENT}
                </body>
            </html>
            "#
                    ),
                    "https://www.b.com",
                ),
                backlinks: vec![Link {
                    source: "https://www.a.com".to_string().into(),
                    destination: "https://www.b.com".to_string().into(),
                    text: "B site is great".to_string(),
                }],
                host_centrality: 1.0,
                page_centrality: 0.0,
                fetch_time_ms: 500,
                pre_computed_score: 0.0,
                primary_image: None,
                node_id: None,
                host_topic: None,
                crawl_stability: 0.0,
                dmoz_description: None,
            })
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");

        let mut result = index
            .search(&query, ranker.collector())
            .expect("Search failed");

        result
            .documents
            .sort_by(|a, b| a.url.partial_cmp(&b.url).unwrap());

        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.a.com");
    }

    #[test]
    fn limited_top_docs() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");
        let query = Query::parse(
            "runner",
            index.schema(),
            index.tokenizers(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(
            RegionCount::default(),
            SignalAggregator::default(),
            index.fastfield_cache(),
        );

        for _ in 0..100 {
            let dedup_s = crate::rand_words(100);

            index
                .insert(Webpage::new(
                    &format!(
                        r#"
                    <html>
                        <head>
                            <title>Website for runners</title>
                        </head>
                        <body>
                            {CONTENT} {dedup_s}
                        </body>
                    </html>
                    "#
                    ),
                    "https://www.example.com",
                ))
                .expect("failed to insert webpage");
        }

        index.commit().expect("failed to commit index");

        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.documents.len(), 20);
    }

    #[test]
    fn host_search() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");
        let query = Query::parse(
            "dr",
            index.schema(),
            index.tokenizers(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(
            RegionCount::default(),
            SignalAggregator::default(),
            index.fastfield_cache(),
        );

        index
            .insert(Webpage::new(
                &format!(
                    r#"
                    <html>
                        <head>
                            <title>News website</title>
                        </head>
                        <body>
                            {CONTENT}
                        </body>
                    </html>
                "#
                ),
                "https://www.dr.dk",
            ))
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");

        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.dr.dk");
    }

    #[test]
    fn merge() {
        let mut index1 = InvertedIndex::temporary().expect("Unable to open index");

        index1
            .insert(Webpage::new(
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
                "https://www.example.com",
            ))
            .expect("failed to insert webpage");

        let mut index2 = InvertedIndex::temporary().expect("Unable to open index");

        index2
            .insert(Webpage::new(
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
                "https://www.example.com",
            ))
            .expect("failed to insert webpage");

        let mut index = index1.merge(index2);
        index.commit().unwrap();

        let query = Query::parse(
            "website",
            index.schema(),
            index.tokenizers(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(
            RegionCount::default(),
            SignalAggregator::default(),
            index.fastfield_cache(),
        );

        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.num_docs, 2);
        assert_eq!(result.documents.len(), 2);
        assert_eq!(result.documents[0].url, "https://www.example.com");
        assert_eq!(result.documents[1].url, "https://www.example.com");
    }

    #[test]
    fn match_across_fields() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");
        let query = Query::parse(
            "example test",
            index.schema(),
            index.tokenizers(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(
            RegionCount::default(),
            SignalAggregator::default(),
            index.fastfield_cache(),
        );

        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.documents.len(), 0);
        assert_eq!(result.num_docs, 0);

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
        index.commit().expect("failed to commit index");

        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.num_docs, 1);
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com");
    }

    #[test]
    fn only_show_primary_images_when_relevant() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");
        let ranker = Ranker::new(
            RegionCount::default(),
            SignalAggregator::default(),
            index.fastfield_cache(),
        );

        let mut webpage = Webpage::new(
            &format!(
                r#"
                    <html>
                        <head>
                            <meta property="og:image" content="https://example.com/link_to_image.html" />
                            <meta property="og:description" content="This is an image for the test website" />
                            <meta property="og:title" content="title" />
                            <title>Test website</title>
                        </head>
                        <body>
                            {CONTENT}
                        </body>
                    </html>
                    "#
            ),
            "https://www.example.com",
        );
        let uuid = uuid::uuid!("00000000-0000-0000-0000-ffff00000000");
        webpage.set_primary_image(uuid, webpage.html.primary_image().unwrap());

        index.insert(webpage).expect("failed to insert webpage");
        index.commit().expect("failed to commit index");

        let query = Query::parse(
            "website",
            index.schema(),
            index.tokenizers(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query");

        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");

        assert_eq!(result.num_docs, 1);
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com");
        assert_eq!(
            result.documents[0].primary_image,
            Some(StoredPrimaryImage {
                uuid,
                title_terms: hashset! {"title".to_string()},
                description_terms: hashset! {"this".to_string(), "is".to_string(), "an".to_string(), "image".to_string(), "for".to_string(), "the".to_string(), "test".to_string(), "website".to_string()}
            })
        );

        let query = Query::parse(
            "best website",
            index.schema(),
            index.tokenizers(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query");

        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");

        assert_eq!(result.num_docs, 1);
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com");
        assert_eq!(result.documents[0].primary_image, None);
    }

    #[test]
    fn id_links_removed_during_indexing() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");
        let query = Query::parse(
            "website",
            index.schema(),
            index.tokenizers(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(
            RegionCount::default(),
            SignalAggregator::default(),
            index.fastfield_cache(),
        );

        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.documents.len(), 0);
        assert_eq!(result.num_docs, 0);

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
                "https://www.example.com#tag",
            ))
            .expect("failed to insert webpage");
        index.commit().expect("failed to commit index");

        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.num_docs, 1);
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com");
    }

    #[test]
    fn remove_duplicates() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");
        let query = Query::parse(
            "dr",
            index.schema(),
            index.tokenizers(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(
            RegionCount::default(),
            SignalAggregator::default(),
            index.fastfield_cache(),
        );

        index
            .insert(Webpage::new(
                &format!(
                    r#"
                    <html>
                        <head>
                            <title>News website</title>
                        </head>
                        <body>
                            {CONTENT}
                        </body>
                    </html>
                "#
                ),
                "https://www.dr.xyz",
            ))
            .expect("failed to insert webpage");

        index
            .insert(Webpage::new(
                &format!(
                    r#"
                    <html>
                        <head>
                            <title>News website</title>
                        </head>
                        <body>
                            {CONTENT} dr
                        </body>
                    </html>
                "#
                ),
                "https://www.dr.dk",
            ))
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");

        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.dr.dk");
    }
}
