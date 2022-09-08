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
use serde::Serialize;
use tantivy::collector::{Collector, Count};
use tantivy::merge_policy::NoMergePolicy;
use tantivy::schema::Schema;
use tantivy::tokenizer::TokenizerManager;
use tantivy::{DocAddress, Document, IndexReader, IndexWriter};

use crate::image_store::Image;
use crate::query::Query;
use crate::schema::{Field, ALL_FIELDS};
use crate::snippet;
use crate::webpage::region::Region;
use crate::webpage::{StoredPrimaryImage, Webpage};
use crate::Result;
use crate::{schema::create_schema, tokenizer::Tokenizer};
use std::fs;
use std::path::Path;
use std::sync::Arc;

pub struct InvertedIndex {
    pub path: String,
    tantivy_index: tantivy::Index,
    writer: IndexWriter,
    reader: IndexReader,
    schema: Arc<Schema>,
}

impl InvertedIndex {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let schema = create_schema();

        let mut tantivy_index = if path.as_ref().exists() {
            tantivy::Index::open_in_dir(&path)?
        } else {
            fs::create_dir_all(&path)?;
            tantivy::Index::create_in_dir(&path, schema.clone())?
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

        let writer = tantivy_index.writer_with_num_threads(1, 4_000_000_000)?;

        let merge_policy = NoMergePolicy::default();
        writer.set_merge_policy(Box::new(merge_policy));

        Ok(InvertedIndex {
            writer,
            reader: tantivy_index.reader()?,
            schema: Arc::new(schema),
            path: path.as_ref().to_str().unwrap().to_string(),
            tantivy_index,
        })
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

    pub fn search<C>(&self, query: &Query, collector: C) -> Result<InvertedIndexSearchResult>
    where
        C: Collector<Fruit = Vec<(f64, tantivy::DocAddress)>>,
    {
        let searcher = self.reader.searcher();

        let (count, docs) = searcher.search(query, &(Count, collector))?;

        let mut webpages: Vec<RetrievedWebpage> = docs
            .into_iter()
            .map(|(_score, doc_address)| self.retrieve_doc(doc_address, &searcher))
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

        Ok(InvertedIndexSearchResult {
            num_docs: count,
            documents: webpages,
        })
    }

    pub fn merge_all_segments(&mut self) -> Result<()> {
        let segments: Vec<_> = self
            .tantivy_index
            .load_metas()?
            .segments
            .into_iter()
            .collect();

        if segments.len() > 1 {
            let segment_ids: Vec<_> = segments.iter().map(|segment| segment.id()).collect();
            self.writer.merge(&segment_ids[..]).wait()?;

            let path = Path::new(&self.path);
            for segment in segments {
                for file in segment.list_files() {
                    std::fs::remove_file(path.join(file)).ok();
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
        let doc = searcher.doc(doc_address)?;
        Ok(RetrievedWebpage::from(doc))
    }

    pub fn merge(mut self, mut other: InvertedIndex) -> Self {
        other.commit().expect("failed to commit index");
        self.commit().expect("failed to commit index");

        let other_meta = other
            .tantivy_index
            .load_metas()
            .expect("failed to laod tantivy metadata for index");

        let mut meta = self
            .tantivy_index
            .load_metas()
            .expect("failed to laod tantivy metadata for index");

        let x = other.path.clone();
        let other_path = Path::new(x.as_str());
        other.writer.wait_merging_threads().unwrap();

        let path = self.path.clone();
        let self_path = Path::new(path.as_str());
        self.writer.wait_merging_threads().unwrap();

        for segment in other_meta.segments {
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

        Self::open(path).expect("failed to open index")
    }

    pub fn stop(self) {
        self.writer.wait_merging_threads().unwrap()
    }

    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}

#[derive(Debug, Serialize)]
pub struct InvertedIndexSearchResult {
    pub num_docs: usize,
    pub documents: Vec<RetrievedWebpage>,
}

#[derive(Default, Debug, Serialize)]
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
    pub region: Region,
}

impl From<Document> for RetrievedWebpage {
    fn from(doc: Document) -> Self {
        let mut webpage = RetrievedWebpage::default();

        for value in doc.field_values() {
            match ALL_FIELDS[value.field.field_id() as usize] {
                Field::Title => {
                    webpage.title = value
                        .value
                        .as_text()
                        .expect("Title field should be text")
                        .to_string()
                }
                Field::StemmedCleanBody => {
                    webpage.body = value
                        .value
                        .as_text()
                        .expect("Body field should be text")
                        .to_string()
                }
                Field::Description => {
                    let desc = value
                        .value
                        .as_text()
                        .expect("Description field should be text")
                        .to_string();

                    webpage.description = if desc.is_empty() { None } else { Some(desc) }
                }
                Field::Url => {
                    webpage.url = value
                        .value
                        .as_text()
                        .expect("Url field should be text")
                        .to_string()
                }
                Field::PrimaryImage => {
                    webpage.primary_image = {
                        let bytes = value
                            .value
                            .as_bytes()
                            .expect("Primary image field should be bytes");

                        bincode::deserialize(bytes).unwrap()
                    }
                }
                Field::LastUpdated => {
                    webpage.updated_time = {
                        let timestamp = value.value.as_u64().unwrap() as i64;
                        if timestamp == 0 {
                            None
                        } else {
                            Some(NaiveDateTime::from_timestamp(timestamp, 0))
                        }
                    }
                }
                Field::AllBody => {
                    webpage.dirty_body = value
                        .value
                        .as_text()
                        .expect("All body field should be text")
                        .to_string()
                }
                Field::Region => {
                    webpage.region = {
                        let id = value.value.as_u64().unwrap();
                        Region::from_id(id)
                    }
                }
                Field::BacklinkText
                | Field::Centrality
                | Field::Site
                | Field::StemmedTitle
                | Field::CleanBody
                | Field::Domain
                | Field::DomainIfHomepage
                | Field::IsHomepage
                | Field::NumTrackers
                | Field::NumCleanBodyTokens
                | Field::NumDescriptionTokens
                | Field::NumTitleTokens
                | Field::NumUrlTokens
                | Field::FetchTimeMs => {}
            }
        }

        webpage
    }
}

#[cfg(test)]
mod tests {
    use maplit::hashset;

    use crate::{
        ranking::{goggles::SignalAggregator, Ranker},
        webpage::{region::RegionCount, Link},
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
        let ranker = Ranker::new(RegionCount::default(), SignalAggregator::default());

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
                vec![],
                1.0,
                500,
            ))
            .expect("failed to parse webpage");
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
        let ranker = Ranker::new(RegionCount::default(), SignalAggregator::default());

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
        let ranker = Ranker::new(RegionCount::default(), SignalAggregator::default());

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
                vec![],
                1.0,
                500,
            ))
            .expect("failed to parse webpage");
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
        let ranker = Ranker::new(RegionCount::default(), SignalAggregator::default());

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
                vec![],
                1.0,
                500,
            ))
            .expect("failed to parse webpage");
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
        let ranker = Ranker::new(RegionCount::default(), SignalAggregator::default());

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
                vec![],
                1.0,
                500,
            ))
            .expect("failed to parse webpage");
        index
            .insert(Webpage::new(
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
                vec![Link {
                    source: "https://www.a.com".to_string().into(),
                    destination: "https://www.b.com".to_string().into(),
                    text: "B site is great".to_string(),
                }],
                1.0,
                500,
            ))
            .expect("failed to parse webpage");

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
        let ranker = Ranker::new(RegionCount::default(), SignalAggregator::default());

        for _ in 0..100 {
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
                    vec![],
                    1.0,
                    500,
                ))
                .expect("failed to parse webpage");
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
        let ranker = Ranker::new(RegionCount::default(), SignalAggregator::default());

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
                vec![],
                1.0,
                500,
            ))
            .expect("failed to parse webpage");
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
                    {CONTENT}
                </body>
            </html>
            "#
                ),
                "https://www.example.com",
                vec![],
                1.,
                500,
            ))
            .expect("failed to parse webpage");

        let mut index = index1.merge(index2);
        index.commit().unwrap();

        let query = Query::parse(
            "website",
            index.schema(),
            index.tokenizers(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(RegionCount::default(), SignalAggregator::default());

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
        let ranker = Ranker::new(RegionCount::default(), SignalAggregator::default());

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
                vec![],
                1.0,
                500,
            ))
            .expect("failed to parse webpage");
        index.commit().expect("failed to commit index");

        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.num_docs, 1);
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com");
    }

    #[test]
    fn fetch_time_ranking() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");
        let query = Query::parse(
            "test",
            index.schema(),
            index.tokenizers(),
            &SignalAggregator::default(),
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(RegionCount::default(), SignalAggregator::default());

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
                "https://www.first.com",
                vec![],
                1.0,
                0,
            ))
            .expect("failed to parse webpage");
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
                "https://www.second.com",
                vec![],
                1.0,
                5000,
            ))
            .expect("failed to parse webpage");
        index.commit().expect("failed to commit index");

        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.num_docs, 2);
        assert_eq!(result.documents.len(), 2);
        assert_eq!(result.documents[0].url, "https://www.first.com");
        assert_eq!(result.documents[1].url, "https://www.second.com");
    }

    #[test]
    fn only_show_primary_images_when_relevant() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");
        let ranker = Ranker::new(RegionCount::default(), SignalAggregator::default());

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
            vec![],
            1.0,
            500,
        );
        let uuid = uuid::uuid!("00000000-0000-0000-0000-ffff00000000");
        webpage.set_primary_image(uuid, webpage.html.primary_image().unwrap());

        index.insert(webpage).expect("failed to parse webpage");
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
}
