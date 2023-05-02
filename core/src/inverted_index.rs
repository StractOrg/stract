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

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use tantivy::collector::{Collector, Count};
use tantivy::directory::MmapDirectory;
use tantivy::merge_policy::NoMergePolicy;
use tantivy::schema::Schema;
use tantivy::tokenizer::{Tokenizer as _, TokenizerManager};
use tantivy::{Document, IndexReader, IndexWriter, SegmentMeta};

use crate::collector::Hashes;
use crate::fastfield_reader::FastFieldReader;
use crate::human_website_annotations::Topic;
use crate::image_store::Image;
use crate::query::Query;
use crate::ranking::initial::Score;
use crate::ranking::pipeline::RankingWebsite;
use crate::ranking::SignalAggregator;
use crate::schema::{FastField, Field, TextField, ALL_FIELDS};
use crate::search_ctx::Ctx;
use crate::snippet;
use crate::tokenizer::{BigramTokenizer, Identity, TrigramTokenizer};
use crate::webgraph::NodeID;
use crate::webpage::region::Region;
use crate::webpage::{schema_org, StoredPrimaryImage, Webpage};
use crate::Result;
use crate::{schema::create_schema, tokenizer::Tokenizer};
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize)]
pub struct InitialSearchResult {
    pub num_websites: usize,
    pub top_websites: Vec<WebsitePointer>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct WebsitePointer {
    pub score: Score,
    pub hashes: Hashes,
    pub address: DocAddress,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
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

        let tokenizer = Tokenizer::Bigram(BigramTokenizer::default());
        tantivy_index
            .tokenizers()
            .register(tokenizer.as_str(), tokenizer);

        let tokenizer = Tokenizer::Trigram(TrigramTokenizer::default());
        tantivy_index
            .tokenizers()
            .register(tokenizer.as_str(), tokenizer);

        let writer = tantivy_index.writer_with_num_threads(1, 1_000_000_000)?;

        let merge_policy = NoMergePolicy::default();
        writer.set_merge_policy(Box::new(merge_policy));

        let reader: IndexReader = tantivy_index.reader_builder().try_into()?;

        Ok(InvertedIndex {
            writer,
            reader,
            schema: Arc::new(schema),
            path: path.as_ref().to_str().unwrap().to_string(),
            tantivy_index,
        })
    }

    pub fn fastfield_reader(&self, tv_searcher: &tantivy::Searcher) -> FastFieldReader {
        FastFieldReader::new(tv_searcher)
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

    pub fn search_initial<C>(
        &self,
        query: &Query,
        ctx: &Ctx,
        collector: C,
    ) -> Result<InitialSearchResult>
    where
        C: Collector<Fruit = Vec<WebsitePointer>>,
    {
        let collector = (Count, collector);
        let (count, pointers) = ctx.tv_searcher.search(query, &collector)?;

        Ok(InitialSearchResult {
            num_websites: count,
            top_websites: pointers,
        })
    }

    pub fn local_search_ctx(&self) -> Ctx {
        let tv_searcher = self.tv_searcher();
        Ctx {
            fastfield_reader: self.fastfield_reader(&tv_searcher),
            tv_searcher,
        }
    }

    pub fn tv_searcher(&self) -> tantivy::Searcher {
        self.reader.searcher()
    }

    pub fn retrieve_ranking_websites(
        &self,
        ctx: &Ctx,
        pointers: Vec<WebsitePointer>,
        mut aggregator: SignalAggregator,
        fastfield_reader: &FastFieldReader,
    ) -> Result<Vec<RankingWebsite>> {
        let mut top_websites = Vec::new();

        let mut pointers: Vec<_> = pointers.into_iter().enumerate().collect();
        pointers.sort_by(|a, b| a.1.address.segment.cmp(&b.1.address.segment));
        let mut prev_segment = None;
        for (orig_index, pointer) in pointers {
            let update_segment = match prev_segment {
                Some(prev_segment) if prev_segment != pointer.address.segment => true,
                None => true,
                _ => false,
            };

            if update_segment {
                let segment_reader = ctx.tv_searcher.segment_reader(pointer.address.segment);
                aggregator.register_segment(&ctx.tv_searcher, segment_reader, fastfield_reader)?;
            }

            prev_segment = Some(pointer.address.segment);

            top_websites.push((orig_index, RankingWebsite::new(pointer, &mut aggregator)));
        }

        top_websites.sort_by(|a, b| a.0.cmp(&b.0));

        Ok(top_websites
            .into_iter()
            .map(|(_, website)| website)
            .collect())
    }

    pub fn website_host_node(&self, website: &WebsitePointer) -> Result<Option<NodeID>> {
        let searcher = self.reader.searcher();
        let doc = searcher.doc(website.address.into())?;
        let field = self
            .schema()
            .get_field(Field::Fast(FastField::HostNodeID).name())
            .unwrap();

        let id = doc.get_first(field).unwrap().as_u64().unwrap();
        if id == u64::MAX {
            Ok(None)
        } else {
            Ok(Some(id.into()))
        }
    }

    pub fn retrieve_websites(
        &self,
        websites: &[WebsitePointer],
        query: &Query,
    ) -> Result<Vec<RetrievedWebpage>> {
        let tv_searcher = self.reader.searcher();
        let mut webpages: Vec<RetrievedWebpage> = websites
            .iter()
            .map(|website| self.retrieve_doc(website.address, &tv_searcher))
            .filter_map(|res| res.ok())
            .map(|mut doc| {
                if let Some(image) = doc.primary_image.as_ref() {
                    if !query.simple_terms().iter().all(|term| {
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
            page.snippet = snippet::generate(query, &page.body, &page.region);
        }

        Ok(webpages)
    }

    pub fn merge_into_max_segments(&mut self, max_num_segments: u32) -> Result<()> {
        assert!(max_num_segments > 0);

        let mut segments: Vec<_> = self
            .tantivy_index
            .load_metas()?
            .segments
            .into_iter()
            .collect();

        if segments.len() <= max_num_segments as usize {
            return Ok(());
        }

        let num_segments = (max_num_segments + 1) / 2; // ceil(num_segments/2)

        let mut merge_segments = Vec::new();

        for _ in 0..num_segments {
            merge_segments.push(SegmentMergeCandidate {
                num_docs: 0,
                segments: Vec::new(),
            });
        }

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
            let segment_ids: Vec<_> = merge.segments.iter().map(|segment| segment.id()).collect();
            self.writer.merge(&segment_ids[..]).wait()?;

            let path = Path::new(&self.path);
            for segment in merge.segments {
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

            fs::remove_dir_all(other_path).ok();

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

    pub(crate) fn get_webpage(&self, url: &str) -> Option<RetrievedWebpage> {
        let tv_searcher = self.reader.searcher();
        let field = tv_searcher
            .schema()
            .get_field(Field::Text(TextField::Url).name())
            .unwrap();
        let tokenizer = Tokenizer::default();
        let mut stream = tokenizer.token_stream(url);
        let mut term_queries = Vec::new();

        while let Some(tok) = stream.next() {
            let term = tantivy::Term::from_field_text(field, &tok.text);
            term_queries.push(term);
        }

        let query = tantivy::query::PhraseQuery::new(term_queries);
        let mut res = tv_searcher
            .search(&query, &tantivy::collector::TopDocs::with_limit(1))
            .unwrap();

        res.pop()
            .map(|(_, doc)| self.retrieve_doc(doc.into(), &tv_searcher).unwrap())
    }
}

#[derive(Debug, Serialize)]
pub struct SearchResult {
    pub num_docs: usize,
    pub documents: Vec<RetrievedWebpage>,
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq)]
pub struct RetrievedWebpage {
    pub title: String,
    pub url: String,
    pub body: String,
    pub snippet: String,
    pub dirty_body: String,
    pub description: Option<String>,
    pub dmoz_description: Option<String>,
    pub favicon: Option<Image>,
    pub primary_image: Option<StoredPrimaryImage>,
    pub updated_time: Option<NaiveDateTime>,
    pub host_topic: Option<Topic>,
    pub schema_org: Vec<schema_org::Item>,
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
                        .to_string();
                }
                Field::Text(TextField::StemmedCleanBody) => {
                    webpage.body = value
                        .value
                        .as_text()
                        .expect("Body field should be text")
                        .to_string();
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
                        .to_string();
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
                            NaiveDateTime::from_timestamp_opt(timestamp, 0)
                        }
                    }
                }
                Field::Text(TextField::AllBody) => {
                    webpage.dirty_body = value
                        .value
                        .as_text()
                        .expect("All body field should be text")
                        .to_string();
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
                        webpage.host_topic = Some(facet.clone().into());
                    }
                }
                Field::Text(TextField::DmozDescription) => {
                    let desc = value
                        .value
                        .as_text()
                        .expect("Dmoz description field should be text")
                        .to_string();

                    webpage.dmoz_description = if desc.is_empty() { None } else { Some(desc) }
                }
                Field::Text(TextField::SchemaOrgJson) => {
                    let json = value
                        .value
                        .as_text()
                        .expect("Schema.org json field should be stored as text")
                        .to_string();

                    webpage.schema_org = serde_json::from_str(&json).unwrap_or_default();
                }
                _ => {}
            }
        }

        webpage
    }
}

#[cfg(test)]
mod tests {
    use maplit::{hashmap, hashset};

    use crate::{
        ranking::{Ranker, SignalAggregator},
        searcher::SearchQuery,
        webpage::{Html, Link},
    };

    use super::*;

    const CONTENT: &str = "this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever";

    fn search<C>(
        index: &InvertedIndex,
        query: &Query,
        ctx: &Ctx,
        collector: C,
    ) -> Result<SearchResult>
    where
        C: Collector<Fruit = Vec<WebsitePointer>>,
    {
        let initial_result = index.search_initial(query, ctx, collector)?;

        let pointers: Vec<_> = initial_result.top_websites;

        let websites = index.retrieve_websites(&pointers, query)?;

        Ok(SearchResult {
            num_docs: initial_result.num_websites,
            documents: websites,
        })
    }

    #[test]
    fn simple_search() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");
        let ctx = index.local_search_ctx();

        let query = Query::parse(
            &ctx,
            &SearchQuery {
                query: "test".to_string(),
                ..Default::default()
            },
            &index,
        )
        .expect("Failed to parse query");

        let ranker = Ranker::new(
            SignalAggregator::new(Some(&query)),
            ctx.fastfield_reader.clone(),
        );
        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
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
        let ctx = index.local_search_ctx();

        let ranker = Ranker::new(
            SignalAggregator::new(Some(&query)),
            ctx.fastfield_reader.clone(),
        );

        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
        assert_eq!(result.num_docs, 1);
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com");
    }

    #[test]
    fn document_not_matching() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");

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

        let ctx = index.local_search_ctx();
        let query = Query::parse(
            &ctx,
            &SearchQuery {
                query: "this query should not match".to_string(),
                ..Default::default()
            },
            &index,
        )
        .expect("Failed to parse query");

        let ranker = Ranker::new(
            SignalAggregator::new(Some(&query)),
            ctx.fastfield_reader.clone(),
        );

        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
        assert_eq!(result.documents.len(), 0);
        assert_eq!(result.num_docs, 0);
    }

    #[test]
    fn english_stemming() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");

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

        let ctx = index.local_search_ctx();
        let query = Query::parse(
            &ctx,
            &SearchQuery {
                query: "runner".to_string(),
                ..Default::default()
            },
            &index,
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(
            SignalAggregator::new(Some(&query)),
            ctx.fastfield_reader.clone(),
        );

        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com");
    }

    #[test]
    fn stemmed_query_english() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");

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

        let ctx = index.local_search_ctx();
        let query = Query::parse(
            &ctx,
            &SearchQuery {
                query: "runners".to_string(),
                ..Default::default()
            },
            &index,
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(
            SignalAggregator::new(Some(&query)),
            ctx.fastfield_reader.clone(),
        );

        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com");
    }

    #[test]
    fn not_searchable_backlinks() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");

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

        let ctx = index.local_search_ctx();
        let query = Query::parse(
            &ctx,
            &SearchQuery {
                query: "great site".to_string(),
                ..Default::default()
            },
            &index,
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(
            SignalAggregator::new(Some(&query)),
            ctx.fastfield_reader.clone(),
        );

        let mut result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");

        result
            .documents
            .sort_by(|a, b| a.url.partial_cmp(&b.url).unwrap());

        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.a.com");
    }

    #[test]
    fn limited_top_docs() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");

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

        let ctx = index.local_search_ctx();
        let query = Query::parse(
            &ctx,
            &SearchQuery {
                query: "runner".to_string(),
                ..Default::default()
            },
            &index,
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(
            SignalAggregator::new(Some(&query)),
            ctx.fastfield_reader.clone(),
        );

        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
        assert_eq!(result.documents.len(), 20);
    }

    #[test]
    fn host_search() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");

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

        let ctx = index.local_search_ctx();
        let query = Query::parse(
            &ctx,
            &SearchQuery {
                query: "dr".to_string(),
                ..Default::default()
            },
            &index,
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(
            SignalAggregator::new(Some(&query)),
            ctx.fastfield_reader.clone(),
        );

        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
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

        let ctx = index.local_search_ctx();
        let query = Query::parse(
            &ctx,
            &SearchQuery {
                query: "website".to_string(),
                ..Default::default()
            },
            &index,
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(
            SignalAggregator::new(Some(&query)),
            ctx.fastfield_reader.clone(),
        );

        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
        assert_eq!(result.num_docs, 2);
        assert_eq!(result.documents.len(), 2);
        assert_eq!(result.documents[0].url, "https://www.example.com");
        assert_eq!(result.documents[1].url, "https://www.example.com");
    }

    #[test]
    fn match_across_fields() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");

        let ctx = index.local_search_ctx();
        let query = Query::parse(
            &ctx,
            &SearchQuery {
                query: "example test".to_string(),
                ..Default::default()
            },
            &index,
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(
            SignalAggregator::new(Some(&query)),
            ctx.fastfield_reader.clone(),
        );

        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
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

        let ctx = index.local_search_ctx();
        let ranker = Ranker::new(
            SignalAggregator::new(Some(&query)),
            ctx.fastfield_reader.clone(),
        );
        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
        assert_eq!(result.num_docs, 1);
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com");
    }

    #[test]
    fn only_show_primary_images_when_relevant() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");

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

        let ctx = index.local_search_ctx();
        let query = Query::parse(
            &ctx,
            &SearchQuery {
                query: "website".to_string(),
                ..Default::default()
            },
            &index,
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(
            SignalAggregator::new(Some(&query)),
            ctx.fastfield_reader.clone(),
        );

        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");

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
            &ctx,
            &SearchQuery {
                query: "best website".to_string(),
                ..Default::default()
            },
            &index,
        )
        .expect("Failed to parse query");

        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");

        assert_eq!(result.num_docs, 1);
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com");
        assert_eq!(result.documents[0].primary_image, None);
    }

    #[test]
    fn id_links_removed_during_indexing() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");

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

        let ctx = index.local_search_ctx();
        let query = Query::parse(
            &ctx,
            &SearchQuery {
                query: "website".to_string(),
                ..Default::default()
            },
            &index,
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(
            SignalAggregator::new(Some(&query)),
            ctx.fastfield_reader.clone(),
        );

        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
        assert_eq!(result.num_docs, 1);
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com");
    }

    #[test]
    fn remove_duplicates() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");

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

        let ctx = index.local_search_ctx();
        let query = Query::parse(
            &ctx,
            &SearchQuery {
                query: "dr".to_string(),
                ..Default::default()
            },
            &index,
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(
            SignalAggregator::new(Some(&query)),
            ctx.fastfield_reader.clone(),
        );

        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.dr.dk");
    }

    #[test]
    fn schema_org_stored() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");

        index
            .insert(Webpage::new(
                &format!(
                    r#"
                    <html>
                        <head>
                            <title>News website</title>
                            <script type="application/ld+json">{{"@context":"http://schema.org","@type":"LiveBlogPosting","coverageStartTime":"2022-11-14T23:45:00+00:00","coverageEndTime":"2022-11-15T23:45:00.000Z","datePublished":"2022-11-14T23:45:00+00:00","articleBody":"","author":[{{"name":"DR"}}],"copyrightYear":2022}}</script>
                        </head>
                        <body>
                            {CONTENT} test
                            <article itemscope="" itemType="http://schema.org/NewsArticle">
                                <div itemProp="publisher" itemscope="" itemType="https://schema.org/Organization"><meta itemProp="name" content="DR"/>
                                </div>
                            </article>
                        </body>
                    </html>
                "#
                ),
                "https://www.example.com",
            ))
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");

        let ctx = index.local_search_ctx();
        let query = Query::parse(
            &ctx,
            &SearchQuery {
                query: "test".to_string(),
                ..Default::default()
            },
            &index,
        )
        .expect("Failed to parse query");
        let ranker = Ranker::new(
            SignalAggregator::new(Some(&query)),
            ctx.fastfield_reader.clone(),
        );

        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
        assert_eq!(result.documents.len(), 1);
        let schema = result.documents[0].schema_org.clone();

        assert_eq!(schema.len(), 2);

        assert_eq!(
            schema[0].itemtype,
            Some(schema_org::OneOrMany::One("LiveBlogPosting".to_string()))
        );
        assert_eq!(
            schema[0].properties.get("coverageStartTime"),
            Some(&schema_org::OneOrMany::One(schema_org::Property::String(
                "2022-11-14T23:45:00+00:00".to_string()
            )))
        );
        assert_eq!(
            schema[1].itemtype,
            Some(schema_org::OneOrMany::One("NewsArticle".to_string()))
        );
        assert_eq!(
            schema[1].properties.get("publisher"),
            Some(&schema_org::OneOrMany::One(schema_org::Property::Item(
                schema_org::Item {
                    itemtype: Some(schema_org::OneOrMany::One("Organization".to_string())),
                    properties: hashmap! {
                        "name".to_string() => schema_org::OneOrMany::One(schema_org::Property::String("DR".to_string()))
                    }
                }
            )))
        );
    }

    #[test]
    fn get_webpage() {
        let mut index = InvertedIndex::temporary().expect("Unable to open index");

        index
            .insert(Webpage::new(
                &format!(
                    r#"
                    <html>
                        <head>
                            <title>News website</title>
                            <script type="application/ld+json">{{"@context":"http://schema.org","@type":"LiveBlogPosting","coverageStartTime":"2022-11-14T23:45:00+00:00","coverageEndTime":"2022-11-15T23:45:00.000Z","datePublished":"2022-11-14T23:45:00+00:00","articleBody":"","author":[{{"name":"DR"}}],"copyrightYear":2022}}</script>
                        </head>
                        <body>
                            {CONTENT} test
                            <article itemscope="" itemType="http://schema.org/NewsArticle">
                                <div itemProp="publisher" itemscope="" itemType="https://schema.org/Organization"><meta itemProp="name" content="DR"/>
                                </div>
                            </article>
                        </body>
                    </html>
                "#
                ),
                "https://www.example.com",
            ))
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");

        let webpage = index.get_webpage("https://www.example.com").unwrap();
        assert_eq!(webpage.title, "News website".to_string());
        assert_eq!(webpage.url, "https://www.example.com".to_string());
    }
}
