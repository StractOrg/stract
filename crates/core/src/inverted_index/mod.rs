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

//! The inverted index is the main data structure of the search engine.
//!
//! It is a mapping from terms to a list of documents. Imagine a hash map
//! { term -> \[doc1, doc2, doc3\] } etc. During search, we look up the terms
//! from the query in the index and perform an intersection of the lists of
//! documents. The result is a list of documents that match the query which
//! is then ranked.
//!
//! The inverted index is implemented using tantivy. The inverted index in
//! tantivy is actually a FST (finite state transducer) and not a hash map.
//! This allows us to perform more advanced queries than just term lookups,
//! but the principle is the same.

mod indexing;
mod key_phrase;
mod retrieved_webpage;
mod search;

pub use indexing::merge_tantivy_segments;
pub use key_phrase::KeyPhrase;
pub use retrieved_webpage::RetrievedWebpage;

use tantivy::directory::MmapDirectory;

use tantivy::index::SegmentId;
use tantivy::schema::Schema;
use tantivy::tokenizer::TokenizerManager;
use tantivy::{IndexReader, IndexWriter};

use crate::collector::{approx_count, Hashes};
use crate::config::SnippetConfig;
use crate::numericalfield_reader::NumericalFieldReader;

use crate::ranking::initial::Score;

use crate::schema::{numerical_field, Field, NumericalFieldEnum};
use crate::tokenizer::fields::{
    BigramTokenizer, Identity, JsonField, NewlineTokenizer, Stemmed, TrigramTokenizer, UrlTokenizer,
};
use crate::Result;
use crate::{schema::create_schema, tokenizer::FieldTokenizer};
use std::fs;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct InitialSearchResult {
    pub num_websites: approx_count::Count,
    pub top_websites: Vec<WebpagePointer>,
}

#[derive(
    Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Clone, PartialEq,
)]
pub struct WebpagePointer {
    pub score: Score,
    pub hashes: Hashes,
    pub address: DocAddress,
}

#[derive(
    Debug,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    Clone,
    Copy,
    PartialEq,
)]
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

fn register_tokenizers(manager: &TokenizerManager) {
    let tokenizer = FieldTokenizer::default();
    manager.register(tokenizer.as_str(), tokenizer);

    let tokenizer = FieldTokenizer::Stemmed(Stemmed::default());
    manager.register(tokenizer.as_str(), tokenizer);

    let tokenizer = FieldTokenizer::Identity(Identity::default());
    manager.register(tokenizer.as_str(), tokenizer);

    let tokenizer = FieldTokenizer::Bigram(BigramTokenizer::default());
    manager.register(tokenizer.as_str(), tokenizer);

    let tokenizer = FieldTokenizer::Trigram(TrigramTokenizer::default());
    manager.register(tokenizer.as_str(), tokenizer);

    let tokenizer = FieldTokenizer::Url(UrlTokenizer);
    manager.register(tokenizer.as_str(), tokenizer);

    let tokenizer = FieldTokenizer::Json(JsonField);
    manager.register(tokenizer.as_str(), tokenizer);

    let tokenizer = FieldTokenizer::Newline(NewlineTokenizer::default());
    manager.register(tokenizer.as_str(), tokenizer);
}

pub struct InvertedIndex {
    pub path: String,
    tantivy_index: tantivy::Index,
    writer: Option<IndexWriter>,
    reader: IndexReader,
    schema: Arc<Schema>,
    snippet_config: SnippetConfig,
    columnfield_reader: NumericalFieldReader,
}

impl InvertedIndex {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let schema = create_schema();

        let tantivy_index = if path.as_ref().exists() {
            let mmap_directory = MmapDirectory::open(&path)?;
            tantivy::Index::open(mmap_directory)?
        } else {
            let index_settings = tantivy::IndexSettings {
                sort_by_field: Some(tantivy::IndexSortByField {
                    field: Field::Numerical(NumericalFieldEnum::from(
                        numerical_field::PreComputedScore,
                    ))
                    .name()
                    .to_string(),
                    order: tantivy::Order::Desc,
                }),
                ..Default::default()
            };

            fs::create_dir_all(&path)?;
            let mmap_directory = MmapDirectory::open(&path)?;
            tantivy::Index::create(mmap_directory, schema.clone(), index_settings)?
        };

        register_tokenizers(tantivy_index.tokenizers());

        let reader: IndexReader = tantivy_index.reader_builder().try_into()?;
        let columnfield_reader = NumericalFieldReader::new(&reader.searcher());

        Ok(InvertedIndex {
            writer: None,
            reader,
            schema: Arc::new(schema),
            path: path.as_ref().to_str().unwrap().to_string(),
            tantivy_index,
            snippet_config: SnippetConfig::default(),
            columnfield_reader,
        })
    }

    pub fn re_open(&mut self) -> Result<()> {
        *self = Self::open(self.path.clone())?;
        Ok(())
    }

    pub fn columnfield_reader(&self) -> NumericalFieldReader {
        self.columnfield_reader.clone()
    }

    pub fn set_snippet_config(&mut self, config: SnippetConfig) {
        self.snippet_config = config;
    }

    pub fn tokenizers(&self) -> &TokenizerManager {
        self.tantivy_index.tokenizers()
    }

    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    pub fn schema_ref(&self) -> &Schema {
        &self.schema
    }

    pub fn segment_ids(&self) -> Vec<SegmentId> {
        self.tantivy_index.searchable_segment_ids().unwrap()
    }

    pub fn num_segments(&self) -> usize {
        self.tantivy_index.searchable_segments().unwrap().len()
    }

    pub fn num_documents(&self) -> u64 {
        self.reader.searcher().num_docs()
    }

    #[cfg(test)]
    pub fn temporary() -> Result<(Self, file_store::temp::TempDir)> {
        let dir = crate::gen_temp_dir()?;
        let mut s = Self::open(dir.as_ref().join("index"))?;

        s.prepare_writer()?;

        Ok((s, dir))
    }
}

#[derive(Debug, serde::Serialize, bincode::Encode)]
pub struct SearchResult {
    pub num_docs: approx_count::Count,
    pub documents: Vec<RetrievedWebpage>,
}

#[cfg(test)]
mod tests {
    use candle_core::Tensor;
    use maplit::hashmap;
    use url::Url;

    use crate::{
        collector::MainCollector,
        config::CollectorConfig,
        query::Query,
        ranking::{LocalRanker, SignalComputer},
        search_ctx::Ctx,
        searcher::SearchQuery,
        webgraph::{NodeID, SmallEdgeWithLabel},
        webpage::{schema_org, Html, Webpage},
        OneOrMany,
    };

    use super::*;

    const CONTENT: &str = "this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever";

    pub fn search(
        index: &InvertedIndex,
        query: &Query,
        ctx: &Ctx,
        collector: MainCollector,
    ) -> Result<SearchResult> {
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
        let (mut index, _dir) = InvertedIndex::temporary().expect("Unable to open index");
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

        let ranker = LocalRanker::new(
            SignalComputer::new(Some(&query)),
            ctx.columnfield_reader.clone(),
            CollectorConfig::default(),
        );
        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
        assert_eq!(result.documents.len(), 0);

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
        index.commit().expect("failed to commit index");
        let ctx = index.local_search_ctx();

        let ranker = LocalRanker::new(
            SignalComputer::new(Some(&query)),
            ctx.columnfield_reader.clone(),
            CollectorConfig::default(),
        );

        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com/");
    }

    #[test]
    fn document_not_matching() {
        let (mut index, _dir) = InvertedIndex::temporary().expect("Unable to open index");

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

        let ranker = LocalRanker::new(
            SignalComputer::new(Some(&query)),
            ctx.columnfield_reader.clone(),
            CollectorConfig::default(),
        );

        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
        assert_eq!(result.documents.len(), 0);
    }

    #[test]
    fn english_stemming() {
        let (mut index, _dir) = InvertedIndex::temporary().expect("Unable to open index");

        index
            .insert(
                &Webpage::test_parse(
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
                )
                .unwrap(),
            )
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
        let ranker = LocalRanker::new(
            SignalComputer::new(Some(&query)),
            ctx.columnfield_reader.clone(),
            CollectorConfig::default(),
        );

        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com/");
    }

    #[test]
    fn stemmed_query_english() {
        let (mut index, _dir) = InvertedIndex::temporary().expect("Unable to open index");

        index
            .insert(
                &Webpage::test_parse(
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
                )
                .unwrap(),
            )
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
        let ranker = LocalRanker::new(
            SignalComputer::new(Some(&query)),
            ctx.columnfield_reader.clone(),
            CollectorConfig::default(),
        );

        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com/");
    }

    #[test]
    fn not_searchable_backlinks() {
        let (mut index, _dir) = InvertedIndex::temporary().expect("Unable to open index");

        index
            .insert(
                &Webpage::test_parse(
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
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");
        let mut webpage = Webpage {
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
            )
            .unwrap(),
            host_centrality: 1.0,
            fetch_time_ms: 500,
            ..Default::default()
        };

        webpage.set_backlinks(vec![SmallEdgeWithLabel {
            from: NodeID::from(0u64),
            to: NodeID::from(1u64),
            label: "B site is great".to_string(),
            rel_flags: Default::default(),
        }]);

        index.insert(&webpage).expect("failed to insert webpage");

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
        let ranker = LocalRanker::new(
            SignalComputer::new(Some(&query)),
            ctx.columnfield_reader.clone(),
            CollectorConfig::default(),
        );

        let mut result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");

        result
            .documents
            .sort_by(|a, b| a.url.partial_cmp(&b.url).unwrap());

        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.a.com/");
    }

    #[test]
    fn limited_top_docs() {
        let (mut index, _dir) = InvertedIndex::temporary().expect("Unable to open index");

        for _ in 0..100 {
            let dedup_s = crate::rand_words(100);

            index
                .insert(
                    &Webpage::test_parse(
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
                    )
                    .unwrap(),
                )
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
        let ranker = LocalRanker::new(
            SignalComputer::new(Some(&query)),
            ctx.columnfield_reader.clone(),
            CollectorConfig::default(),
        );

        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
        assert_eq!(result.documents.len(), 20);
    }

    #[test]
    fn host_search() {
        let (mut index, _dir) = InvertedIndex::temporary().expect("Unable to open index");

        index
            .insert(
                &Webpage::test_parse(
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
                )
                .unwrap(),
            )
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
        let ranker = LocalRanker::new(
            SignalComputer::new(Some(&query)),
            ctx.columnfield_reader.clone(),
            CollectorConfig::default(),
        );

        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.dr.dk/");
    }

    #[test]
    fn merge() {
        let (index1, _dir1) = InvertedIndex::temporary().expect("Unable to open index");

        index1
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
                    "https://www.example.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");

        let (index2, _dir2) = InvertedIndex::temporary().expect("Unable to open index");

        index2
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
                    "https://www.example.com",
                )
                .unwrap(),
            )
            .expect("failed to insert webpage");

        let mut index = index1.merge(index2);
        index.prepare_writer().unwrap();
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
        let ranker = LocalRanker::new(
            SignalComputer::new(Some(&query)),
            ctx.columnfield_reader.clone(),
            CollectorConfig::default(),
        );

        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
        assert_eq!(result.documents.len(), 2);
        assert_eq!(result.documents[0].url, "https://www.example.com/");
        assert_eq!(result.documents[1].url, "https://www.example.com/");
    }

    #[test]
    fn match_across_fields() {
        let (mut index, _dir) = InvertedIndex::temporary().expect("Unable to open index");

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
        let ranker = LocalRanker::new(
            SignalComputer::new(Some(&query)),
            ctx.columnfield_reader.clone(),
            CollectorConfig::default(),
        );

        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
        assert_eq!(result.documents.len(), 0);

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
        index.commit().expect("failed to commit index");

        let ctx = index.local_search_ctx();
        let ranker = LocalRanker::new(
            SignalComputer::new(Some(&query)),
            ctx.columnfield_reader.clone(),
            CollectorConfig::default(),
        );
        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com/");
    }

    #[test]
    fn id_links_removed_during_indexing() {
        let (mut index, _dir) = InvertedIndex::temporary().expect("Unable to open index");

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
                                {CONTENT}
                            </body>
                        </html>
                    "#
                    ),
                    "https://www.example.com#tag",
                )
                .unwrap(),
            )
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
        let ranker = LocalRanker::new(
            SignalComputer::new(Some(&query)),
            ctx.columnfield_reader.clone(),
            CollectorConfig::default(),
        );

        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com/");
    }

    #[test]
    fn schema_org_stored() {
        let (mut index, _dir) = InvertedIndex::temporary().expect("Unable to open index");

        index
            .insert(&Webpage::test_parse(
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
            ).unwrap())
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
        let ranker = LocalRanker::new(
            SignalComputer::new(Some(&query)),
            ctx.columnfield_reader.clone(),
            CollectorConfig::default(),
        );

        let result =
            search(&index, &query, &ctx, ranker.collector(ctx.clone())).expect("Search failed");
        assert_eq!(result.documents.len(), 1);
        let schema = result.documents[0].schema_org.clone();

        assert_eq!(schema.len(), 2);

        assert_eq!(
            schema[0].itemtype,
            Some(OneOrMany::One("LiveBlogPosting".to_string()))
        );
        assert_eq!(
            schema[0].properties.get("coverageStartTime"),
            Some(&OneOrMany::One(schema_org::Property::String(
                "2022-11-14T23:45:00+00:00".to_string()
            )))
        );
        assert_eq!(
            schema[1].itemtype,
            Some(OneOrMany::One("NewsArticle".to_string()))
        );
        assert_eq!(
            schema[1].properties.get("publisher"),
            Some(&OneOrMany::One(schema_org::Property::Item(
                schema_org::Item {
                    itemtype: Some(OneOrMany::One("Organization".to_string())),
                    properties: hashmap! {
                        "name".to_string() => OneOrMany::One(schema_org::Property::String("DR".to_string()))
                    }
                }
            )))
        );
    }

    #[test]
    fn get_webpage() {
        let (mut index, _dir) = InvertedIndex::temporary().expect("Unable to open index");

        index
            .insert(&Webpage::test_parse(
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
            ).unwrap())
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");

        let webpage = index.get_webpage("https://www.example.com").unwrap();
        assert_eq!(webpage.title, "News website".to_string());
        assert_eq!(webpage.url, "https://www.example.com/".to_string());
    }

    #[test]
    fn get_homepage() {
        let (mut index, _dir) = InvertedIndex::temporary().expect("Unable to open index");

        index
            .insert(&Webpage::test_parse(
                &format!(
                    r#"
                    <html>
                        <head>
                            <title>News website</title>
                            <script type="application/ld+json">{{"@context":"http://schema.org","@type":"LiveBlogPosting","coverageStartTime":"2022-11-14T23:45:00+00:00","coverageEndTime":"2022-11-15T23:45:00.000Z","datePublished":"2022-11-14T23:45:00+00:00","articleBody":"","author":[{{"name":"DR"}}],"url":"https://www.example.com","mainEntityOfPage":"https://www.example.com"}}
                            </script>
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
            ).unwrap())
            .expect("failed to insert webpage");

        index.commit().expect("failed to commit index");

        let webpage = index
            .get_homepage(&Url::parse("https://www.example.com").unwrap())
            .unwrap();
        assert_eq!(webpage.title, "News website".to_string());
        assert_eq!(webpage.url, "https://www.example.com/".to_string());
    }

    #[test]
    fn test_title_embeddings_stored() {
        let (mut index, _dir) = InvertedIndex::temporary().expect("Unable to open index");

        let mut webpage = Webpage::test_parse(
            &format!(
                r#"
                <html>
                    <head>
                        <title>Test website A</title>
                    </head>
                    <body>
                        {CONTENT} test
                    </body>
                </html>
            "#,
                CONTENT = crate::rand_words(100)
            ),
            "https://www.a.com",
        )
        .unwrap();

        webpage.title_embedding =
            Some(Tensor::rand(0.0, 1.0, &[2, 2], &candle_core::Device::Cpu).unwrap());

        index.insert(&webpage).unwrap();

        let mut webpage = Webpage::test_parse(
            &format!(
                r#"
                <html>
                    <head>
                        <title>Test website B</title>
                    </head>
                    <body>
                        {CONTENT} test
                    </body>
                </html>
            "#,
                CONTENT = crate::rand_words(100)
            ),
            "https://www.b.com",
        )
        .unwrap();

        webpage.title_embedding = None;

        index.insert(&webpage).unwrap();
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

        let ranker = LocalRanker::new(
            SignalComputer::new(Some(&query)),
            ctx.columnfield_reader.clone(),
            CollectorConfig::default(),
        );

        let res = index
            .search_initial(&query, &ctx, ranker.collector(ctx.clone()))
            .unwrap();

        let columnfield_reader = index.columnfield_reader();

        let ranking_websites = index
            .retrieve_ranking_websites(
                &ctx,
                res.top_websites,
                ranker.computer(),
                &columnfield_reader,
            )
            .unwrap();

        assert_eq!(ranking_websites.len(), 2);
        assert!(ranking_websites[0].title_embedding().is_some());
        assert!(ranking_websites[1].title_embedding().is_none());
    }

    #[test]
    fn test_approximate_count() {
        let (mut index, _dir) = InvertedIndex::temporary().expect("Unable to open index");

        let webpage = Webpage::test_parse(
            &format!(
                r#"
                <html>
                    <head>
                        <title>Test website</title>
                    </head>
                    <body>
                        {CONTENT} test
                    </body>
                </html>
            "#,
                CONTENT = crate::rand_words(100)
            ),
            "https://www.a.com",
        )
        .unwrap();

        for _ in 0..1_000 {
            index.insert(&webpage).unwrap();
        }

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

        let collector_config = CollectorConfig {
            max_docs_considered: 100,
            ..Default::default()
        };

        let ranker = LocalRanker::new(
            SignalComputer::new(Some(&query)),
            ctx.columnfield_reader.clone(),
            collector_config,
        );

        let res = index
            .search_initial(&query, &ctx, ranker.collector(ctx.clone()))
            .unwrap();

        assert_eq!(res.num_websites, approx_count::Count::Approximate(1_000));
    }

    #[test]
    fn test_search_special_characters() {
        let (mut index, _dir) = InvertedIndex::temporary().expect("Unable to open index");

        let webpage = Webpage::test_parse(
            &format!(
                r#"
                <html>
                    <head>
                        <title>C++</title>
                    </head>
                    <body>
                        {CONTENT} test
                    </body>
                </html>
            "#,
                CONTENT = crate::rand_words(100)
            ),
            "https://www.a.com",
        )
        .unwrap();

        index.insert(&webpage).unwrap();

        index.commit().expect("failed to commit index");

        let ctx = index.local_search_ctx();

        let query = Query::parse(
            &ctx,
            &SearchQuery {
                query: "c++".to_string(),
                ..Default::default()
            },
            &index,
        )
        .expect("Failed to parse query");

        let ranker = LocalRanker::new(
            SignalComputer::new(Some(&query)),
            ctx.columnfield_reader.clone(),
            CollectorConfig::default(),
        );

        let res = index
            .search_initial(&query, &ctx, ranker.collector(ctx.clone()))
            .unwrap();

        assert_eq!(res.top_websites.len(), 1);

        let webpages = index.retrieve_websites(&res.top_websites, &query).unwrap();

        assert_eq!(webpages.len(), 1);
        assert_eq!(webpages[0].title, "C++");
        assert_eq!(webpages[0].url, "https://www.a.com/");
    }

    #[test]
    fn test_unicode_normalization() {
        let (mut index, _dir) = InvertedIndex::temporary().expect("Unable to open index");

        let webpage = Webpage::test_parse(
            &format!(
                r#"
                <html>
                    <head>
                        <title>æble café</title>
                    </head>
                    <body>
                        {CONTENT} test
                    </body>
                </html>
            "#,
                CONTENT = crate::rand_words(100)
            ),
            "https://www.a.com",
        )
        .unwrap();

        index.insert(&webpage).unwrap();

        index.commit().expect("failed to commit index");

        let test = |q: &str| {
            let ctx = index.local_search_ctx();

            let query = Query::parse(
                &ctx,
                &SearchQuery {
                    query: q.to_string(),
                    ..Default::default()
                },
                &index,
            )
            .expect("Failed to parse query");

            let ranker = LocalRanker::new(
                SignalComputer::new(Some(&query)),
                ctx.columnfield_reader.clone(),
                CollectorConfig::default(),
            );

            let res = index
                .search_initial(&query, &ctx, ranker.collector(ctx.clone()))
                .unwrap();

            assert_eq!(res.top_websites.len(), 1, "query: {}", q);

            let webpages = index.retrieve_websites(&res.top_websites, &query).unwrap();

            assert_eq!(webpages.len(), 1);
            assert_eq!(webpages[0].title, "æble café");
            assert_eq!(webpages[0].url, "https://www.a.com/");
        };

        test("cafe");
        test("café");
        test("æble");
        test("æble café");
    }
}
