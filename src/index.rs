use serde::{Deserialize, Serialize};
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
use tantivy::collector::{Collector, Count};
use tantivy::merge_policy::NoMergePolicy;
use tantivy::schema::Schema;
use tantivy::{DocAddress, Document, IndexReader, IndexWriter, LeasedItem};

use crate::directory::{self, DirEntry};
use crate::query::Query;
use crate::schema::{Field, ALL_FIELDS};
use crate::searcher::SearchResult;
use crate::snippet;
use crate::webpage::Webpage;
use crate::Result;
use crate::{schema::create_schema, tokenizer::Tokenizer};
use std::fs;
use std::path::Path;
use std::sync::Arc;

pub struct Index {
    pub path: String,
    tantivy_index: tantivy::Index,
    writer: IndexWriter,
    reader: IndexReader,
    schema: Arc<Schema>,
}

impl Index {
    #[cfg(test)]
    pub fn temporary() -> Result<Self> {
        let path = crate::gen_temp_path();
        Self::open(path)
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let schema = create_schema();

        let tantivy_index = if path.as_ref().exists() {
            tantivy::Index::open_in_dir(path.as_ref())?
        } else {
            fs::create_dir_all(path.as_ref())?;
            tantivy::Index::create_in_dir(path.as_ref(), schema.clone())?
        };

        let tokenizer = Tokenizer::default();
        tantivy_index
            .tokenizers()
            .register(tokenizer.as_str(), tokenizer);

        let tokenizer = Tokenizer::new_stemmed();
        tantivy_index
            .tokenizers()
            .register(tokenizer.as_str(), tokenizer);

        let writer = tantivy_index.writer(10_000_000_000)?;

        let merge_policy = NoMergePolicy::default();
        writer.set_merge_policy(Box::new(merge_policy));

        Ok(Index {
            writer,
            reader: tantivy_index.reader()?,
            schema: Arc::new(schema),
            path: path.as_ref().to_str().unwrap().to_string(),
            tantivy_index,
        })
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

    pub fn search<C>(&self, query: &Query, collector: C) -> Result<SearchResult>
    where
        C: Collector<Fruit = Vec<(f64, tantivy::DocAddress)>>,
    {
        let searcher = self.reader.searcher();

        let (count, docs) = searcher.search(query, &(Count, collector))?;

        let mut webpages: Vec<RetrievedWebpage> = docs
            .into_iter()
            .map(|(_score, doc_address)| Index::retrieve_doc(doc_address, &searcher))
            .filter(|page| page.is_ok())
            .map(|page| page.unwrap())
            .collect();

        for page in &mut webpages {
            page.snippet = snippet::generate(query, &page.body, &searcher)?;
        }

        Ok(SearchResult {
            num_docs: count,
            documents: webpages,
        })
    }

    pub fn merge_all_segments(&mut self) -> Result<()> {
        let segment_ids: Vec<_> = self
            .tantivy_index
            .load_metas()?
            .segments
            .into_iter()
            .map(|segment| segment.id())
            .collect();

        self.writer.merge(&segment_ids[..]).wait()?;

        Ok(())
    }

    fn retrieve_doc(
        doc_address: DocAddress,
        searcher: &LeasedItem<tantivy::Searcher>,
    ) -> Result<RetrievedWebpage> {
        let doc = searcher.doc(doc_address)?;
        Ok(RetrievedWebpage::from(doc))
    }

    pub fn merge(mut self, mut other: Index) -> Self {
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

    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}

#[derive(Default, Debug)]
pub struct RetrievedWebpage {
    pub title: String,
    pub url: String,
    pub snippet: String,
    pub body: String,
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
                Field::Body => {
                    webpage.body = value
                        .value
                        .as_text()
                        .expect("Body field should be text")
                        .to_string()
                }
                Field::Url => {
                    webpage.url = value
                        .value
                        .as_text()
                        .expect("Url field should be text")
                        .to_string()
                }
                Field::BacklinkText
                | Field::Centrality
                | Field::Host
                | Field::StemmedTitle
                | Field::StemmedBody
                | Field::Domain
                | Field::DomainIfHomepage
                | Field::IsHomepage => {}
            }
        }

        webpage
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
        index.writer.wait_merging_threads().unwrap();
        let root = directory::scan_folder(path).unwrap();

        Self { root }
    }
}

#[cfg(test)]
mod tests {
    use crate::{ranking::Ranker, webpage::Link};

    use super::*;

    const CONTENT: &str = "this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever";

    #[test]
    fn simple_search() {
        let mut index = Index::temporary().expect("Unable to open index");
        let query = Query::parse("website", index.schema()).expect("Failed to parse query");
        let ranker = Ranker::new(query.clone());

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
        let mut index = Index::temporary().expect("Unable to open index");
        let query = Query::parse("this query should not match", index.schema())
            .expect("Failed to parse query");
        let ranker = Ranker::new(query.clone());

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
        let mut index = Index::temporary().expect("Unable to open index");
        let query = Query::parse("runner", index.schema()).expect("Failed to parse query");
        let ranker = Ranker::new(query.clone());

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
        let mut index = Index::temporary().expect("Unable to open index");
        let query = Query::parse("runners", index.schema()).expect("Failed to parse query");
        let ranker = Ranker::new(query.clone());

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
    fn searchable_backlinks() {
        let mut index = Index::temporary().expect("Unable to open index");
        let query = Query::parse("great site", index.schema()).expect("Failed to parse query");
        let ranker = Ranker::new(query.clone());

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
            ))
            .expect("failed to parse webpage");

        index.commit().expect("failed to commit index");

        let mut result = index
            .search(&query, ranker.collector())
            .expect("Search failed");

        result
            .documents
            .sort_by(|a, b| a.url.partial_cmp(&b.url).unwrap());

        assert_eq!(result.documents.len(), 2);
        assert_eq!(result.documents[0].url, "https://www.a.com");
        assert_eq!(result.documents[1].url, "https://www.b.com");
    }

    #[test]
    fn limited_top_docs() {
        let mut index = Index::temporary().expect("Unable to open index");
        let query = Query::parse("runner", index.schema()).expect("Failed to parse query");
        let ranker = Ranker::new(query.clone());

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
        let mut index = Index::temporary().expect("Unable to open index");
        let query = Query::parse("dr", index.schema()).expect("Failed to parse query");
        let ranker = Ranker::new(query.clone());

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

    #[test]
    fn merge() {
        let mut index1 = Index::temporary().expect("Unable to open index");

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
            ))
            .expect("failed to parse webpage");

        let mut index2 = Index::temporary().expect("Unable to open index");

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
            ))
            .expect("failed to parse webpage");

        let mut index = index1.merge(index2);
        index.commit().unwrap();

        let query = Query::parse("website", index.schema()).expect("Failed to parse query");
        let ranker = Ranker::new(query.clone());

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
        let mut index = Index::temporary().expect("Unable to open index");
        let query = Query::parse("example test", index.schema()).expect("Failed to parse query");
        let ranker = Ranker::new(query.clone());

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
    fn proximity_ranking() {
        let mut index = Index::temporary().expect("Unable to open index");
        let query = Query::parse("termA termB", index.schema()).expect("Failed to parse query");
        let ranker = Ranker::new(query.clone());

        index
            .insert(Webpage::new(
                &format!(
                    r#"
                        <html>
                            <head>
                                <title>Test website</title>
                            </head>
                            <body>
                                {CONTENT} termA termB d d d d d d d d d
                            </body>
                        </html>
                    "#
                ),
                "https://www.first.com",
                vec![],
                1.0,
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
                                {CONTENT} termA d d d d d d d d d termB
                            </body>
                        </html>
                    "#
                ),
                "https://www.third.com",
                vec![],
                1.0,
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
                                {CONTENT} termA d d d d termB d d d d d
                            </body>
                        </html>
                    "#
                ),
                "https://www.second.com",
                vec![],
                1.0,
            ))
            .expect("failed to parse webpage");
        index.commit().expect("failed to commit index");

        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.num_docs, 3);
        assert_eq!(result.documents.len(), 3);
        assert_eq!(result.documents[0].url, "https://www.first.com");
        assert_eq!(result.documents[1].url, "https://www.second.com");
        assert_eq!(result.documents[2].url, "https://www.third.com");
    }
}
