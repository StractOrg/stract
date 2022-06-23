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
use tantivy::{DocAddress, Document, LeasedItem};

use crate::query::Query;
use crate::schema::{Field, ALL_FIELDS};
use crate::searcher::SearchResult;
use crate::snippet;
use crate::webpage::Webpage;
use crate::Result;
use std::path::Path;

pub struct Index {
    pub(crate) tantivy_index: tantivy::Index,
    pub(crate) writer: tantivy::IndexWriter,
    pub(crate) reader: tantivy::IndexReader,
    pub(crate) schema: tantivy::schema::Schema,
}

impl Index {
    pub fn open<P: AsRef<Path>>(_path: P) -> Result<Self> {
        todo!();
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
        let tantivy_query = query.tantivy(&self.schema, self.tantivy_index.tokenizers());
        let searcher = self.reader.searcher();

        let (count, docs) = searcher.search(&tantivy_query, &(Count, collector))?;

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

    fn retrieve_doc(
        doc_address: DocAddress,
        searcher: &LeasedItem<tantivy::Searcher>,
    ) -> Result<RetrievedWebpage> {
        let doc = searcher.doc(doc_address)?;
        Ok(RetrievedWebpage::from(doc))
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
                Field::BacklinkText | Field::Centrality | Field::FastUrl => {}
            }
        }

        webpage
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::temporary_index;
    use crate::{ranking::Ranker, webpage::Link};

    use super::*;

    #[test]
    fn simple_search() {
        let mut index = temporary_index().expect("Unable to open index");
        let query = Query::parse("website").expect("Failed to parse query");
        let ranker = Ranker::new(query.clone());

        let result = index
            .search(&query, ranker.collector())
            .expect("Search failed");
        assert_eq!(result.documents.len(), 0);
        assert_eq!(result.num_docs, 0);

        index
            .insert(Webpage::new(
                r#"
            <html>
                <head>
                    <title>Test website</title>
                </head>
            </html>
            "#,
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
        let mut index = temporary_index().expect("Unable to open index");
        let query = Query::parse("this query should not match").expect("Failed to parse query");
        let ranker = Ranker::new(query.clone());

        index
            .insert(Webpage::new(
                r#"
            <html>
                <head>
                    <title>Test website</title>
                </head>
            </html>
            "#,
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
        let mut index = temporary_index().expect("Unable to open index");
        let query = Query::parse("runner").expect("Failed to parse query");
        let ranker = Ranker::new(query.clone());

        index
            .insert(Webpage::new(
                r#"
            <html>
                <head>
                    <title>Website for runners</title>
                </head>
            </html>
            "#,
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
        let mut index = temporary_index().expect("Unable to open index");
        let query = Query::parse("runners").expect("Failed to parse query");
        let ranker = Ranker::new(query.clone());

        index
            .insert(Webpage::new(
                r#"
            <html>
                <head>
                    <title>Fast runner</title>
                </head>
            </html>
            "#,
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
        let mut index = temporary_index().expect("Unable to open index");
        let query = Query::parse("great site").expect("Failed to parse query");
        let ranker = Ranker::new(query.clone());

        index
            .insert(Webpage::new(
                r#"
            <html>
                <head>
                    <title>Website A</title>
                </head>
                <a href="https://www.b.com">B site is great</a>
            </html>
            "#,
                "https://www.a.com",
                vec![],
                1.0,
            ))
            .expect("failed to parse webpage");
        index
            .insert(Webpage::new(
                r#"
            <html>
                <head>
                    <title>Website B</title>
                </head>
            </html>
            "#,
                "https://www.b.com",
                vec![Link {
                    source: "https://www.a.com".to_string(),
                    destination: "https://www.b.com".to_string(),
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
        let mut index = temporary_index().expect("Unable to open index");
        let query = Query::parse("runner").expect("Failed to parse query");
        let ranker = Ranker::new(query.clone());

        for _ in 0..100 {
            index
                .insert(Webpage::new(
                    r#"
                    <html>
                        <head>
                            <title>Website for runners</title>
                        </head>
                    </html>
                    "#,
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
        let mut index = temporary_index().expect("Unable to open index");
        let query = Query::parse("dr").expect("Failed to parse query");
        let ranker = Ranker::new(query.clone());

        index
            .insert(Webpage::new(
                r#"
            <html>
                <head>
                    <title>News website</title>
                </head>
            </html>
            "#,
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
}
