use tantivy::collector::Count;
use tantivy::{DocAddress, Document, LeasedItem, Searcher};

use crate::query::Query;
use crate::ranking;
use crate::schema::{create_schema, Field, ALL_FIELDS};
use crate::webpage::Webpage;
use crate::Result;
use std::path::Path;

pub struct Index {
    tantivy_index: tantivy::Index,
    writer: tantivy::IndexWriter,
    reader: tantivy::IndexReader,
    schema: tantivy::schema::Schema,
}

impl Index {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        todo!();
    }

    pub(crate) fn temporary() -> Result<Self> {
        let schema = create_schema();
        let tantivy_index = tantivy::Index::create_in_ram(schema);

        Ok(Self {
            writer: tantivy_index.writer(100_000_000)?,
            reader: tantivy_index.reader()?,
            schema: create_schema(),
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

    pub fn search(&self, query: &Query) -> Result<SearchResult> {
        let tantivy_query = query.tantivy(&self.schema, self.tantivy_index.tokenizers());
        let searcher = self.reader.searcher();

        let (count, docs) =
            searcher.search(&tantivy_query, &(Count, ranking::initial_collector()))?;

        let fast_pages: Vec<RetrievedWebpage> = docs
            .into_iter()
            .map(|(_score, doc_address)| Index::retrieve_doc(doc_address, &searcher))
            .filter(|page| page.is_ok())
            .map(|page| page.unwrap())
            .collect();

        Ok(SearchResult {
            num_docs: count,
            documents: fast_pages,
        })
    }

    fn retrieve_doc(
        doc_address: DocAddress,
        searcher: &LeasedItem<Searcher>,
    ) -> Result<RetrievedWebpage> {
        let doc = searcher.doc(doc_address)?;
        Ok(RetrievedWebpage::from(doc))
    }
}

pub struct SearchResult {
    pub num_docs: usize,
    pub documents: Vec<RetrievedWebpage>,
}

#[derive(Default)]
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
                Field::BacklinkText | Field::Centrality => {}
            }
        }

        webpage
    }
}

#[cfg(test)]
mod tests {
    use crate::webpage::Link;

    use super::*;

    #[test]
    fn simple_search() {
        let mut index = Index::temporary().expect("Unable to open index");
        let query = Query::parse("website").expect("Failed to parse query");

        let result = index.search(&query).expect("Search failed");
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

        let result = index.search(&query).expect("Search failed");
        assert_eq!(result.num_docs, 1);
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com");
    }

    #[test]
    fn document_not_matching() {
        let mut index = Index::temporary().expect("Unable to open index");
        let query = Query::parse("this query should not match").expect("Failed to parse query");

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

        let result = index.search(&query).expect("Search failed");
        assert_eq!(result.documents.len(), 0);
        assert_eq!(result.num_docs, 0);
    }

    #[test]
    fn english_stemming() {
        let mut index = Index::temporary().expect("Unable to open index");
        let query = Query::parse("runner").expect("Failed to parse query");

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

        let result = index.search(&query).expect("Search failed");
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com");
    }

    #[test]
    fn searchable_backlinks() {
        let mut index = Index::temporary().expect("Unable to open index");
        let query = Query::parse("great site").expect("Failed to parse query");

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

        let mut result = index.search(&query).expect("Search failed");

        result
            .documents
            .sort_by(|a, b| a.url.partial_cmp(&b.url).unwrap());

        assert_eq!(result.documents.len(), 2);
        assert_eq!(result.documents[0].url, "https://www.a.com");
        assert_eq!(result.documents[1].url, "https://www.b.com");
    }

    // #[test]
    // fn snippet() {
    //     todo!();
    // }
}
