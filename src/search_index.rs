use tantivy::collector::Count;
use tantivy::schema::{IndexRecordOption, TextFieldIndexing, TextOptions};
use tantivy::{DocAddress, Document, LeasedItem, Searcher};

use crate::query::Query;
use crate::ranking;
use crate::{webpage::Webpage, Result};
use std::path::Path;

#[derive(Clone)]
pub enum Field {
    Title,
    Body,
    Url,
}
pub static ALL_FIELDS: [Field; 3] = [Field::Title, Field::Body, Field::Url];

impl Field {
    fn default_options(&self) -> tantivy::schema::TextOptions {
        TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("en_stem")
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            )
            .set_stored()
    }
    pub fn options(&self) -> tantivy::schema::TextOptions {
        match self {
            Field::Title => self.default_options(),
            Field::Body => self.default_options(),
            Field::Url => self.default_options(),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Field::Title => "title",
            Field::Body => "body",
            Field::Url => "url",
        }
    }
}

fn create_schema() -> tantivy::schema::Schema {
    let mut builder = tantivy::schema::Schema::builder();

    for field in &ALL_FIELDS {
        builder.add_text_field(field.as_str(), field.options());
    }

    builder.build()
}
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

    fn temporary() -> Result<Self> {
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
    num_docs: usize,
    documents: Vec<RetrievedWebpage>,
}

#[derive(Default)]
pub struct RetrievedWebpage {
    title: String,
    url: String,
    snippet: String,
    body: String,
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
            }
        }

        webpage
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_search() {
        let mut index = Index::temporary().expect("Unable to open index");
        let query = Query::parse("website").expect("Failed to parse query");

        let result = index.search(&query).expect("Search failed");
        assert_eq!(result.documents.len(), 0);
        assert_eq!(result.num_docs, 0);

        index
            .insert(Webpage::parse(
                r#"
            <html>
                <head>
                    <title>Test website</title>
                </head>
            </html>
            "#,
                "https://www.example.com",
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
            .insert(Webpage::parse(
                r#"
            <html>
                <head>
                    <title>Test website</title>
                </head>
            </html>
            "#,
                "https://www.example.com",
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
            .insert(Webpage::parse(
                r#"
            <html>
                <head>
                    <title>Website for runners</title>
                </head>
            </html>
            "#,
                "https://www.example.com",
            ))
            .expect("failed to parse webpage");
        index.commit().expect("failed to commit index");

        let result = index.search(&query).expect("Search failed");
        assert_eq!(result.documents.len(), 1);
        assert_eq!(result.documents[0].url, "https://www.example.com");
    }

    // #[test]
    // fn snippet() {
    //     todo!();
    // }
}
