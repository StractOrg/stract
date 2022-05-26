use crate::query::Query;
use crate::{webpage::Webpage, Result};
use std::path::Path;

fn create_schema() -> tantivy::schema::Schema {
    todo!();
}
pub struct Index {
    tantivy_index: tantivy::Index,
}

impl Index {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        todo!();
    }

    fn temporary() -> Self {
        let schema = create_schema();
        let tantivy_index = tantivy::Index::create_in_ram(schema);

        Self { tantivy_index }
    }

    pub fn insert(&mut self, webpage: Webpage) {
        todo!();
    }

    pub fn commit(&mut self) {
        todo!();
    }

    pub fn search(&self, query: &Query) -> Vec<FastWebpage> {
        todo!();
    }

    pub fn retrieve(&self, webpages: Vec<FastWebpage>) -> Vec<RetrievedWebpage> {
        todo!();
    }
}

pub struct FastWebpage {
    url: String,
}
pub struct RetrievedWebpage {
    title: String,
    url: String,
    snippet: String,
    body: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_search() {
        let mut index = Index::temporary();
        let query = Query::parse("website");

        let result = index.search(&query);
        assert_eq!(result.len(), 0);

        index.insert(Webpage::parse(
            r#"
            <html>
                <head>
                    <title>Test website</title>
                </head>
            </html>
            "#,
            "https://www.example.com",
        ));
        index.commit();

        let result = index.search(&query);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].url, "https://www.example.com");
    }

    #[test]
    fn retrieve_doc() {
        let mut index = Index::temporary();
        let query = Query::parse("website");

        index.insert(Webpage::parse(
            r#"
            <html>
                <head>
                    <title>Test website</title>
                </head>
            </html>
            "#,
            "https://www.example.com",
        ));
        index.commit();

        let result = index.search(&query);
        let full_docs = index.retrieve(result);
        assert_eq!(full_docs.len(), 1);
        assert_eq!(full_docs[0].url, "https://www.example.com");
        assert_eq!(full_docs[0].title, "Test website");
    }

    #[test]
    fn document_not_matching() {
        let mut index = Index::temporary();
        let query = Query::parse("this query should not match");

        index.insert(Webpage::parse(
            r#"
            <html>
                <head>
                    <title>Test website</title>
                </head>
            </html>
            "#,
            "https://www.example.com",
        ));
        index.commit();

        let result = index.search(&query);
        assert_eq!(result.len(), 0);
    }
}
