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

use std::{fs, path::Path, sync::Arc};

use tantivy::{
    collector::TopDocs,
    query::{BooleanQuery, Occur, QueryClone, TermQuery},
    schema::{IndexRecordOption, Schema, TextFieldIndexing, TextOptions},
    IndexReader, IndexWriter, Term,
};

use crate::{
    image_store::EntityImageStore,
    tokenizer::{NormalTokenizer, Tokenizer},
    Result,
};

use self::entity::Entity;
pub(crate) mod entity;

pub struct EntityIndex {
    image_store: EntityImageStore,
    writer: IndexWriter,
    reader: IndexReader,
    schema: Arc<Schema>,
}

fn schema() -> Schema {
    let mut builder = tantivy::schema::Schema::builder();

    builder.add_text_field(
        "title",
        TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer(NormalTokenizer::as_str())
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            )
            .set_stored(),
    );
    builder.add_text_field(
        "abstract",
        TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer(NormalTokenizer::as_str())
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            )
            .set_stored(),
    );

    builder.build()
}

fn entity_to_tantivy(entity: Entity, schema: &tantivy::schema::Schema) -> tantivy::Document {
    let mut doc = tantivy::Document::new();

    doc.add_text(schema.get_field("title").unwrap(), entity.title);
    doc.add_text(
        schema.get_field("abstract").unwrap(),
        entity.page_abstract.text,
    );

    doc
}

#[derive(Debug)]
pub struct StoredEntity {
    pub title: String,
    pub entity_abstract: String,
}

impl EntityIndex {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        if !path.as_ref().exists() {
            fs::create_dir_all(path.as_ref())?;
        }

        let schema = schema();
        let tv_path = path.as_ref().join("inverted_index");
        let tantivy_index = if tv_path.exists() {
            tantivy::Index::open_in_dir(&tv_path)?
        } else {
            fs::create_dir_all(&tv_path)?;
            tantivy::Index::create_in_dir(&tv_path, schema.clone())?
        };

        let tokenizer = Tokenizer::default();
        tantivy_index
            .tokenizers()
            .register(tokenizer.as_str(), tokenizer);

        let tokenizer = Tokenizer::new_stemmed();
        tantivy_index
            .tokenizers()
            .register(tokenizer.as_str(), tokenizer);

        let image_store = EntityImageStore::open(path.as_ref().join("images"));

        let writer = tantivy_index.writer(10_000_000_000)?;
        let reader = tantivy_index.reader()?;

        Ok(Self {
            image_store,
            writer,
            reader,
            schema: Arc::new(schema),
        })
    }

    pub fn insert(&mut self, entity: Entity) {
        let doc = entity_to_tantivy(entity, &self.schema);
        self.writer.add_document(doc).unwrap();
    }

    pub fn commit(&mut self) {
        self.writer.commit().unwrap();
    }

    pub fn search(&self, query: String) -> Option<StoredEntity> {
        let searcher = self.reader.searcher();

        let title = self.schema.get_field("title").unwrap();
        let entity_abstract = self.schema.get_field("abstract").unwrap();

        let term_queries: Vec<(Occur, Box<dyn tantivy::query::Query>)> = query
            .split(' ')
            .flat_map(|term| {
                vec![
                    (
                        Occur::Must,
                        TermQuery::new(
                            Term::from_field_text(title, &term.to_ascii_lowercase()),
                            IndexRecordOption::WithFreqsAndPositions,
                        )
                        .box_clone(),
                    ),
                    (
                        Occur::Should,
                        TermQuery::new(
                            Term::from_field_text(entity_abstract, &term.to_ascii_lowercase()),
                            IndexRecordOption::WithFreqsAndPositions,
                        )
                        .box_clone(),
                    ),
                ]
            })
            .collect();
        let query = BooleanQuery::from(term_queries);

        searcher
            .search(&query, &TopDocs::with_limit(1))
            .unwrap()
            .first()
            .map(|(_score, doc_address)| {
                let doc = searcher.doc(*doc_address).unwrap();
                let title = doc
                    .get_first(title)
                    .and_then(|val| match val {
                        tantivy::schema::Value::Str(string) => Some(string.clone()),
                        _ => None,
                    })
                    .unwrap();

                let entity_abstract = doc
                    .get_first(entity_abstract)
                    .and_then(|val| match val {
                        tantivy::schema::Value::Str(string) => Some(string.clone()),
                        _ => None,
                    })
                    .unwrap();

                StoredEntity {
                    title,
                    entity_abstract,
                }
            })
    }
}
