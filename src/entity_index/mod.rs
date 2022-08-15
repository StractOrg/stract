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

use std::{collections::BTreeMap, fs, path::Path, sync::Arc, time::Duration};

use tantivy::{
    collector::TopDocs,
    query::{BooleanQuery, MoreLikeThisQuery, Occur, QueryClone, TermQuery},
    schema::{BytesOptions, IndexRecordOption, Schema, TextFieldIndexing, TextOptions},
    DocAddress, IndexReader, IndexWriter, LeasedItem, Searcher, Term,
};
use tracing::info;

use crate::{
    image_downloader::{ImageDownloadJob, ImageDownloader},
    image_store::{EntityImageStore, Image, ImageStore},
    tokenizer::NormalTokenizer,
    webpage::Url,
    Result,
};

use self::entity::{Entity, Link, Span};
pub(crate) mod entity;

pub struct EntityIndex {
    image_store: EntityImageStore,
    image_downloader: ImageDownloader<String>,
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
    builder.add_bytes_field("info", BytesOptions::default().set_stored());
    builder.add_bytes_field("links", BytesOptions::default().set_stored());

    builder.build()
}

fn entity_to_tantivy(entity: Entity, schema: &tantivy::schema::Schema) -> tantivy::Document {
    let mut doc = tantivy::Document::new();

    doc.add_text(schema.get_field("title").unwrap(), entity.title);
    doc.add_text(
        schema.get_field("abstract").unwrap(),
        entity.page_abstract.text,
    );
    doc.add_bytes(
        schema.get_field("info").unwrap(),
        bincode::serialize(&entity.info).unwrap(),
    );
    doc.add_bytes(
        schema.get_field("links").unwrap(),
        bincode::serialize(&entity.page_abstract.links).unwrap(),
    );

    doc
}

fn wikipedify_url(url: Url) -> Url {
    let name = url.raw().replace(' ', "_");
    let hex = format!("{:?}", md5::compute(&name));
    format!(
        "https://upload.wikimedia.org/wikipedia/commons/{:}/{:}",
        hex[0..1].to_string() + "/" + &hex[0..2],
        name
    )
    .into()
}

#[derive(Debug)]
pub struct StoredEntity {
    pub title: String,
    pub entity_abstract: String,
    pub image: Option<String>,
    pub related_entities: Vec<StoredEntity>,
    pub info: BTreeMap<String, Span>,
    pub links: Vec<Link>,
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

        tantivy_index
            .tokenizers()
            .register(NormalTokenizer::as_str(), NormalTokenizer::default());

        let image_store = EntityImageStore::open(path.as_ref().join("images"));

        let writer = tantivy_index.writer(10_000_000_000)?;
        let reader = tantivy_index.reader()?;

        Ok(Self {
            image_store,
            writer,
            reader,
            image_downloader: ImageDownloader::new(),
            schema: Arc::new(schema),
        })
    }

    pub fn insert(&mut self, entity: Entity) {
        if let Some(image) = entity.image.clone() {
            let image = wikipedify_url(image);
            self.image_downloader.schedule(ImageDownloadJob {
                key: entity.title.clone(),
                url: image,
                timeout: Some(Duration::from_secs(10)),
            })
        }
        let doc = entity_to_tantivy(entity, &self.schema);
        self.writer.add_document(doc).unwrap();
    }

    pub fn commit(&mut self) {
        self.writer.commit().unwrap();
        info!("downloading images");
        // self.image_downloader.download(&mut self.image_store);
    }

    fn related_entities(&self, doc: DocAddress) -> Vec<StoredEntity> {
        let searcher = self.reader.searcher();
        let query = MoreLikeThisQuery::builder()
            .with_min_doc_frequency(1)
            .with_max_doc_frequency(10)
            .with_min_term_frequency(1)
            .with_min_word_length(2)
            .with_max_word_length(5)
            .with_boost_factor(1.0)
            .with_document(doc);

        match searcher.search(&query, &TopDocs::with_limit(10)) {
            Ok(result) => result
                .into_iter()
                .filter(|(_, related_doc)| doc != *related_doc)
                .map(|(_, doc_address)| {
                    self.retrieve_stored_entity(&searcher, &doc_address, false, false, false)
                })
                .filter(|entity| entity.image.is_some())
                .take(4)
                .collect(),
            Err(_) => Vec::new(),
        }
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
                self.retrieve_stored_entity(&searcher, doc_address, true, true, true)
            })
    }

    fn retrieve_stored_entity(
        &self,
        searcher: &LeasedItem<Searcher>,
        doc_address: &DocAddress,
        get_related: bool,
        decode_info: bool,
        get_links: bool,
    ) -> StoredEntity {
        let title = self.schema.get_field("title").unwrap();
        let entity_abstract = self.schema.get_field("abstract").unwrap();
        let info = self.schema.get_field("info").unwrap();
        let links = self.schema.get_field("links").unwrap();

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

        let info = if decode_info {
            bincode::deserialize(
                doc.get_first(info)
                    .and_then(|val| match val {
                        tantivy::schema::Value::Bytes(bytes) => Some(bytes),
                        _ => None,
                    })
                    .unwrap(),
            )
            .unwrap()
        } else {
            BTreeMap::new()
        };

        let related_entities = if get_related {
            self.related_entities(*doc_address)
        } else {
            Vec::new()
        };

        let image = self.retrieve_image(&title).map(|_| title.clone());

        let links: Vec<Link> = if get_links {
            bincode::deserialize(
                doc.get_first(links)
                    .and_then(|val| match val {
                        tantivy::schema::Value::Bytes(bytes) => Some(bytes),
                        _ => None,
                    })
                    .unwrap(),
            )
            .unwrap()
        } else {
            Vec::new()
        };

        StoredEntity {
            title,
            entity_abstract,
            image,
            related_entities,
            info,
            links,
        }
    }

    pub fn retrieve_image(&self, key: &String) -> Option<Image> {
        self.image_store.get(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wikipedia_image_url_aristotle() {
        assert_eq!(
            wikipedify_url("Aristotle Altemps Inv8575.jpg".to_string().into()).full(),
            "https://upload.wikimedia.org/wikipedia/commons/a/ae/Aristotle_Altemps_Inv8575.jpg"
                .to_string()
        );
    }
}
