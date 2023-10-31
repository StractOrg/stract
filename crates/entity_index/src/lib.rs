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

use std::{
    collections::{BTreeMap, HashSet},
    fs,
    path::Path,
    sync::Arc,
    time::Duration,
};

use base64::{prelude::BASE64_STANDARD as BASE64_ENGINE, Engine};
use imager::{
    image_downloader::{ImageDownloadJob, ImageDownloader},
    image_store::{EntityImageStore, Image, ImageStore},
};
use kv::{rocksdb_store::RocksDbStore, Kv};
use serde::{Deserialize, Serialize};
use tantivy::{
    collector::TopDocs,
    query::{BooleanQuery, MoreLikeThisQuery, Occur, QueryClone, TermQuery},
    schema::{BytesOptions, IndexRecordOption, Schema, TextFieldIndexing, TextOptions},
    tokenizer::Tokenizer,
    DocAddress, IndexReader, IndexWriter, Searcher, TantivyDocument, Term,
};
use tokenizer::Normal;
use tracing::info;
use url::Url;

use self::entity::{Entity, Link, Span};
pub mod builder;
pub mod entity;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("tantivy error")]
    Tantivy(#[from] tantivy::error::TantivyError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct EntityIndex {
    image_store: EntityImageStore,
    image_downloader: ImageDownloader<String>,
    writer: IndexWriter,
    reader: IndexReader,
    schema: Arc<Schema>,
    stopwords: HashSet<String>,
    attribute_occurrences: Box<dyn Kv<String, u32>>,
}

fn schema() -> Schema {
    let mut builder = tantivy::schema::Schema::builder();

    builder.add_text_field(
        "title",
        TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer(Normal::as_str())
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            )
            .set_stored(),
    );
    builder.add_text_field(
        "abstract",
        TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer(Normal::as_str())
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            )
            .set_stored(),
    );
    builder.add_bytes_field("info", BytesOptions::default().set_stored());
    builder.add_bytes_field("links", BytesOptions::default().set_stored());
    builder.add_text_field(
        "has_image",
        TextOptions::default()
            .set_indexing_options(
                TextFieldIndexing::default()
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            )
            .set_stored(),
    );

    builder.build()
}

fn entity_to_tantivy(entity: Entity, schema: &tantivy::schema::Schema) -> TantivyDocument {
    let mut doc = TantivyDocument::new();

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
    let has_image = if entity.image.is_some() {
        "true"
    } else {
        "false"
    };

    doc.add_text(schema.get_field("has_image").unwrap(), has_image);

    doc
}

fn wikipedify_url(url: &str) -> Vec<Url> {
    let mut name = url.replace(' ', "_");

    if name.starts_with("File:") {
        if let Some(index) = name.find("File:") {
            name = name[index + "File:".len()..].to_string();
        }
    }

    let hex = format!("{:?}", md5::compute(&name));
    vec![
        Url::parse(&format!(
            "https://upload.wikimedia.org/wikipedia/commons/{:}/{:}",
            hex[0..1].to_string() + "/" + &hex[0..2],
            name
        ))
        .unwrap(),
        Url::parse(&format!(
            "https://upload.wikimedia.org/wikipedia/en/{:}/{:}",
            hex[0..1].to_string() + "/" + &hex[0..2],
            name
        ))
        .unwrap(),
    ]
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StoredEntity {
    pub title: String,
    pub entity_abstract: String,
    pub image_id: Option<String>,
    pub related_entities: Vec<EntityMatch>,
    pub best_info: Vec<(String, Span)>,
    pub links: Vec<Link>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EntityMatch {
    pub entity: StoredEntity,
    pub score: f32,
}

impl EntityIndex {
    pub fn open(path: &Path) -> Result<Self> {
        if !path.exists() {
            fs::create_dir_all(path)?;
        }

        let schema = schema();
        let tv_path = path.join("inverted_index");
        let tantivy_index = if tv_path.exists() {
            tantivy::Index::open_in_dir(&tv_path)?
        } else {
            fs::create_dir_all(&tv_path)?;
            tantivy::Index::create_in_dir(&tv_path, schema.clone())?
        };

        let attribute_occurrences =
            Box::new(RocksDbStore::open(&path.join("attribute_occurrences")));

        let stopwords: HashSet<String> = include_str!("../../core/stopwords/English.txt")
            .lines()
            .take(50)
            .map(str::to_ascii_lowercase)
            .collect();

        tantivy_index.tokenizers().register(
            Normal::as_str(),
            Normal::with_stopwords(stopwords.clone().into_iter().collect()),
        );

        let image_store = EntityImageStore::open(&path.join("images"));

        let writer = tantivy_index.writer(10_000_000_000)?;
        let reader = tantivy_index.reader()?;

        Ok(Self {
            image_store,
            writer,
            reader,
            image_downloader: ImageDownloader::new(),
            schema: Arc::new(schema),
            stopwords,
            attribute_occurrences,
        })
    }

    fn best_info(&self, info: BTreeMap<String, Span>) -> Vec<(String, Span)> {
        let mut info: Vec<_> = info.into_iter().collect();

        info.sort_by(|(a, _), (b, _)| {
            self.get_attribute_occurrence(b)
                .unwrap_or(0)
                .cmp(&self.get_attribute_occurrence(a).unwrap_or(0))
        });

        info.into_iter()
            .map(|(key, mut value)| {
                if let Some(start) = value.text.find(|c: char| !(c.is_whitespace() || c == '*')) {
                    value.text.replace_range(0..start, "");
                    for link in &mut value.links {
                        link.start = link.start.saturating_sub(start);
                        link.end = link.end.saturating_sub(start);
                    }
                }

                (key.replace('_', " "), value)
            })
            .filter(|(key, _)| {
                !matches!(
                    key.as_str(),
                    "caption"
                        | "image size"
                        | "label"
                        | "landscape"
                        | "signature"
                        | "name"
                        | "website"
                        | "logo"
                        | "image caption"
                        | "alt"
                )
            })
            .take(5)
            .collect()
    }

    pub fn insert(&mut self, entity: Entity) {
        for attribute in entity.info.keys() {
            let current = self.attribute_occurrences.get(attribute).unwrap_or(0);
            self.attribute_occurrences
                .insert(attribute.to_string(), current + 1);
        }

        if let Some(image) = entity.image.clone() {
            let image = wikipedify_url(&image).into_iter().collect();

            self.image_downloader.schedule(ImageDownloadJob {
                key: entity.title.clone(),
                urls: image,
                timeout: Some(Duration::from_secs(10)),
            });
        }
        let doc = entity_to_tantivy(entity, &self.schema);
        self.writer.add_document(doc).unwrap();
    }

    pub fn commit(&mut self) {
        self.writer.commit().unwrap();
        self.reader.reload().unwrap();
        self.attribute_occurrences.flush();
        info!("downloading images");
        self.image_downloader.download(&mut self.image_store);
    }

    fn related_entities(&self, doc: DocAddress) -> Vec<EntityMatch> {
        let searcher = self.reader.searcher();
        let more_like_this_query = MoreLikeThisQuery::builder()
            .with_min_doc_frequency(1)
            .with_min_term_frequency(1)
            .with_min_word_length(2)
            .with_boost_factor(1.0)
            .with_document(doc);

        let image_query = TermQuery::new(
            Term::from_field_text(self.schema.get_field("has_image").unwrap(), "true"),
            IndexRecordOption::WithFreqsAndPositions,
        );

        let query = BooleanQuery::from(vec![
            (Occur::Must, more_like_this_query.box_clone()),
            (Occur::Must, image_query.box_clone()),
        ]);

        match searcher.search(&query, &TopDocs::with_limit(1_000)) {
            Ok(result) => result
                .into_iter()
                .filter(|(_, related_doc)| doc != *related_doc)
                .map(|(score, doc_address)| {
                    let entity =
                        self.retrieve_stored_entity(&searcher, doc_address, false, false, false);

                    EntityMatch { entity, score }
                })
                .filter(|m| m.entity.image_id.is_some())
                .take(4)
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    pub fn search(&self, query: &str) -> Option<EntityMatch> {
        let searcher = self.reader.searcher();

        let title = self.schema.get_field("title").unwrap();
        let entity_abstract = self.schema.get_field("abstract").unwrap();

        let mut term_queries = Vec::new();
        let mut tokenizer = Normal::default();
        let mut stream = tokenizer.token_stream(query);
        while let Some(token) = stream.next() {
            if self.stopwords.contains(&token.text) {
                continue;
            }

            term_queries.push((
                Occur::Must,
                TermQuery::new(
                    Term::from_field_text(title, &token.text),
                    IndexRecordOption::WithFreqsAndPositions,
                )
                .box_clone(),
            ));

            term_queries.push((
                Occur::Should,
                TermQuery::new(
                    Term::from_field_text(entity_abstract, &token.text),
                    IndexRecordOption::WithFreqsAndPositions,
                )
                .box_clone(),
            ));
        }

        let query = BooleanQuery::from(term_queries);

        searcher
            .search(&query, &TopDocs::with_limit(1))
            .unwrap()
            .first()
            .map(|(score, doc_address)| {
                let entity = self.retrieve_stored_entity(&searcher, *doc_address, true, true, true);

                EntityMatch {
                    entity,
                    score: *score,
                }
            })
    }

    fn retrieve_stored_entity(
        &self,
        searcher: &Searcher,
        doc_address: DocAddress,
        get_related: bool,
        decode_info: bool,
        get_links: bool,
    ) -> StoredEntity {
        let title = self.schema.get_field("title").unwrap();
        let entity_abstract = self.schema.get_field("abstract").unwrap();
        let info = self.schema.get_field("info").unwrap();
        let links = self.schema.get_field("links").unwrap();

        let doc: TantivyDocument = searcher.doc(doc_address).unwrap();
        let title = doc
            .get_first(title)
            .and_then(|val| match val {
                tantivy::schema::OwnedValue::Str(string) => Some(string.clone()),
                _ => None,
            })
            .unwrap();

        let entity_abstract = doc
            .get_first(entity_abstract)
            .and_then(|val| match val {
                tantivy::schema::OwnedValue::Str(string) => Some(string.clone()),
                _ => None,
            })
            .unwrap();

        let info = if decode_info {
            bincode::deserialize(
                doc.get_first(info)
                    .and_then(|val| match val {
                        tantivy::schema::OwnedValue::Bytes(bytes) => Some(bytes),
                        _ => None,
                    })
                    .unwrap(),
            )
            .unwrap()
        } else {
            BTreeMap::new()
        };

        let best_info = self.best_info(info);

        let related_entities = if get_related {
            self.related_entities(doc_address)
        } else {
            Vec::new()
        };

        let image_id = BASE64_ENGINE.encode(&title);
        let image_id = if self.retrieve_image(&image_id).is_some() {
            Some(image_id)
        } else {
            None
        };

        let links: Vec<Link> = if get_links {
            bincode::deserialize(
                doc.get_first(links)
                    .and_then(|val| match val {
                        tantivy::schema::OwnedValue::Bytes(bytes) => Some(bytes),
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
            image_id,
            related_entities,
            best_info,
            links,
        }
    }

    pub fn retrieve_image(&self, key: &str) -> Option<Image> {
        let key = BASE64_ENGINE.decode(key).ok()?;
        let key = String::from_utf8(key).ok()?;

        self.image_store.get(&key)
    }

    pub fn get_attribute_occurrence(&self, attribute: &String) -> Option<u32> {
        self.attribute_occurrences.get(attribute)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn wikipedia_image_url_aristotle() {
        assert_eq!(
            wikipedify_url("Aristotle Altemps Inv8575.jpg")
                .first()
                .unwrap()
                .as_str(),
            "https://upload.wikimedia.org/wikipedia/commons/a/ae/Aristotle_Altemps_Inv8575.jpg"
        );
    }

    #[test]
    fn wikipedia_image_url_with_file() {
        assert_eq!(
            wikipedify_url("File:Aristotle Altemps Inv8575.jpg")
                .first()
                .unwrap()
                .as_str(),
            "https://upload.wikimedia.org/wikipedia/commons/a/ae/Aristotle_Altemps_Inv8575.jpg"
        );
    }

    #[test]
    fn stopwords_title_ignored() {
        let mut index = EntityIndex::open(&stdx::gen_temp_path()).unwrap();

        index.insert(Entity {
            title: "the ashes".to_string(),
            page_abstract: Span {
                text: String::new(),
                links: Vec::new(),
            },
            info: BTreeMap::new(),
            image: None,
            paragraphs: Vec::new(),
            categories: HashSet::new(),
        });

        index.commit();

        assert!(index.search("the").is_none());
        assert_eq!(
            index.search("ashes").unwrap().entity.title.as_str(),
            "the ashes"
        );
        assert_eq!(
            index.search("the ashes").unwrap().entity.title.as_str(),
            "the ashes"
        );
    }
}
