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

use std::{
    collections::{BTreeMap, HashSet},
    fs,
    path::Path,
    sync::Arc,
    time::Duration,
};

use tantivy::{
    collector::TopDocs,
    query::{BooleanQuery, MoreLikeThisQuery, Occur, QueryClone, TermQuery},
    schema::{BytesOptions, IndexRecordOption, Schema, TextFieldIndexing, TextOptions},
    tokenizer::Tokenizer,
    DocAddress, IndexReader, IndexWriter, Searcher, Term,
};
use tracing::info;

use crate::{
    image_downloader::{ImageDownloadJob, ImageDownloader},
    image_store::{EntityImageStore, Image, ImageStore},
    kv::{rocksdb_store::RocksDbStore, Kv},
    tokenizer::Normal,
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
    let has_image = if entity.image.is_some() {
        "true"
    } else {
        "false"
    };

    doc.add_text(schema.get_field("has_image").unwrap(), has_image);

    doc
}

fn wikipedify_url(url: &Url) -> Vec<Url> {
    let mut name = url.raw().replace(' ', "_");

    if name.starts_with("File:") {
        if let Some(index) = name.find("File:") {
            name = name[index + "File:".len()..].to_string();
        }
    }

    let hex = format!("{:?}", md5::compute(&name));
    vec![
        format!(
            "https://upload.wikimedia.org/wikipedia/commons/{:}/{:}",
            hex[0..1].to_string() + "/" + &hex[0..2],
            name
        )
        .into(),
        format!(
            "https://upload.wikimedia.org/wikipedia/en/{:}/{:}",
            hex[0..1].to_string() + "/" + &hex[0..2],
            name
        )
        .into(),
    ]
}

#[derive(Debug, PartialEq, Eq)]
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

        let attribute_occurrences = RocksDbStore::open(path.as_ref().join("attribute_occurrences"));

        let stopwords: HashSet<String> = include_str!("../../stopwords/English.txt")
            .lines()
            .take(50)
            .map(str::to_ascii_lowercase)
            .collect();

        tantivy_index.tokenizers().register(
            Normal::as_str(),
            Normal::with_stopwords(stopwords.clone().into_iter().collect()),
        );

        let image_store = EntityImageStore::open(path.as_ref().join("images"));

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

    pub fn insert(&mut self, entity: Entity) {
        for attribute in entity.info.keys() {
            let current = self.attribute_occurrences.get(attribute).unwrap_or(0);
            self.attribute_occurrences
                .insert(attribute.to_string(), current + 1);
        }

        if let Some(image) = entity.image.clone() {
            let image = wikipedify_url(&image)
                .into_iter()
                .filter(Url::is_valid_uri)
                .collect();

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

    fn related_entities(&self, doc: DocAddress) -> Vec<StoredEntity> {
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
                .map(|(_, doc_address)| {
                    self.retrieve_stored_entity(&searcher, doc_address, false, false, false)
                })
                .filter(|entity| entity.image.is_some())
                .take(4)
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    pub fn search(&self, query: &str) -> Option<StoredEntity> {
        let searcher = self.reader.searcher();

        let title = self.schema.get_field("title").unwrap();
        let entity_abstract = self.schema.get_field("abstract").unwrap();

        let mut term_queries = Vec::new();
        let mut stream = Normal::default().token_stream(query);
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
            .map(|(_score, doc_address)| {
                self.retrieve_stored_entity(&searcher, *doc_address, true, true, true)
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

        let doc = searcher.doc(doc_address).unwrap();
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
            self.related_entities(doc_address)
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
            wikipedify_url(&"Aristotle Altemps Inv8575.jpg".to_string().into())
                .first()
                .unwrap()
                .full(),
            "https://upload.wikimedia.org/wikipedia/commons/a/ae/Aristotle_Altemps_Inv8575.jpg"
                .to_string()
        );
    }

    #[test]
    fn wikipedia_image_url_with_file() {
        assert_eq!(
            wikipedify_url(&"File:Aristotle Altemps Inv8575.jpg".to_string().into())
                .first()
                .unwrap()
                .full(),
            "https://upload.wikimedia.org/wikipedia/commons/a/ae/Aristotle_Altemps_Inv8575.jpg"
                .to_string()
        );
    }

    #[test]
    fn stopwords_title_ignored() {
        let mut index = EntityIndex::open(crate::gen_temp_path()).unwrap();

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

        assert_eq!(index.search("the"), None);
        assert_eq!(index.search("ashes").unwrap().title.as_str(), "the ashes");
        assert_eq!(
            index.search("the ashes").unwrap().title.as_str(),
            "the ashes"
        );
    }
}
