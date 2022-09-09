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
use crate::{schema_org::SchemaOrg, tokenizer, Error, Result};
use chrono::{DateTime, FixedOffset};
use itertools::Itertools;
use kuchiki::{iter::NodeEdge, traits::TendrilSink, NodeRef};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    panic,
};
use tantivy::tokenizer::{PreTokenizedString, Tokenizer};
use uuid::Uuid;
use whatlang::Lang;

mod just_text;
pub mod region;
mod url;

use crate::schema::{Field, ALL_FIELDS, CENTRALITY_SCALING};

pub use self::url::Url;
use self::{
    just_text::{JustText, Paragraph},
    region::Region,
};

static URL_REGEX: once_cell::sync::Lazy<Regex> = once_cell::sync::Lazy::new(|| {
    Regex::new(r#"(((http|ftp|https):/{2})+(([0-9a-z_-]+\.)+(aero|asia|biz|cat|com|coop|edu|gov|info|int|jobs|mil|mobi|museum|name|net|org|pro|tel|travel|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cu|cv|cx|cy|cz|cz|de|dj|dk|dm|do|dz|ec|ee|eg|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mn|mn|mo|mp|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|nom|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ra|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|sj|sk|sl|sm|sn|so|sr|st|su|sv|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw|arpa)(:[0-9]+)?((/([~0-9a-zA-Z\#\+%@\./_-]+))?(\?[0-9a-zA-Z\+%@/&\[\];=_-]+)?)?))\b"#).unwrap()
});

#[derive(PartialEq, Eq, Debug)]
pub struct FaviconLink {
    pub link: Url,
    width: Option<u32>,
    height: Option<u32>,
    image_type: Option<String>,
}

#[derive(PartialEq, Eq, Debug)]
pub struct ImageLink {
    pub url: Url,
    pub title: Option<String>,
    pub description: Option<String>,
}

pub struct Preprocessor<const N: usize> {
    removed_tags: [&'static str; N],
    num_open_tags: [i64; N],
}

impl<const N: usize> Preprocessor<N> {
    pub fn new(removed_tags: [&'static str; N]) -> Self {
        Self {
            removed_tags,
            num_open_tags: [0; N],
        }
    }

    pub fn update(&mut self, edge: &NodeEdge<NodeRef>) {
        match edge {
            NodeEdge::Start(node) => {
                if let Some(element) = node.as_element() {
                    let element_name: &str = element.name.local.borrow();
                    if let Some((_, n)) = self
                        .removed_tags
                        .iter()
                        .zip(self.num_open_tags.iter_mut())
                        .find(|(name, _)| **name == element_name)
                    {
                        *n += 1;
                    }
                }
            }
            NodeEdge::End(node) => {
                if let Some(element) = node.as_element() {
                    let element_name: &str = element.name.local.borrow();
                    if let Some((_, n)) = self
                        .removed_tags
                        .iter()
                        .zip(self.num_open_tags.iter_mut())
                        .find(|(name, _)| **name == element_name)
                    {
                        *n -= 1;
                    }
                }
            }
        }
    }

    pub fn is_inside_removed(&self) -> bool {
        self.num_open_tags.iter().any(|n| *n > 0)
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredPrimaryImage {
    pub uuid: Uuid,
    pub title_terms: HashSet<String>,
    pub description_terms: HashSet<String>,
}

pub struct Webpage {
    pub html: Html,
    pub backlinks: Vec<Link>,
    pub centrality: f64,
    pub fetch_time_ms: u64,
    pub primary_image: Option<StoredPrimaryImage>,
}

impl Webpage {
    #[cfg(test)]
    pub fn new(
        html: &str,
        url: &str,
        backlinks: Vec<Link>,
        centrality: f64,
        fetch_time_ms: u64,
    ) -> Self {
        let html = Html::parse(html, url);

        Self {
            html,
            backlinks,
            centrality,
            fetch_time_ms,
            primary_image: None,
        }
    }

    pub fn into_tantivy(self, schema: &tantivy::schema::Schema) -> Result<tantivy::Document> {
        let region = Region::guess_from(&self);

        let mut doc = self.html.into_tantivy(schema)?;

        if let Ok(region) = region {
            doc.add_u64(
                schema
                    .get_field(Field::Region.as_str())
                    .expect("Failed to get region field"),
                region.id() as u64,
            );
        } else {
            doc.add_u64(
                schema
                    .get_field(Field::Region.as_str())
                    .expect("Failed to get region field"),
                Region::All.id() as u64,
            );
        }

        let backlink_text: String = itertools::intersperse(
            self.backlinks.into_iter().map(|link| link.text),
            "\n".to_string(),
        )
        .collect();

        doc.add_text(
            schema
                .get_field(Field::BacklinkText.as_str())
                .expect("Failed to get backlink-text field"),
            backlink_text,
        );

        doc.add_u64(
            schema
                .get_field(Field::Centrality.as_str())
                .expect("Failed to get centrality field"),
            (self.centrality * CENTRALITY_SCALING as f64) as u64,
        );

        doc.add_u64(
            schema
                .get_field(Field::FetchTimeMs.as_str())
                .expect("Failed to get fetch_time_ms field"),
            self.fetch_time_ms,
        );

        let image = bincode::serialize(&self.primary_image).unwrap();
        doc.add_bytes(
            schema
                .get_field(Field::PrimaryImage.as_str())
                .expect("Failed to get primary_image field"),
            image,
        );

        Ok(doc)
    }

    pub(crate) fn set_primary_image(&mut self, uuid: Uuid, image: ImageLink) {
        let mut title_terms = HashSet::new();

        if let Some(title) = image.title {
            let mut tokenizer = tokenizer::Normal::default().token_stream(title.as_str());
            while let Some(token) = tokenizer.next() {
                title_terms.insert(token.text.clone());
            }
        }

        let mut description_terms = HashSet::new();

        if let Some(description) = image.description {
            let mut tokenizer = tokenizer::Normal::default().token_stream(description.as_str());
            while let Some(token) = tokenizer.next() {
                description_terms.insert(token.text.clone());
            }
        }

        self.primary_image = Some(StoredPrimaryImage {
            uuid,
            title_terms,
            description_terms,
        });
    }
}

struct Script {
    attributes: HashMap<String, String>,
    content: String,
}

#[derive(Debug)]
pub struct Html {
    url: Url,
    root: NodeRef, // this is reference counted (cheap to clone)
    all_text: Option<String>,
    clean_text: Option<String>,
    lang: Option<Lang>,
}

impl Html {
    pub fn parse(html: &str, url: &str) -> Self {
        Self::parse_including_text(html, url, true)
    }

    pub fn parse_including_text(html: &str, url: &str, include_text: bool) -> Self {
        let root = kuchiki::parse_html().one(html);

        let mut all_text = None;
        let mut clean_text = None;
        let mut lang = None;

        if include_text {
            let paragraphs = JustText::paragraphs(root.clone());

            lang = paragraphs
                .iter()
                .max_by_key(|paragraph| paragraph.text.len())
                .and_then(|paragraph| {
                    whatlang::detect(&paragraph.text).and_then(|info| {
                        if info.is_reliable() && info.confidence() > 0.95 {
                            Some(info.lang())
                        } else {
                            None
                        }
                    })
                });

            all_text = Html::calculate_all_text(&paragraphs, &lang.unwrap_or(Lang::Eng));
            clean_text = Html::calculate_clean_text(&paragraphs, &lang.unwrap_or(Lang::Eng));
        }

        Self {
            root,
            all_text,
            clean_text,
            lang,
            url: url.to_string().into(),
        }
    }

    pub fn links(&self) -> Vec<Link> {
        let mut links = Vec::new();
        let mut open_links = Vec::new();
        let mut preprocessor = Preprocessor::new(["script", "style", "head", "noscript"]);

        for edge in self.root.traverse() {
            preprocessor.update(&edge);
            if preprocessor.is_inside_removed() {
                continue;
            }

            match edge {
                NodeEdge::Start(node) => {
                    if let Some(element) = node.as_element() {
                        if &element.name.local == "a" {
                            open_links.push((String::new(), element.attributes.clone()));
                        }
                    }
                }
                NodeEdge::End(node) => {
                    if let Some(element) = node.as_element() {
                        if &element.name.local == "a" {
                            if let Some((text, attributes)) = open_links.pop() {
                                if let Some(dest) = attributes.borrow().get("href") {
                                    links.push(Link {
                                        source: self.url.clone(),
                                        destination: dest.to_string().into(),
                                        text: text.trim().to_string(),
                                    });
                                }
                            }
                        }
                    }

                    if let Some(text) = node.as_text() {
                        let raw_text = text.borrow();
                        let text = raw_text.trim();

                        if !text.is_empty() {
                            for (link_text, _) in &mut open_links {
                                link_text.push('\n');
                                link_text.push_str(text);
                            }
                        }
                    }
                }
            }
        }

        links
    }

    pub fn favicon(&self) -> Option<FaviconLink> {
        for node in self.root.select("link").unwrap() {
            if !matches!(node.attributes.borrow().get("rel"), Some("icon")) {
                continue;
            }

            if let Some(link) = node.attributes.borrow().get("href") {
                let (width, height) = match node.attributes.borrow().get("sizes") {
                    Some(size) => {
                        if let Some((width, height)) = size.split_once('x') {
                            (width.parse().ok(), height.parse().ok())
                        } else {
                            (None, None)
                        }
                    }
                    _ => (None, None),
                };

                let image_type = node.attributes.borrow().get("type").map(|t| t.to_string());
                let mut link: Url = link.to_string().into();

                if !link.is_full_path() {
                    link.prefix_with(&self.url);
                }

                let favicon = FaviconLink {
                    link,
                    width,
                    height,
                    image_type,
                };

                return Some(favicon);
            }
        }

        None
    }

    fn calculate_clean_text(paragraphs: &[Paragraph], lang: &Lang) -> Option<String> {
        let text = JustText::default().extract_from_paragraphs(paragraphs, lang);

        if text.is_empty() {
            None
        } else {
            Some(text)
        }
    }

    pub fn clean_text(&self) -> Option<String> {
        self.clean_text.clone()
    }

    fn calculate_all_text(paragraphs: &[Paragraph], lang: &Lang) -> Option<String> {
        let text = JustText {
            max_link_density: 2.0,
            length_low: 0,
            length_high: 0,
            stopwords_low: -1.0,
            stopwords_high: -1.0,
            max_heading_distance: 10000,
        }
        .extract_from_paragraphs(paragraphs, lang);

        if text.is_empty() {
            None
        } else {
            Some(text)
        }
    }

    fn all_text(&self) -> Option<String> {
        self.all_text.clone()
    }

    pub fn title(&self) -> Option<String> {
        if let Ok(title) = self.root.select_first("title") {
            let title = title.text_contents().trim().to_string();
            if title.is_empty() {
                None
            } else {
                Some(title)
            }
        } else {
            None
        }
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub fn metadata(&self) -> Vec<Meta> {
        let mut metas = Vec::new();

        for node in self.root.select("meta").unwrap() {
            if let Some(element) = node.as_node().as_element() {
                metas.push(
                    element
                        .attributes
                        .borrow()
                        .map
                        .iter()
                        .map(|(name, attr)| {
                            (
                                name.local.borrow().to_string(),
                                attr.borrow().value.to_string(),
                            )
                        })
                        .collect(),
                );
            }
        }

        metas
    }

    fn pretokenize_title(&self) -> Result<PreTokenizedString> {
        let title = self.title();

        if title.is_none() {
            return Err(Error::EmptyField("title"));
        }
        let title = title.unwrap();

        Ok(self.pretokenize_string(title))
    }

    fn pretokenize_all_text(&self) -> Result<PreTokenizedString> {
        let all_text = self.all_text();

        if all_text.is_none() {
            return Err(Error::EmptyField("all body"));
        }
        let all_text = all_text.unwrap();

        Ok(self.pretokenize_string(all_text))
    }

    fn pretokenize_clean_text(&self) -> PreTokenizedString {
        let clean_text = self.clean_text().unwrap_or_default();
        self.pretokenize_string(clean_text)
    }

    fn pretokenize_url(&self) -> PreTokenizedString {
        let url = self.url().full();
        self.pretokenize_string(url)
    }

    fn pretokenize_description(&self) -> PreTokenizedString {
        let text = self.description().unwrap_or_default();

        self.pretokenize_string(text)
    }

    fn pretokenize_string(&self, text: String) -> PreTokenizedString {
        let mut tokens = Vec::new();

        {
            let mut stream = tokenizer::Normal::default().token_stream(&text);
            while let Some(token) = stream.next() {
                tokens.push(token.clone());
            }
        }

        PreTokenizedString { text, tokens }
    }

    pub fn into_tantivy(self, schema: &tantivy::schema::Schema) -> Result<tantivy::Document> {
        let mut doc = tantivy::Document::new();

        let title = self.pretokenize_title()?;
        let all_text = self.pretokenize_all_text()?;
        let clean_text = self.pretokenize_clean_text();
        let url = self.pretokenize_url();
        let description = self.pretokenize_description();

        for field in &ALL_FIELDS {
            let tantivy_field = schema
                .get_field(field.as_str())
                .unwrap_or_else(|| panic!("Unknown field: {}", field.as_str()));

            match field {
                Field::Title => doc.add_pre_tokenized_text(tantivy_field, title.clone()),
                Field::StemmedTitle => {
                    let mut tokens = title.tokens.clone();
                    stem_tokens(&mut tokens, self.lang.unwrap_or(Lang::Eng));

                    doc.add_pre_tokenized_text(
                        tantivy_field,
                        PreTokenizedString {
                            text: title.text.clone(),
                            tokens,
                        },
                    );
                }
                Field::CleanBody => doc.add_pre_tokenized_text(tantivy_field, clean_text.clone()),
                Field::StemmedCleanBody => {
                    let mut tokens = clean_text.tokens.clone();
                    stem_tokens(&mut tokens, self.lang.unwrap_or(Lang::Eng));

                    doc.add_pre_tokenized_text(
                        tantivy_field,
                        PreTokenizedString {
                            text: clean_text.text.clone(),
                            tokens,
                        },
                    );
                }
                Field::Description => {
                    doc.add_pre_tokenized_text(tantivy_field, description.clone());
                }
                Field::Url => doc.add_pre_tokenized_text(tantivy_field, url.clone()),
                Field::Site => doc.add_text(tantivy_field, self.url().site()),
                Field::Domain => doc.add_text(tantivy_field, self.url().domain()),
                Field::DomainIfHomepage => {
                    if self.url().is_homepage() {
                        doc.add_text(tantivy_field, self.url().domain());
                    } else {
                        doc.add_text(tantivy_field, "");
                    }
                }
                Field::IsHomepage => {
                    doc.add_u64(tantivy_field, if self.url().is_homepage() { 1 } else { 0 });
                }
                Field::LastUpdated => doc.add_u64(
                    tantivy_field,
                    self.updated_time()
                        .map_or(0, |time| time.timestamp().max(0) as u64),
                ),
                Field::AllBody => doc.add_pre_tokenized_text(tantivy_field, all_text.clone()),
                Field::NumTrackers => doc.add_u64(tantivy_field, self.trackers().len() as u64),
                Field::NumUrlTokens => doc.add_u64(tantivy_field, url.tokens.len() as u64),
                Field::NumTitleTokens => doc.add_u64(tantivy_field, title.tokens.len() as u64),
                Field::NumCleanBodyTokens => {
                    doc.add_u64(tantivy_field, clean_text.tokens.len() as u64)
                }
                Field::NumDescriptionTokens => {
                    doc.add_u64(tantivy_field, description.tokens.len() as u64)
                }
                Field::BacklinkText
                | Field::Centrality
                | Field::FetchTimeMs
                | Field::Region
                | Field::PrimaryImage => {}
            }
        }

        Ok(doc)
    }

    fn scripts(&self) -> Vec<Script> {
        let mut scripts = Vec::new();

        for node in self.root.select("script").unwrap() {
            let content = node.text_contents().trim().to_string();
            let attributes = node
                .attributes
                .borrow()
                .map
                .iter()
                .map(|(name, attr)| {
                    (
                        name.local.borrow().to_string(),
                        attr.borrow().value.to_string(),
                    )
                })
                .collect();

            scripts.push(Script {
                attributes,
                content,
            });
        }

        scripts
    }

    pub fn schema_org(&self) -> Vec<SchemaOrg> {
        let mut schemas = Vec::new();

        for schema in self.scripts().into_iter().filter(|script| {
            matches!(
                script.attributes.get("type").map(String::as_str),
                Some("application/ld+json")
            )
        }) {
            if let Ok(schema) = serde_json::from_str(&schema.content) {
                schemas.push(schema);
            }
        }

        schemas
    }

    pub fn trackers(&self) -> Vec<Url> {
        let mut links: Vec<Url> = Vec::new();

        for script in self.scripts() {
            if let Some(link) = script.attributes.get("src") {
                links.push(link.to_string().into());
            }

            for res in URL_REGEX.find_iter(&script.content) {
                links.push(res.as_str().to_string().into());
            }
        }

        for node in self.root.select("link").unwrap() {
            if let Some(link) = node.attributes.borrow().get("href") {
                links.push(link.to_string().into());
            }
        }

        links
            .into_iter()
            .filter(|link| !link.site().is_empty())
            .filter(|link| link.site() != self.url().site())
            .unique_by(|link| link.site().to_string())
            .collect()
    }

    fn article_modified_time(&self) -> Option<DateTime<FixedOffset>> {
        self.metadata()
            .into_iter()
            .find(|metadata| {
                if let Some(property) = metadata.get("property") {
                    property == &String::from("article:modified_time")
                } else {
                    false
                }
            })
            .and_then(|metadata| {
                metadata
                    .get("content")
                    .and_then(|time| DateTime::parse_from_rfc3339(time).ok())
            })
    }

    fn og_updated_time(&self) -> Option<DateTime<FixedOffset>> {
        self.metadata()
            .into_iter()
            .find(|metadata| {
                if let Some(property) = metadata.get("property") {
                    property == &String::from("og:updated_time")
                } else {
                    false
                }
            })
            .and_then(|metadata| {
                metadata
                    .get("content")
                    .and_then(|time| DateTime::parse_from_rfc3339(time).ok())
            })
    }

    fn og_image(&self) -> Option<ImageLink> {
        self.metadata()
            .into_iter()
            .find(|metadata| {
                if let Some(property) = metadata.get("property") {
                    property == &String::from("og:image")
                } else {
                    false
                }
            })
            .and_then(|metadata| metadata.get("content").map(|link| link.clone().into()))
            .map(|url| ImageLink {
                url,
                title: self.og_title(),
                description: self.description(),
            })
    }

    #[allow(unreachable_patterns)]
    fn schema_org_images(&self) -> Vec<Url> {
        self.schema_org()
            .into_iter()
            .filter(|schema| matches!(schema, SchemaOrg::ImageObject(_)))
            .filter_map(|schema| {
                match schema {
                    SchemaOrg::ImageObject(image) => image.content_url.map(Url::from),
                    _ => None, // has been filtered, so only image is possible
                }
            })
            .collect()
    }

    pub fn updated_time(&self) -> Option<DateTime<FixedOffset>> {
        self.og_updated_time()
            .or_else(|| self.article_modified_time())
    }

    pub fn primary_image(&self) -> Option<ImageLink> {
        self.og_image()
            .or_else(|| {
                self.schema_org_images()
                    .first()
                    .cloned()
                    .map(|url| ImageLink {
                        url,
                        title: self.og_title(),
                        description: self.description(),
                    })
            })
            .map(|mut image| {
                if !image.url.is_full_path() {
                    image.url.prefix_with(&self.url);
                }

                image
            })
    }

    pub fn description(&self) -> Option<String> {
        self.metadata()
            .into_iter()
            .find(|metadata| {
                if let Some(property) = metadata.get("property") {
                    property == &String::from("og:description")
                } else {
                    false
                }
            })
            .and_then(|metadata| metadata.get("content").cloned())
    }

    pub fn og_title(&self) -> Option<String> {
        self.metadata()
            .into_iter()
            .find(|metadata| {
                if let Some(property) = metadata.get("property") {
                    property == &String::from("og:title")
                } else {
                    false
                }
            })
            .and_then(|metadata| metadata.get("content").cloned())
    }
}

fn stemmer_from_lang(lang: &Lang) -> rust_stemmers::Stemmer {
    match lang {
        Lang::Ara => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Arabic),
        Lang::Dan => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Danish),
        Lang::Nld => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Dutch),
        Lang::Fin => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Finnish),
        Lang::Fra => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::French),
        Lang::Deu => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::German),
        Lang::Ell => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Greek),
        Lang::Hun => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Hungarian),
        Lang::Ita => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Italian),
        Lang::Por => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Portuguese),
        Lang::Ron => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Romanian),
        Lang::Rus => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Russian),
        Lang::Spa => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Spanish),
        Lang::Swe => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Swedish),
        Lang::Tam => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Tamil),
        Lang::Tur => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::Turkish),
        _ => rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::English),
    }
}

fn stem_tokens(tokens: &mut [tantivy::tokenizer::Token], lang: Lang) {
    let stemmer = stemmer_from_lang(&lang);
    for token in tokens {
        // TODO remove allocation
        if let Ok(stemmed_str) = panic::catch_unwind(|| stemmer.stem(&token.text).into_owned()) {
            token.text.clear();
            token.text.push_str(&stemmed_str);
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Link {
    pub source: Url,
    pub destination: Url,
    pub text: String,
}

pub type Meta = HashMap<String, String>;

#[cfg(test)]
mod tests {
    // TODO: make test macro to test both dom parsers

    use crate::{schema::create_schema, schema_org::ImageObject};

    use super::*;

    const CONTENT: &str = "this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever this is the best example website ever";

    #[test]
    fn simple() {
        let raw = format!(
            r#"
            <html>
                <head>
                    <title>Best website</title>
                    <meta name="meta1" content="value">
                </head>
                <body>
                    <a href="example.com">Link to example</a>
                    <p>{CONTENT}</p>
                </body>
            </html>
        "#
        );

        let webpage = Html::parse(&raw, "https://www.example.com/whatever");

        assert_eq!(webpage.title(), Some("Best website".to_string()));

        assert_eq!(
            webpage.links(),
            vec![Link {
                source: "https://www.example.com/whatever".to_string().into(),
                destination: "example.com".to_string().into(),
                text: "Link to example".to_string()
            }]
        );
        assert_eq!(webpage.clean_text(), Some(CONTENT.to_string()));

        let mut expected_meta = HashMap::new();
        expected_meta.insert("name".to_string(), "meta1".to_string());
        expected_meta.insert("content".to_string(), "value".to_string());

        assert_eq!(webpage.metadata(), vec![expected_meta]);
        assert_eq!(
            webpage.url().to_string().as_str(),
            "https://www.example.com/whatever"
        );
        assert_eq!(webpage.url().site(), "www.example.com");
        assert_eq!(webpage.url().domain(), "example.com");
    }

    #[test]
    fn empty_title() {
        let raw = format!(
            r#"
            <html>
                <head>
                    <title></title>
                </head>
                <body>
                    <p>{CONTENT}</p>
                </body>
            </html>
        "#
        );

        let webpage = Html::parse(&raw, "https://www.example.com/whatever");

        assert_eq!(webpage.title(), None);
    }

    #[test]
    fn text_raw_body() {
        let raw = format!(
            r#"
            <html>
                <body>
                    {CONTENT}
                </body>
            </html>
        "#
        );

        let webpage = Html::parse(&raw, "https://www.example.com/whatever");

        assert_eq!(webpage.clean_text(), Some(CONTENT.to_string()));
    }

    #[test]
    fn script_tags_text_ignored() {
        let raw = format!(
            r#"
            <html>
                <head>
                    <title>Best website</title>
                    <meta name="meta1" content="value">
                    <script>this should not be extracted</script>
                </head>
                <body>
                    <script>this should not be extracted</script>
                    <p>{CONTENT}</p>
                    <div>
                        <script>this should not be extracted</script>
                        <p>This text should be the second text extracted</p>
                    </div>
                    <script>this should not be extracted</script>
                </body>
            </html>
        "#
        );

        let webpage = Html::parse(&raw, "https://www.example.com");

        assert!(!webpage.clean_text().unwrap().contains("not"));
    }

    #[test]
    fn style_tags_text_ignored() {
        let raw = format!(
            r#"
            <html>
                <head>
                    <title>Best website</title>
                    <meta name="meta1" content="value">
                    <style>this should not be extracted</style>
                </head>
                <body>
                    <style>this should not be extracted</style>
                    <p>{CONTENT}</p>
                    <div>
                        <style>this should not be extracted</style>
                        <p>This text should be the second text extracted</p>
                    </div>
                    <style>this should not be extracted</style>
                </body>
            </html>
        "#
        );

        let webpage = Html::parse(&raw, "https://www.example.com");

        assert!(!webpage.clean_text().unwrap().contains("not"));
    }

    #[test]
    fn co_uk_domain() {
        let raw = "";

        let webpage = Html::parse(raw, "https://www.domain.co.uk");
        assert_eq!(webpage.url().domain(), "domain.co.uk");
    }

    #[test]
    fn is_homepage() {
        let webpage = Html::parse("", "https://www.example.com");
        assert!(webpage.url().is_homepage());

        let webpage = Html::parse("", "https://www.example.com/");
        assert!(webpage.url().is_homepage());

        let webpage = Html::parse("", "https://www.example.com/test");
        assert!(!webpage.url().is_homepage());

        let webpage = Html::parse("", "https://example.com/test");
        assert!(!webpage.url().is_homepage());

        let webpage = Html::parse("", "https://example.com/");
        assert!(webpage.url().is_homepage());

        let webpage = Html::parse("", "https://example.com");
        assert!(webpage.url().is_homepage());

        let webpage = Html::parse("", "http://example.com");
        assert!(webpage.url().is_homepage());
    }

    #[test]
    fn hard_parsing() {
        let webpage = Html::parse(include_str!("../../testcases/parsing/yasudaya.html"), "");
        assert_eq!(
            webpage.title(),
            Some("パチンコ大当たり情報 - Ｐジューシーハニー３ 大当たり詳細ページ - やすだひばりヶ丘店".to_string())
        );
        assert!(webpage.all_text().is_some());
        assert!(!webpage.all_text().unwrap().is_empty());

        let webpage = Html::parse(include_str!("../../testcases/parsing/5390001.html"), "");
        assert_eq!(
            webpage.title(),
            Some("特效烟机系列_山东壹线文化传播有限公司".to_string())
        );
        assert!(webpage.all_text().is_some());
        assert!(!webpage.all_text().unwrap().is_empty());

        let webpage = Html::parse(
            include_str!("../../testcases/parsing/77p2p-7.live-105.html"),
            "",
        );
        assert_eq!(
            webpage.title(),
            Some("77p2pЅu¤WЖ[¬Э - ҐDјЅ :: іnєс".to_string())
        );
        assert!(webpage.all_text().is_some());
        assert!(!webpage.all_text().unwrap().is_empty());
    }

    #[test]
    fn out_of_bounds_str() {
        let webpage = Html::parse(
            include_str!("../../testcases/parsing/byte_index_out_of_bounds.html"),
            "",
        );
        assert_eq!(webpage.title(), Some("Test".to_string()));
        assert!(webpage.all_text().is_some());
        assert!(!webpage.all_text().unwrap().is_empty());

        let schema = create_schema();
        webpage.into_tantivy(&schema).unwrap();
    }

    #[test]
    fn test_find_protocol() {
        assert_eq!(
            Url::from("https://example.com".to_string()).protocol(),
            "https"
        );
        assert_eq!(
            Url::from("http://example.com".to_string()).protocol(),
            "http"
        );
    }

    #[test]
    fn simple_favicon() {
        let raw = r#"
            <html>
                <head>
                    <link rel="icon" sizes="192x192" href="https://example.com/favicon.png" />
                </head>
            </html>
        "#
        .to_string();

        let webpage = Html::parse(&raw, "https://www.example.com");
        assert_eq!(
            webpage.favicon(),
            Some(FaviconLink {
                link: "https://example.com/favicon.png".to_string().into(),
                width: Some(192),
                height: Some(192),
                image_type: None
            })
        );
    }

    fn full_link_favicon(href: &str, site_url: &str, expected: &str) {
        let raw = format!(
            r#"
            <html>
                <head>
                    <link rel="icon" sizes="192x192" href="{href}" />
                </head>
            </html>
        "#
        );

        let webpage = Html::parse(&raw, site_url);
        assert_eq!(
            webpage.favicon(),
            Some(FaviconLink {
                link: expected.to_string().into(),
                width: Some(192),
                height: Some(192),
                image_type: None
            })
        );
    }

    #[test]
    fn test_full_link_favicon_simple() {
        full_link_favicon(
            "/favicon.png",
            "https://www.example.com/",
            "https://www.example.com/favicon.png",
        );
        full_link_favicon(
            "/favicon.png",
            "https://www.example.com",
            "https://www.example.com/favicon.png",
        );
        full_link_favicon(
            "favicon.png",
            "https://www.example.com",
            "https://www.example.com/favicon.png",
        );
        full_link_favicon(
            "favicon.png",
            "https://www.example.com/",
            "https://www.example.com/favicon.png",
        );
        full_link_favicon(
            "favicon.png",
            "https://www.example.com/test",
            "https://www.example.com/test/favicon.png",
        );
        full_link_favicon(
            "/favicon.png",
            "https://www.example.com/test",
            "https://www.example.com/favicon.png",
        );
    }

    #[test]
    fn domain_from_domain_url() {
        let url: Url = "example.com".to_string().into();
        assert_eq!(url.domain(), "example.com");
    }

    #[test]
    fn schema_dot_org_json_ld() {
        let html = r#"
    <html>
        <head>
            <script type="application/ld+json">
                {
                "@context": "https://schema.org",
                "@type": "ImageObject",
                "author": "Jane Doe",
                "contentLocation": "Puerto Vallarta, Mexico",
                "contentUrl": "mexico-beach.jpg",
                "datePublished": "2008-01-25",
                "description": "I took this picture while on vacation last year.",
                "name": "Beach in Mexico"
                }
            </script>
        </head>
        <body>
        </body>
    </html>
        "#;

        let html = Html::parse(html, "example.com");

        assert_eq!(
            html.schema_org(),
            vec![SchemaOrg::ImageObject(ImageObject {
                name: Some("Beach in Mexico".to_string()),
                description: Some("I took this picture while on vacation last year.".to_string()),
                author: Some("Jane Doe".to_string()),
                content_url: Some("mexico-beach.jpg".to_string()),
            })]
        );
    }

    #[test]
    fn no_schema_dot_org_json_ld() {
        let html = r#"
    <html>
        <head>
            <script>
                {
                "invalid": "schema"
                }
            </script>
        </head>
        <body>
        </body>
    </html>
        "#;

        let html = Html::parse(html, "example.com");

        assert!(html.schema_org().is_empty());

        let html = r#"
    <html>
        <head>
            <script type="application/ld+json">
                {
                "invalid": "schema"
                }
            </script>
        </head>
        <body>
        </body>
    </html>
        "#;

        let html = Html::parse(html, "example.com");

        assert!(html.schema_org().is_empty());
    }

    #[test]
    fn metadata_updated_time() {
        let html = r#"
    <html>
        <head>
            <meta property="og:updated_time" content="2022-06-22T19:37:34+00:00" />
        </head>
        <body>
        </body>
    </html>
        "#;
        let html = Html::parse(html, "example.com");

        assert_eq!(
            html.updated_time(),
            Some(DateTime::parse_from_rfc3339("2022-06-22T19:37:34+00:00").unwrap())
        );

        let html = r#"
    <html>
        <head>
        </head>
        <body>
        </body>
    </html>
        "#;
        let html = Html::parse(html, "example.com");

        assert_eq!(html.updated_time(), None);

        let html = r#"
    <html>
        <head>
            <meta property="og:whutwhut" content="2022-06-22T19:37:34+00:00" />
        </head>
        <body>
        </body>
    </html>
        "#;
        let html = Html::parse(html, "example.com");

        assert_eq!(html.updated_time(), None);

        let html = r#"
    <html>
        <head>
            <meta property="og:updated_time" content="2ss022-06-22T19:37:34+00:00" />
        </head>
        <body>
        </body>
    </html>
        "#;
        let html = Html::parse(html, "example.com");

        assert_eq!(html.updated_time(), None);
    }

    #[test]
    fn primary_image() {
        let html = r#"
    <html>
        <head>
            <meta property="og:image" content="https://example.com/link_to_image.html" />
            <meta property="og:description" content="desc" />
            <meta property="og:title" content="title" />
        </head>
        <body>
        </body>
    </html>
        "#;
        let html = Html::parse(html, "example.com");

        assert_eq!(
            html.primary_image(),
            Some(ImageLink {
                url: "https://example.com/link_to_image.html".to_string().into(),
                title: Some("title".to_string()),
                description: Some("desc".to_string())
            })
        );

        let html = r#"
    <html>
        <head>
        </head>
        <body>
        </body>
    </html>
        "#;
        let html = Html::parse(html, "example.com");

        assert_eq!(html.primary_image(), None);

        let html = r#"
    <html>
        <head>
            <script type="application/ld+json">
                {
                "@context": "https://schema.org",
                "@type": "ImageObject",
                "author": "Jane Doe",
                "contentLocation": "Puerto Vallarta, Mexico",
                "contentUrl": "mexico-beach.jpg",
                "datePublished": "2008-01-25",
                "description": "I took this picture while on vacation last year.",
                "name": "Beach in Mexico"
                }
            </script>
        </head>
        <body>
        </body>
    </html>
        "#;
        let html = Html::parse(html, "https://example.com");

        assert_eq!(
            html.primary_image(),
            Some(ImageLink {
                url: "https://example.com/mexico-beach.jpg".to_string().into(),
                title: None,
                description: None
            })
        );
    }

    #[test]
    fn description() {
        let html = r#"
    <html>
        <head>
            <meta property="og:description" content="This is a page description" />
        </head>
        <body>
        </body>
    </html>
        "#;
        let html = Html::parse(html, "example.com");

        assert_eq!(
            html.description(),
            Some("This is a page description".to_string())
        );

        let html = r#"
    <html>
        <head>
        </head>
        <body>
        </body>
    </html>
        "#;
        let html = Html::parse(html, "example.com");

        assert_eq!(html.description(), None);
    }

    #[test]
    fn article_modified_time() {
        let html = r#"
    <html>
        <head>
            <meta property="article:modified_time" content="2022-06-22T19:37:34+00:00" />
        </head>
        <body>
        </body>
    </html>
        "#;
        let html = Html::parse(html, "example.com");

        assert_eq!(
            html.updated_time(),
            Some(DateTime::parse_from_rfc3339("2022-06-22T19:37:34+00:00").unwrap())
        );
    }

    #[test]
    fn trackers() {
        let html = r#"
            <html>
                <head>
                    <script>
                        !function(){var analytics=window.analytics=window.analytics||[];if(!analytics.initialize)if(analytics.invoked)window.console&&console.error&&console.error("Segment snippet included twice.");else{analytics.invoked=!0;analytics.methods=["trackSubmit","trackClick","trackLink","trackForm","pageview","identify","reset","group","track","ready","alias","debug","page","once","off","on","addSourceMiddleware","addIntegrationMiddleware","setAnonymousId","addDestinationMiddleware"];analytics.factory=function(e){return function(){var t=Array.prototype.slice.call(arguments);t.unshift(e);analytics.push(t);return analytics}};for(var e=0;e<analytics.methods.length;e++){var key=analytics.methods[e];analytics[key]=analytics.factory(key)}analytics.load=function(key,e){var t=document.createElement("script");t.type="text/javascript";t.async=!0;t.src="https://cdn.segment.com/analytics.js/v1/" + key + "/analytics.min.js";var n=document.getElementsByTagName("script")[0];n.parentNode.insertBefore(t,n);analytics._loadOptions=e};analytics._writeKey="";analytics.SNIPPET_VERSION="4.13.2";
                        analytics.load("");
                        analytics.page();
                        }}();
                    </script>
                    <script>
                        (function(h,o,t,j,a,r){
                            h.hj=h.hj||function(){(h.hj.q=h.hj.q||[]).push(arguments)};
                            a.appendChild(r);
                        })(window,document,'https://static.hotjar.com/c/hotjar-','.js?sv=');
                    </script>
                    <script src="https://thirdparty.com/js"></script>
                    <script src="https://example.com/js"></script>
                    <link href='//securepubads.g.doubleclick.net' rel='preconnect'>
                    <script src="https://thirdparty.com/js"></script>
                    <script src="/js/file"></script>
                </head>
                <body>
                </body>
            </html>
        "#;
        let html = Html::parse(html, "example.com");

        assert_eq!(
            html.trackers()
                .into_iter()
                .map(|url| url.site().to_string())
                .collect::<Vec<_>>(),
            vec![
                "cdn.segment.com".to_string(),
                "static.hotjar.com".to_string(),
                "thirdparty.com".to_string(),
                "securepubads.g.doubleclick.net".to_string()
            ]
        );
    }

    #[test]
    fn parse_title_with_scripts() {
        let html = Html::parse(
            r#"
                    <html>
                        <head>
                            <script>
                                !function(){var analytics=window.analytics=window.analytics||[];if(!analytics.initialize)if(analytics.invoked)window.console&&console.error&&console.error("Segment snippet included twice.");else{analytics.invoked=!0;analytics.methods=["trackSubmit","trackClick","trackLink","trackForm","pageview","identify","reset","group","track","ready","alias","debug","page","once","off","on","addSourceMiddleware","addIntegrationMiddleware","setAnonymousId","addDestinationMiddleware"];analytics.factory=function(e){return function(){var t=Array.prototype.slice.call(arguments);t.unshift(e);analytics.push(t);return analytics}};for(var e=0;e<analytics.methods.length;e++){var key=analytics.methods[e];analytics[key]=analytics.factory(key)}analytics.load=function(key,e){var t=document.createElement("script");t.type="text/javascript";t.async=!0;t.src="https://cdn.segment.com/analytics.js/v1/" + key + "/analytics.min.js";var n=document.getElementsByTagName("script")[0];n.parentNode.insertBefore(t,n);analytics._loadOptions=e};analytics._writeKey="";analytics.SNIPPET_VERSION="4.13.2";
                                analytics.load("");
                                analytics.page();
                                }}();
                            </script>
                            <script>
                                (function(h,o,t,j,a,r){
                                    h.hj=h.hj||function(){(h.hj.q=h.hj.q||[]).push(arguments)};
                                    a.appendChild(r);
                                })(window,document,'https://static.hotjar.com/c/hotjar-','.js?sv=');
                            </script>
                            <script src="https://thirdparty.com/js"></script>
                            <link href='//securepubads.g.doubleclick.net' rel='preconnect'>
                            <title>Test site</title>
                        </head>
                        <body>
                            test
                        </body>
                    </html>
                "#,
            "example.com",
        );

        assert_eq!(html.title(), Some("Test site".to_string()));
        assert_eq!(html.all_text(), Some("test".to_string()));
    }
}
