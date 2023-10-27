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
use chrono::{DateTime, FixedOffset, Utc};
use itertools::Itertools;
use kuchiki::{iter::NodeEdge, traits::TendrilSink, NodeRef};
use regex::Regex;
use schema::{FastField, Field, TextField, ALL_FIELDS, FLOAT_SCALING};
use std::{collections::HashMap, panic, str::FromStr};
use stdx::{enum_map::EnumSet, prehashed::hash};
use tantivy::{
    tokenizer::{PreTokenizedString, Tokenizer},
    TantivyDocument,
};
use url::Url;
use webgraph::NodeID;
use whatlang::Lang;

mod just_text;
pub mod region;
pub mod safety_classifier;
pub mod schema_org;
pub mod url_ext;

use self::{
    just_text::{JustText, Paragraph},
    region::Region,
    url_ext::UrlExt,
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Encountered an empty required field ({0}) when converting to tantivy")]
    EmptyField(&'static str),

    // #[error("Parsing error")]
    // ParsingError(String),

    // #[error("Failed to download warc files after all retries")]
    // DownloadFailed,

    // #[error("Query cannot be completely empty")]
    // EmptyQuery,
    #[error("Unknown region")]
    UnknownRegion,

    // #[error("Unknown CLI option")]
    // UnknownCLIOption,

    // #[error("The stackoverflow schema was not structured as expected")]
    // InvalidStackoverflowSchema,

    // #[error("Internal error")]
    // InternalError(String),
    #[error("IO error")]
    Io(#[from] std::io::Error),

    #[error("JSON error")]
    Json(#[from] serde_json::Error),

    #[error("URL parse error")]
    Url(#[from] url::ParseError),

    #[error("CSV error")]
    Csv(#[from] csv::Error),

    #[error("Bincode error")]
    Bincode(#[from] bincode::Error),

    #[error("Unknown webpage robots meta tag")]
    UnknownRobotsMetaTag,

    #[error("Unknown microformat")]
    UnknownMicroformat,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub static URL_REGEX: once_cell::sync::Lazy<Regex> = once_cell::sync::Lazy::new(|| {
    Regex::new(r"(((http|ftp|https):/{2})+(([0-9a-z_-]+\.)+(aero|asia|biz|cat|com|coop|edu|gov|info|int|jobs|mil|mobi|museum|name|net|org|pro|tel|travel|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cu|cv|cx|cy|cz|cz|de|dj|dk|dm|do|dz|ec|ee|eg|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mn|mn|mo|mp|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|nom|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ra|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|sj|sk|sl|sm|sn|so|sr|st|su|sv|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw|arpa)(:[0-9]+)?((/([~0-9a-zA-Z\#\+%@\./_-]+))?(\?[0-9a-zA-Z\+%@/&\[\];=_-]+)?)?))\b").unwrap()
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
                    let element_name: &str = &element.name.local;
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
                    let element_name: &str = &element.name.local;
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

#[derive(Debug)]
pub struct Webpage {
    pub html: Html,
    pub backlink_labels: Vec<String>,
    pub host_centrality: f64,
    pub page_centrality: f64,
    pub fetch_time_ms: u64,
    pub pre_computed_score: f64,
    pub node_id: Option<NodeID>,
    pub dmoz_description: Option<String>,
    pub safety_classification: Option<safety_classifier::Label>,
}

impl Webpage {
    // TODO: I needed to make this for all targets due to #[cfg(test)] not being exported
    // #[cfg(test)]
    pub fn new(html: &str, url: &str) -> Result<Self> {
        let html = Html::parse(html, url)?;

        Ok(Self {
            html,
            backlink_labels: Vec::new(),
            host_centrality: 0.0,
            page_centrality: 0.0,
            fetch_time_ms: 0,
            pre_computed_score: 0.0,
            node_id: None,
            dmoz_description: None,
            safety_classification: None,
        })
    }

    fn dmoz_description(&self) -> Option<String> {
        self.dmoz_description.as_ref().and_then(|desc| {
            if !self.html.metadata().iter().any(|metadata| {
                if let Some(content) = metadata.get(&"content".to_string()) {
                    content.contains("noodp")
                } else {
                    false
                }
            }) {
                Some(desc.clone())
            } else {
                None
            }
        })
    }

    pub fn into_tantivy(self, schema: &tantivy::schema::Schema) -> Result<TantivyDocument> {
        let region = Region::guess_from(&self);

        let dmoz_description = self.dmoz_description();

        let mut doc = self.html.into_tantivy(schema)?;

        if let Ok(region) = region {
            doc.add_u64(
                schema
                    .get_field(Field::Fast(FastField::Region).name())
                    .expect("Failed to get region field"),
                region.id(),
            );
        } else {
            doc.add_u64(
                schema
                    .get_field(Field::Fast(FastField::Region).name())
                    .expect("Failed to get region field"),
                Region::All.id(),
            );
        }

        let backlink_text: String =
            itertools::intersperse(self.backlink_labels, "\n".to_string()).collect();

        doc.add_text(
            schema
                .get_field(Field::Text(TextField::BacklinkText).name())
                .expect("Failed to get backlink-text field"),
            backlink_text,
        );

        let safety = self
            .safety_classification
            .map(|label| label.to_string())
            .unwrap_or_default();

        doc.add_text(
            schema
                .get_field(Field::Text(TextField::SafetyClassification).name())
                .expect("Failed to get safety_classification field"),
            safety,
        );

        doc.add_u64(
            schema
                .get_field(Field::Fast(FastField::HostCentrality).name())
                .expect("Failed to get host_centrality field"),
            (self.host_centrality * FLOAT_SCALING as f64) as u64,
        );

        doc.add_u64(
            schema
                .get_field(Field::Fast(FastField::PageCentrality).name())
                .expect("Failed to get page_centrality field"),
            (self.page_centrality * FLOAT_SCALING as f64) as u64,
        );

        doc.add_u64(
            schema
                .get_field(Field::Fast(FastField::FetchTimeMs).name())
                .expect("Failed to get fetch_time_ms field"),
            self.fetch_time_ms,
        );

        doc.add_u64(
            schema
                .get_field(Field::Fast(FastField::PreComputedScore).name())
                .expect("failed to get pre_computed_score field"),
            (self.pre_computed_score * FLOAT_SCALING as f64) as u64,
        );

        match &self.node_id {
            Some(node_id) => {
                let [node_id1, node_id2] = stdx::split_u128(node_id.bit_128());
                doc.add_u64(
                    schema
                        .get_field(Field::Fast(FastField::HostNodeID1).name())
                        .expect("Failed to get node_id field 1"),
                    node_id1,
                );
                doc.add_u64(
                    schema
                        .get_field(Field::Fast(FastField::HostNodeID2).name())
                        .expect("Failed to get node_id field 2"),
                    node_id2,
                );
            }
            None => {
                doc.add_u64(
                    schema
                        .get_field(Field::Fast(FastField::HostNodeID1).name())
                        .expect("Failed to get node_id field 1"),
                    u64::MAX,
                );
                doc.add_u64(
                    schema
                        .get_field(Field::Fast(FastField::HostNodeID2).name())
                        .expect("Failed to get node_id field 2"),
                    u64::MAX,
                );
            }
        }

        doc.add_text(
            schema
                .get_field(Field::Text(TextField::DmozDescription).name())
                .expect("failed to get dmoz_description field"),
            dmoz_description.unwrap_or_default(),
        );

        Ok(doc)
    }
}

struct Script {
    attributes: HashMap<String, String>,
    content: String,
}

#[derive(Debug)]
enum RobotsMeta {
    NoIndex,
    NoFollow,
}

impl FromStr for RobotsMeta {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "noindex" => Ok(RobotsMeta::NoIndex),
            "nofollow" => Ok(RobotsMeta::NoFollow),
            _ => Err(Error::UnknownRobotsMetaTag),
        }
    }
}

impl From<RobotsMeta> for usize {
    fn from(val: RobotsMeta) -> Self {
        match val {
            RobotsMeta::NoIndex => 0,
            RobotsMeta::NoFollow => 1,
        }
    }
}

#[allow(clippy::enum_variant_names)]
enum Microformat {
    HCard,
    HEvent,
    HEntry,
    HRecipe,
    HReview,
    HProduct,
}

impl Microformat {
    fn as_str(&self) -> &str {
        match self {
            Microformat::HCard => "h-card",
            Microformat::HEvent => "h-event",
            Microformat::HEntry => "h-entry",
            Microformat::HRecipe => "h-recipe",
            Microformat::HReview => "h-review",
            Microformat::HProduct => "h-product",
        }
    }
}

impl From<Microformat> for usize {
    fn from(value: Microformat) -> Self {
        match value {
            Microformat::HCard => 0,
            Microformat::HEvent => 1,
            Microformat::HEntry => 2,
            Microformat::HRecipe => 3,
            Microformat::HReview => 4,
            Microformat::HProduct => 5,
        }
    }
}

impl TryFrom<usize> for Microformat {
    type Error = Error;

    fn try_from(value: usize) -> Result<Self> {
        match value {
            0 => Ok(Microformat::HCard),
            1 => Ok(Microformat::HEvent),
            2 => Ok(Microformat::HEntry),
            3 => Ok(Microformat::HRecipe),
            4 => Ok(Microformat::HReview),
            5 => Ok(Microformat::HProduct),
            _ => Err(Error::UnknownMicroformat),
        }
    }
}

const ALL_MICROFORMATS: [Microformat; 6] = [
    Microformat::HCard,
    Microformat::HEvent,
    Microformat::HEntry,
    Microformat::HRecipe,
    Microformat::HReview,
    Microformat::HProduct,
];

#[derive(Debug)]
pub struct Html {
    url: Url,
    root: NodeRef, // this is reference counted (cheap to clone)
    all_text: Option<String>,
    clean_text: Option<String>,
    lang: Option<Lang>,
    robots: Option<EnumSet<RobotsMeta>>,
}

impl Html {
    pub fn parse(html: &str, url: &str) -> Result<Self> {
        let mut html = Self::parse_without_text(html, url)?;

        html.parse_text();

        Ok(html)
    }

    // TODO: I needed to make this for all targets due to #[cfg(test)] not being exported
    // #[cfg(test)]
    pub fn set_clean_text(&mut self, text: String) {
        self.clean_text = Some(text);
    }

    pub fn parse_without_text(html: &str, url: &str) -> Result<Self> {
        let root = kuchiki::parse_html().one(html);

        let mut url = Url::parse(url)?;
        url.set_fragment(None); // remove fragment (e.g. #comments

        let mut res = Self {
            root,
            all_text: None,
            clean_text: None,
            lang: None,
            url,
            robots: None,
        };

        let queries: Vec<_> = res
            .url
            .query_pairs()
            .filter(|(key, _)| !key.starts_with("utm_"))
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();

        {
            let mut query_mut = res.url.query_pairs_mut();
            query_mut.clear();
            if !queries.is_empty() {
                query_mut.extend_pairs(queries);
            }
        }

        if res.url.query().unwrap_or_default().is_empty() {
            res.url.set_query(None);
        }

        res.robots = res.parse_robots_meta();

        Ok(res)
    }

    pub fn canonical_url(&self) -> Option<Url> {
        let mut canonical_url = None;

        for node in self.root.select("link").unwrap() {
            if let Some(element) = node.as_node().as_element() {
                if let Some(rel) = element.attributes.borrow().get("rel") {
                    if rel == "canonical" {
                        if let Some(href) = element.attributes.borrow().get("href") {
                            match Url::parse(href) {
                                Ok(url) => canonical_url = Some(url),
                                Err(_) => {
                                    if let Ok(url) = self.url().join(href) {
                                        canonical_url = Some(url);
                                    }
                                }
                            };
                        }
                    }
                }
            }
        }

        canonical_url
    }

    fn parse_robots_meta(&self) -> Option<EnumSet<RobotsMeta>> {
        let mut robots = EnumSet::new();

        for node in self.root.select("meta").unwrap() {
            if let Some(element) = node.as_node().as_element() {
                if let Some(name) = element.attributes.borrow().get("name") {
                    if name == "robots" {
                        if let Some(content) = element.attributes.borrow().get("content") {
                            for part in content.split(',') {
                                let part = part.trim();
                                if let Ok(meta) = part.parse::<RobotsMeta>() {
                                    robots.insert(meta);
                                }
                            }
                        }
                    }
                }
            }
        }

        if robots.is_empty() {
            None
        } else {
            Some(robots)
        }
    }

    pub fn is_no_index(&self) -> bool {
        self.robots
            .as_ref()
            .map(|robots| robots.contains(RobotsMeta::NoIndex))
            .unwrap_or(false)
    }

    pub fn is_no_follow(&self) -> bool {
        self.robots
            .as_ref()
            .map(|robots| robots.contains(RobotsMeta::NoFollow))
            .unwrap_or(false)
    }

    pub fn parse_text(&mut self) {
        let paragraphs = JustText::paragraphs(self.root.clone());

        self.lang = paragraphs
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

        self.all_text = Html::calculate_all_text(&paragraphs, &self.lang.unwrap_or(Lang::Eng));
        self.clean_text = Html::calculate_clean_text(&paragraphs, &self.lang.unwrap_or(Lang::Eng));
    }

    pub fn anchor_links(&self) -> Vec<Link> {
        if self.is_no_follow() {
            return Vec::new();
        }

        let mut links = Vec::new();
        let mut open_links = Vec::new();

        for edge in self.root.traverse() {
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
                                    if dest.starts_with("mailto:") || dest.starts_with("tel:") {
                                        continue;
                                    }

                                    if let Ok(dest) =
                                        Url::parse(dest).or_else(|_| self.url().join(dest))
                                    {
                                        links.push(Link {
                                            source: self.url().clone(),
                                            destination: dest,
                                            text: text.trim().to_string(),
                                        });
                                    }
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

        while let Some((text, attributes)) = open_links.pop() {
            if let Some(rel) = attributes.borrow().get("rel") {
                if rel.contains("nofollow") || rel.contains("sponsored") || rel.contains("ugc") {
                    continue;
                }
            }

            if let Some(dest) = attributes.borrow().get("href") {
                if dest.starts_with("mailto:") || dest.starts_with("tel:") {
                    continue;
                }

                if let Ok(dest) = Url::parse(dest).or_else(|_| self.url().join(dest)) {
                    links.push(Link {
                        source: self.url().clone(),
                        destination: dest,
                        text: text.trim().to_string(),
                    });
                }
            }
        }

        links
    }

    fn links_tag(&self) -> Vec<Link> {
        let mut links = Vec::new();

        for node in self.root.select("link").unwrap() {
            if let Some(element) = node.as_node().as_element() {
                if let Some(href) = element.attributes.borrow().get("href") {
                    if let Ok(href) = Url::parse(href).or_else(|_| self.url().join(href)) {
                        links.push(Link {
                            source: self.url().clone(),
                            destination: href,
                            text: String::new(),
                        });
                    }
                }
            }
        }

        links
    }

    fn metadata_links(&self) -> Vec<Link> {
        self.metadata()
            .into_iter()
            .filter_map(|metadata| {
                // https://github.com/commoncrawl/cc-pyspark/blob/54918e85cf87d47e1f7278965ac04a0fc8e414a0/wat_extract_links.py#L54

                if let Some(prop) = metadata.get("property") {
                    if matches!(
                        prop.as_str(),
                        "og:url"
                            | "og:image"
                            | "og:image:secure_url"
                            | "og:video"
                            | "og:video:url"
                            | "og:video:secure_url"
                            | "twitter:url"
                            | "twitter:image:src"
                    ) {
                        if let Some(content) = metadata.get("content") {
                            if let Ok(destination) = Url::parse(content.as_str())
                                .or_else(|_| self.url().join(content.as_str()))
                            {
                                return Some(Link {
                                    source: self.url().clone(),
                                    destination,
                                    text: String::new(),
                                });
                            }
                        }
                    }
                }

                if let Some(name) = metadata.get("name") {
                    if matches!(
                        name.as_str(),
                        "twitter:image"
                            | "thumbnail"
                            | "application-url"
                            | "msapplication-starturl"
                            | "msapplication-TileImage"
                            | "vb_meta_bburl"
                    ) {
                        if let Some(content) = metadata.get("content") {
                            if let Ok(destination) = Url::parse(content.as_str())
                                .or_else(|_| self.url().join(content.as_str()))
                            {
                                return Some(Link {
                                    source: self.url().clone(),
                                    destination,
                                    text: String::new(),
                                });
                            }
                        }
                    }
                }

                None
            })
            .collect()
    }

    pub fn all_links(&self) -> Vec<Link> {
        let mut links = self.anchor_links();

        links.extend(self.scripts().into_iter().filter_map(|script| {
            match script.attributes.get("src") {
                Some(url) => {
                    let script_url = Url::parse(url.as_str())
                        .or_else(|_| self.url().join(url.as_str()))
                        .ok()?;

                    if script_url.root_domain() != self.url().root_domain() {
                        Some(Link {
                            source: self.url().clone(),
                            destination: script_url,
                            text: String::new(),
                        })
                    } else {
                        None
                    }
                }
                None => None,
            }
        }));

        links.extend(self.links_tag());
        links.extend(self.metadata_links());

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
                let link = Url::parse(link).or_else(|_| self.url().join(link)).ok()?;

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

    pub fn clean_text(&self) -> Option<&String> {
        self.clean_text.as_ref()
    }

    fn calculate_all_text(paragraphs: &[Paragraph], lang: &Lang) -> Option<String> {
        let text = JustText {
            max_link_density: 20.0,
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

    pub fn empty_all_text(&self) -> bool {
        match &self.all_text {
            Some(text) => text.is_empty(),
            None => true,
        }
    }

    pub fn title(&self) -> Option<String> {
        if let Some(title) = self.root.select_first("title") {
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

    fn microformats(&self) -> EnumSet<Microformat> {
        let mut microformats = EnumSet::new();

        for node in self.root.inclusive_descendants() {
            if let Some(element) = node.as_element() {
                if let Some(class) = element.attributes.borrow().get("class") {
                    for microformat in ALL_MICROFORMATS {
                        if class.to_lowercase().as_str() == microformat.as_str() {
                            microformats.insert(microformat);
                        }
                    }
                }
            }
        }

        microformats
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
                        .map(|(name, attr)| (name.local.to_string(), attr.value.to_string()))
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
        let clean_text = self.clean_text().cloned().unwrap_or_default();
        self.pretokenize_string(clean_text)
    }

    fn pretokenize_url(&self) -> PreTokenizedString {
        let url = self.url().to_string();
        self.pretokenize_string(url)
    }

    fn pretokenize_domain(&self) -> PreTokenizedString {
        let mut domain = self.url().root_domain().unwrap_or_default().to_string();

        if let Some(stripped) = domain.strip_prefix("www.") {
            domain = stripped.to_string();
        }

        self.pretokenize_string(domain)
    }

    fn pretokenize_site(&self) -> PreTokenizedString {
        let site = self.url().host_str().unwrap_or_default().to_string();

        self.pretokenize_string(site)
    }

    fn pretokenize_description(&self) -> PreTokenizedString {
        let text = self.description().unwrap_or_default();

        self.pretokenize_string(text)
    }

    fn pretokenize_microformats(&self) -> PreTokenizedString {
        let mut text = String::new();

        for microformat in self.microformats().iter() {
            text.push_str(microformat.as_str());
            text.push(' ');
        }

        self.pretokenize_string(text)
    }

    fn pretokenize_string(&self, text: String) -> PreTokenizedString {
        self.pretokenize_string_with(text, tokenizer::Tokenizer::default())
    }

    fn pretokenize_string_with(
        &self,
        text: String,
        tokenizer: tokenizer::Tokenizer,
    ) -> PreTokenizedString {
        let mut tokenizer = tokenizer;

        let mut tokens = Vec::new();

        {
            let mut stream = tokenizer.token_stream(&text);
            while let Some(token) = stream.next() {
                tokens.push(token.clone());
            }
        }

        PreTokenizedString { text, tokens }
    }

    pub fn into_tantivy(self, schema: &tantivy::schema::Schema) -> Result<TantivyDocument> {
        let mut doc = TantivyDocument::new();

        let title = self.pretokenize_title()?;
        let all_text = self.pretokenize_all_text()?;
        let clean_text = self.pretokenize_clean_text();
        let url = self.pretokenize_url();
        let domain = self.pretokenize_domain();
        let site = self.pretokenize_site();
        let description = self.pretokenize_description();
        let microformats = self.pretokenize_microformats();
        let url_for_site_operator = self.pretokenize_string_with(
            self.url().to_string(),
            tokenizer::Tokenizer::SiteOperator(tokenizer::SiteOperatorUrlTokenizer),
        );

        let domain_name = self
            .url()
            .root_domain()
            .unwrap_or_default()
            .find('.')
            .map(|index| {
                &domain.text[..stdx::ceil_char_boundary(&domain.text, index).min(domain.text.len())]
            })
            .unwrap_or_default()
            .to_string();

        let schemas: Vec<_> = self.schema_org();

        let schema_json = serde_json::to_string(&schemas).ok().unwrap_or_default();

        let pretokenized_schema_json = match schema_org::flattened_json(schemas) {
            Ok(mut f) => {
                let mut tokens = Vec::new();

                {
                    let mut stream = f.token_stream();

                    while let Some(token) = stream.next() {
                        tokens.push(token.clone());
                    }
                }

                PreTokenizedString {
                    text: f.text().to_string(),
                    tokens,
                }
            }
            Err(_) => PreTokenizedString {
                text: String::new(),
                tokens: Vec::new(),
            },
        };

        let site_hash = stdx::split_u128(hash(self.url().host_str().unwrap_or_default()).0);

        let mut url_without_query = self.url().clone();
        url_without_query.set_query(None);

        let url_without_query_hash = stdx::split_u128(hash(url_without_query.as_str()).0);
        let url_hash = stdx::split_u128(hash(self.url().as_str()).0);

        let domain_hash = stdx::split_u128(hash(self.url().root_domain().unwrap_or_default()).0);
        let title_hash = stdx::split_u128(hash(self.title().unwrap_or_default()).0);

        for field in &ALL_FIELDS {
            let tantivy_field = schema
                .get_field(field.name())
                .unwrap_or_else(|_| panic!("Unknown field: {}", field.name()));

            match field {
                Field::Text(TextField::Title) => {
                    doc.add_pre_tokenized_text(tantivy_field, title.clone())
                }
                Field::Text(TextField::StemmedTitle) => {
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
                Field::Text(TextField::CleanBody) => {
                    doc.add_pre_tokenized_text(tantivy_field, clean_text.clone())
                }
                Field::Text(TextField::StemmedCleanBody) => {
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
                Field::Text(TextField::CleanBodyBigrams) => {
                    doc.add_text(
                        tantivy_field,
                        self.clean_text().cloned().unwrap_or_default(),
                    );
                }
                Field::Text(TextField::CleanBodyTrigrams) => {
                    doc.add_text(
                        tantivy_field,
                        self.clean_text().cloned().unwrap_or_default(),
                    );
                }
                Field::Text(TextField::TitleBigrams) => {
                    doc.add_text(tantivy_field, title.text.clone());
                }
                Field::Text(TextField::TitleTrigrams) => {
                    doc.add_text(tantivy_field, title.text.clone());
                }
                Field::Text(TextField::Description) => {
                    doc.add_pre_tokenized_text(tantivy_field, description.clone());
                }
                Field::Text(TextField::Url) => {
                    doc.add_pre_tokenized_text(tantivy_field, url.clone())
                }
                Field::Text(TextField::UrlForSiteOperator) => {
                    doc.add_pre_tokenized_text(tantivy_field, url_for_site_operator.clone())
                }
                Field::Text(TextField::UrlNoTokenizer) => {
                    let url = self.url().to_string();

                    doc.add_pre_tokenized_text(
                        tantivy_field,
                        PreTokenizedString {
                            text: url.clone(),
                            tokens: vec![tantivy::tokenizer::Token {
                                offset_from: 0,
                                offset_to: url.len(),
                                position: 0,
                                text: url,
                                position_length: 1,
                            }],
                        },
                    );
                }
                Field::Text(TextField::SiteWithout) => {
                    doc.add_pre_tokenized_text(tantivy_field, site.clone())
                }
                Field::Text(TextField::Domain) => {
                    doc.add_pre_tokenized_text(tantivy_field, domain.clone())
                }
                Field::Text(TextField::SiteNoTokenizer) => doc.add_pre_tokenized_text(
                    tantivy_field,
                    PreTokenizedString {
                        text: site.text.clone(),
                        tokens: vec![tantivy::tokenizer::Token {
                            offset_from: 0,
                            offset_to: site.text.len(),
                            position: 0,
                            text: site.text.clone(),
                            position_length: 1,
                        }],
                    },
                ),
                Field::Text(TextField::SiteIfHomepageNoTokenizer) => {
                    if self.is_homepage() {
                        doc.add_pre_tokenized_text(
                            tantivy_field,
                            PreTokenizedString {
                                text: site.text.clone(),
                                tokens: vec![tantivy::tokenizer::Token {
                                    offset_from: 0,
                                    offset_to: site.text.len(),
                                    position: 0,
                                    text: site.text.clone(),
                                    position_length: 1,
                                }],
                            },
                        )
                    } else {
                        doc.add_text(tantivy_field, "");
                    }
                }
                Field::Text(TextField::DomainNoTokenizer) => doc.add_pre_tokenized_text(
                    tantivy_field,
                    PreTokenizedString {
                        text: domain.text.clone(),
                        tokens: vec![tantivy::tokenizer::Token {
                            offset_from: 0,
                            offset_to: domain.text.len(),
                            position: 0,
                            text: domain.text.clone(),
                            position_length: 1,
                        }],
                    },
                ),
                Field::Text(TextField::TitleIfHomepage) => {
                    if self.is_homepage() {
                        doc.add_pre_tokenized_text(tantivy_field, title.clone());
                    } else {
                        doc.add_text(tantivy_field, "");
                    }
                }
                Field::Text(TextField::DomainIfHomepage) => {
                    if self.is_homepage() {
                        doc.add_text(tantivy_field, domain.text.clone());
                    } else {
                        doc.add_text(tantivy_field, "");
                    }
                }
                Field::Text(TextField::DomainNameNoTokenizer) => {
                    doc.add_pre_tokenized_text(
                        tantivy_field,
                        PreTokenizedString {
                            text: domain_name.to_string(),
                            tokens: vec![tantivy::tokenizer::Token {
                                offset_from: 0,
                                offset_to: domain_name.len(),
                                position: 0,
                                text: domain_name.to_string(),
                                position_length: 1,
                            }],
                        },
                    );
                }
                Field::Text(TextField::DomainNameIfHomepageNoTokenizer) => {
                    if self.is_homepage() {
                        doc.add_pre_tokenized_text(
                            tantivy_field,
                            PreTokenizedString {
                                text: domain_name.to_string(),
                                tokens: vec![tantivy::tokenizer::Token {
                                    offset_from: 0,
                                    offset_to: domain_name.len(),
                                    position: 0,
                                    text: domain_name.to_string(),
                                    position_length: 1,
                                }],
                            },
                        );
                    } else {
                        doc.add_text(tantivy_field, "");
                    }
                }
                Field::Text(TextField::DomainIfHomepageNoTokenizer) => {
                    if self.is_homepage() {
                        doc.add_pre_tokenized_text(tantivy_field, domain.clone());
                    } else {
                        doc.add_text(tantivy_field, "");
                    }
                }
                Field::Text(TextField::AllBody) => {
                    doc.add_pre_tokenized_text(tantivy_field, all_text.clone())
                }
                Field::Text(TextField::SchemaOrgJson) => {
                    doc.add_text(tantivy_field, schema_json.clone());
                }
                Field::Text(TextField::FlattenedSchemaOrgJson) => {
                    doc.add_pre_tokenized_text(tantivy_field, pretokenized_schema_json.clone());
                }
                Field::Text(TextField::MicroformatTags) => {
                    doc.add_pre_tokenized_text(tantivy_field, microformats.clone());
                }
                Field::Fast(FastField::IsHomepage) => {
                    doc.add_u64(tantivy_field, (self.is_homepage()).into());
                }
                Field::Fast(FastField::LastUpdated) => doc.add_u64(
                    tantivy_field,
                    self.updated_time()
                        .map_or(0, |time| time.timestamp().max(0) as u64),
                ),
                Field::Fast(FastField::TrackerScore) => {
                    doc.add_u64(tantivy_field, self.trackers().len() as u64)
                }
                Field::Fast(FastField::NumUrlTokens) => {
                    doc.add_u64(tantivy_field, url.tokens.len() as u64)
                }
                Field::Fast(FastField::NumMicroformatTagsTokens) => {
                    doc.add_u64(tantivy_field, microformats.tokens.len() as u64)
                }
                Field::Fast(FastField::NumTitleTokens) => {
                    doc.add_u64(tantivy_field, title.tokens.len() as u64)
                }
                Field::Fast(FastField::NumCleanBodyTokens) => {
                    doc.add_u64(tantivy_field, clean_text.tokens.len() as u64)
                }
                Field::Fast(FastField::NumDescriptionTokens) => {
                    doc.add_u64(tantivy_field, description.tokens.len() as u64)
                }
                Field::Fast(FastField::NumUrlForSiteOperatorTokens) => {
                    doc.add_u64(tantivy_field, url_for_site_operator.tokens.len() as u64)
                }
                Field::Fast(FastField::NumDomainTokens) => {
                    doc.add_u64(tantivy_field, domain.tokens.len() as u64)
                }
                Field::Fast(FastField::NumFlattenedSchemaTokens) => {
                    doc.add_u64(tantivy_field, pretokenized_schema_json.tokens.len() as u64)
                }
                Field::Fast(FastField::SiteHash1) => {
                    doc.add_u64(tantivy_field, site_hash[0]);
                }
                Field::Fast(FastField::SiteHash2) => {
                    doc.add_u64(tantivy_field, site_hash[1]);
                }
                Field::Fast(FastField::UrlWithoutQueryHash1) => {
                    doc.add_u64(tantivy_field, url_without_query_hash[0]);
                }
                Field::Fast(FastField::UrlWithoutQueryHash2) => {
                    doc.add_u64(tantivy_field, url_without_query_hash[1]);
                }
                Field::Fast(FastField::UrlHash1) => {
                    doc.add_u64(tantivy_field, url_hash[0]);
                }
                Field::Fast(FastField::UrlHash2) => {
                    doc.add_u64(tantivy_field, url_hash[1]);
                }
                Field::Fast(FastField::DomainHash1) => {
                    doc.add_u64(tantivy_field, domain_hash[0]);
                }
                Field::Fast(FastField::DomainHash2) => {
                    doc.add_u64(tantivy_field, domain_hash[1]);
                }
                Field::Fast(FastField::TitleHash1) => {
                    doc.add_u64(tantivy_field, title_hash[0]);
                }
                Field::Fast(FastField::TitleHash2) => {
                    doc.add_u64(tantivy_field, title_hash[1]);
                }
                Field::Fast(FastField::SimHash) => {
                    let hash = if !clean_text.text.is_empty() {
                        simhash::hash(&clean_text.text)
                    } else {
                        0
                    };
                    doc.add_u64(tantivy_field, hash);
                }
                Field::Fast(FastField::NumPathAndQuerySlashes) => {
                    let num_slashes = self
                        .url()
                        .path_segments()
                        .map(|segments| segments.count())
                        .unwrap_or(0);

                    doc.add_u64(tantivy_field, num_slashes as u64);
                }
                Field::Fast(FastField::NumPathAndQueryDigits) => {
                    let num_digits = self
                        .url()
                        .path()
                        .chars()
                        .filter(|c| c.is_ascii_digit())
                        .count()
                        + self
                            .url()
                            .query()
                            .unwrap_or_default()
                            .chars()
                            .filter(|c| c.is_ascii_digit())
                            .count();

                    doc.add_u64(tantivy_field, num_digits as u64);
                }
                Field::Text(TextField::BacklinkText)
                | Field::Text(TextField::SafetyClassification)
                | Field::Fast(FastField::HostCentrality)
                | Field::Fast(FastField::PageCentrality)
                | Field::Fast(FastField::FetchTimeMs)
                | Field::Fast(FastField::PreComputedScore)
                | Field::Fast(FastField::Region)
                | Field::Fast(FastField::HostNodeID1)
                | Field::Fast(FastField::HostNodeID2)
                | Field::Text(TextField::DmozDescription) => {}
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
                .map(|(name, attr)| (name.local.to_string(), attr.value.to_string()))
                .collect();

            scripts.push(Script {
                attributes,
                content,
            });
        }

        scripts
    }

    pub fn schema_org(&self) -> Vec<schema_org::Item> {
        schema_org::parse(self.root.clone())
    }

    pub fn trackers(&self) -> Vec<Url> {
        let mut links: Vec<Url> = Vec::new();

        for script in self.scripts() {
            if let Some(link) = script
                .attributes
                .get("src")
                .and_then(|link| Url::parse(link).or_else(|_| self.url().join(link)).ok())
            {
                links.push(link);
            }

            for res in URL_REGEX.find_iter(&script.content) {
                if let Ok(link) =
                    Url::parse(res.as_str()).or_else(|_| self.url().join(res.as_str()))
                {
                    links.push(link);
                }
            }
        }

        for node in self.root.select("link").unwrap() {
            if let Some(link) = node
                .attributes
                .borrow()
                .get("href")
                .and_then(|link| Url::parse(link).or_else(|_| self.url().join(link)).ok())
            {
                links.push(link);
            }
        }

        links
            .into_iter()
            .filter(|link| link.host_str().is_some())
            .filter(|link| link.host_str() != self.url().host_str())
            .unique_by(|link| link.host_str().unwrap().to_string())
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
            .and_then(|metadata| {
                metadata
                    .get("content")
                    .and_then(|link| Url::parse(link).or_else(|_| self.url().join(link)).ok())
            })
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
            .filter(|item| item.types_contains("ImageObject"))
            .filter_map(|item| {
                item.properties.get("contentUrl").map(|content_url| {
                    content_url
                        .clone()
                        .many()
                        .into_iter()
                        .filter_map(|url| url.try_into_string())
                        .filter_map(|url| Url::parse(&url).or_else(|_| self.url().join(&url)).ok())
                })
            })
            .flatten()
            .collect()
    }

    pub fn updated_time(&self) -> Option<DateTime<FixedOffset>> {
        if let Some(time) = self
            .og_updated_time()
            .or_else(|| self.article_modified_time())
        {
            let current_time = Utc::now();

            if time > current_time {
                None
            } else {
                Some(time)
            }
        } else {
            None
        }
    }

    pub fn primary_image(&self) -> Option<ImageLink> {
        self.og_image().or_else(|| {
            self.schema_org_images()
                .first()
                .cloned()
                .map(|url| ImageLink {
                    url,
                    title: self.og_title(),
                    description: self.description(),
                })
        })
    }

    pub fn og_description(&self) -> Option<String> {
        self.metadata()
            .into_iter()
            .find(|metadata| {
                if let Some(property) = metadata.get("property") {
                    property.as_str() == "og:description"
                } else {
                    false
                }
            })
            .and_then(|metadata| metadata.get("content").cloned())
    }

    pub fn metadata_description(&self) -> Option<String> {
        self.metadata()
            .into_iter()
            .find(|metadata| {
                if let Some(name) = metadata.get("name") {
                    name.as_str() == "description" || name.as_str() == "Description"
                } else {
                    false
                }
            })
            .and_then(|metadata| metadata.get("content").cloned())
    }

    pub fn description(&self) -> Option<String> {
        self.og_description()
            .or_else(|| self.metadata_description())
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

    pub fn is_homepage(&self) -> bool {
        self.url().path() == "/" && self.url().query().is_none()
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

    use crate::url_ext::UrlExt;
    use schema::create_schema;

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
                    <a href="https://example.com">Link to example</a>
                    <p>{CONTENT}</p>
                    <a href="mailto:hello@example.com">Email me</a>
                </body>
            </html>
        "#
        );

        let webpage = Html::parse(&raw, "https://www.example.com/whatever").unwrap();

        assert_eq!(webpage.title(), Some("Best website".to_string()));

        assert_eq!(
            webpage.anchor_links(),
            vec![Link {
                source: Url::parse("https://www.example.com/whatever").unwrap(),
                destination: Url::parse("https://example.com").unwrap(),
                text: "Link to example".to_string()
            }]
        );
        assert_eq!(webpage.clean_text(), Some(&CONTENT.to_string()));

        let mut expected_meta = HashMap::new();
        expected_meta.insert("name".to_string(), "meta1".to_string());
        expected_meta.insert("content".to_string(), "value".to_string());

        assert_eq!(webpage.metadata(), vec![expected_meta]);
        assert_eq!(
            webpage.url().to_string().as_str(),
            "https://www.example.com/whatever"
        );
        assert_eq!(webpage.url().host_str().unwrap(), "www.example.com");
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

        let webpage = Html::parse(&raw, "https://www.example.com/whatever").unwrap();

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

        let webpage = Html::parse(&raw, "https://www.example.com/whatever").unwrap();

        assert_eq!(webpage.clean_text(), Some(&CONTENT.to_string()));
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

        let webpage = Html::parse(&raw, "https://www.example.com").unwrap();

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

        let webpage = Html::parse(&raw, "https://www.example.com").unwrap();

        assert!(!webpage.clean_text().unwrap().contains("not"));
    }

    #[test]
    fn co_uk_domain() {
        let raw = "";

        let webpage = Html::parse(raw, "https://www.domain.co.uk").unwrap();
        assert_eq!(
            webpage.url().root_domain().unwrap_or_default(),
            "domain.co.uk"
        );
    }

    #[test]
    fn is_homepage() {
        let webpage = Html::parse("", "https://www.example.com").unwrap();
        assert!(webpage.is_homepage());

        let webpage = Html::parse("", "https://www.example.com/").unwrap();
        assert!(webpage.is_homepage());

        let webpage = Html::parse("", "https://www.example.com/test").unwrap();
        assert!(!webpage.is_homepage());

        let webpage = Html::parse("", "https://example.com/test").unwrap();
        assert!(!webpage.is_homepage());

        let webpage = Html::parse("", "https://example.com/").unwrap();
        assert!(webpage.is_homepage());

        let webpage = Html::parse("", "https://example.com").unwrap();
        assert!(webpage.is_homepage());

        let webpage = Html::parse("", "http://example.com").unwrap();
        assert!(webpage.is_homepage());
    }

    #[test]
    fn hard_parsing() {
        let webpage = Html::parse(
            include_str!("../../core/testcases/parsing/yasudaya.html"),
            "https://example.com",
        )
        .unwrap();
        assert_eq!(
            webpage.title(),
            Some("パチンコ大当たり情報 - Ｐジューシーハニー３ 大当たり詳細ページ - やすだひばりヶ丘店".to_string())
        );
        assert!(webpage.all_text().is_some());
        assert!(!webpage.all_text().unwrap().is_empty());

        let webpage = Html::parse(
            include_str!("../../core/testcases/parsing/5390001.html"),
            "https://example.com",
        )
        .unwrap();
        assert_eq!(
            webpage.title(),
            Some("特效烟机系列_山东壹线文化传播有限公司".to_string())
        );
        assert!(webpage.all_text().is_some());
        assert!(!webpage.all_text().unwrap().is_empty());

        let webpage = Html::parse(
            include_str!("../../core/testcases/parsing/77p2p-7.live-105.html"),
            "https://example.com",
        )
        .unwrap();
        assert_eq!(
            webpage.title(),
            Some("77p2pЅu¤WЖ[¬Э - ҐDјЅ :: іnєс".to_string())
        );
        assert!(webpage.all_text().is_some());
        assert!(!webpage.all_text().unwrap().is_empty());
    }

    #[test]
    fn reddit_comments() {
        let webpage = Html::parse(
            include_str!("../../core/testcases/parsing/reddit.html"),
            "https://reddit.com/",
        )
        .unwrap();

        assert!(webpage.clean_text().is_some());
        assert!(webpage.clean_text().unwrap().len() > 1000);
        assert!(webpage
            .all_text()
            .unwrap()
            .contains("They immediately moved outta striking range"));
    }

    #[test]
    fn out_of_bounds_str() {
        let webpage = Html::parse(
            include_str!("../../core/testcases/parsing/byte_index_out_of_bounds.html"),
            "https://example.com",
        )
        .unwrap();
        assert_eq!(webpage.title(), Some("Test".to_string()));
        assert!(webpage.all_text().is_some());
        assert!(!webpage.all_text().unwrap().is_empty());

        let schema = create_schema();
        webpage.into_tantivy(&schema).unwrap();
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

        let webpage = Html::parse(&raw, "https://www.example.com").unwrap();
        assert_eq!(
            webpage.favicon(),
            Some(FaviconLink {
                link: Url::parse("https://example.com/favicon.png").unwrap(),
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

        let webpage = Html::parse(&raw, site_url).unwrap();
        assert_eq!(
            webpage.favicon(),
            Some(FaviconLink {
                link: Url::parse(expected).unwrap(),
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
            "https://www.example.com/test/",
            "https://www.example.com/test/favicon.png",
        );
        full_link_favicon(
            "/favicon.png",
            "https://www.example.com/test",
            "https://www.example.com/favicon.png",
        );
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
        let html = Html::parse(html, "https://example.com").unwrap();

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
        let html = Html::parse(html, "https://example.com").unwrap();

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
        let html = Html::parse(html, "https://example.com").unwrap();

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
        let html = Html::parse(html, "https://example.com").unwrap();

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
        let html = Html::parse(html, "https://example.com").unwrap();

        assert_eq!(
            html.primary_image(),
            Some(ImageLink {
                url: Url::parse("https://example.com/link_to_image.html").unwrap(),
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
        let html = Html::parse(html, "https://example.com").unwrap();

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
        let html = Html::parse(html, "https://example.com").unwrap();

        assert_eq!(
            html.primary_image(),
            Some(ImageLink {
                url: Url::parse("https://example.com/mexico-beach.jpg").unwrap(),
                title: None,
                description: None
            })
        );
    }

    #[test]
    fn future_updated_time_none() {
        let html = r#"
    <html>
        <head>
            <meta property="og:updated_time" content="2122-06-22T19:37:34+00:00" />
        </head>
        <body>
        </body>
    </html>
        "#;
        let html = Html::parse(html, "http://example.com").unwrap();

        assert_eq!(html.updated_time(), None);
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
        let html = Html::parse(html, "http://example.com").unwrap();

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
        let html = Html::parse(html, "http://example.com").unwrap();

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
        let html = Html::parse(html, "http://example.com").unwrap();

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
        let html = Html::parse(html, "http://example.com").unwrap();

        assert_eq!(
            html.trackers()
                .into_iter()
                .map(|url| url.host_str().unwrap().to_string())
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
            "https://example.com",
        ).unwrap();

        assert_eq!(html.title(), Some("Test site".to_string()));
        assert_eq!(html.all_text(), Some("test".to_string()));
    }

    #[test]
    fn dmoz_description() {
        let html = Html::parse(
            r#"
                    <html>
                        <head>
                            <title>Test site</title>
                        </head>
                        <body>
                            test
                        </body>
                    </html>
                "#,
            "https://example.com",
        )
        .unwrap();

        let webpage = Webpage {
            html,
            backlink_labels: Vec::new(),
            host_centrality: 0.0,
            page_centrality: 0.0,
            fetch_time_ms: 500,
            pre_computed_score: 0.0,
            node_id: None,
            dmoz_description: Some("dmoz description".to_string()),
            safety_classification: None,
        };

        assert_eq!(
            webpage.dmoz_description(),
            Some("dmoz description".to_string())
        )
    }

    #[test]
    fn noodp_ignores_dmoz() {
        let html = Html::parse(
            r#"
                    <html>
                        <head>
                            <meta name="robots" content="noodp" />
                            <title>Test site</title>
                        </head>
                        <body>
                            test
                        </body>
                    </html>
                "#,
            "http://example.com",
        )
        .unwrap();
        let webpage = Webpage {
            html,
            backlink_labels: Vec::new(),
            host_centrality: 0.0,
            page_centrality: 0.0,
            fetch_time_ms: 500,
            pre_computed_score: 0.0,
            node_id: None,
            dmoz_description: Some("dmoz description".to_string()),
            safety_classification: None,
        };

        assert_eq!(webpage.dmoz_description(), None)
    }

    #[test]
    fn links() {
        let raw = format!(
            r#"
            <html>
                <head>
                    <title>Best website</title>
                    <meta name="meta1" content="value">
                    <link href="link.com" />
                    <script src="test.com"></script>
                </head>
                <body>
                    <a href="https://example.com">Link to example</a>
                    <p>{CONTENT}</p>
                </body>
            </html>
        "#
        );

        let webpage = Html::parse(&raw, "https://www.example.com/whatever").unwrap();

        assert_eq!(webpage.title(), Some("Best website".to_string()));

        assert_eq!(
            webpage.anchor_links(),
            vec![Link {
                source: Url::parse("https://www.example.com/whatever").unwrap(),
                destination: Url::parse("https://example.com").unwrap(),
                text: "Link to example".to_string()
            },]
        );
    }

    #[test]
    fn stackoverflow_question_has_clean_text() {
        let stackoverflow =
            include_str!("../../core/testcases/schema_org/stackoverflow_with_code.html");
        let html = Html::parse(stackoverflow, "https://www.example.com").unwrap();

        assert!(html.clean_text().is_some());
    }

    #[test]
    fn canonical_url() {
        let html = Html::parse(
            r#"
            <html>
                <head>
                    <link rel="canonical" href="https://example.com/canonical.html" />
                </head>
                <body>
                </body>
            </html>
        "#,
            "https://www.example.com/whatever",
        )
        .unwrap();

        assert_eq!(
            html.canonical_url(),
            Some(Url::parse("https://example.com/canonical.html").unwrap())
        );

        let html = Html::parse(
            r#"
            <html>
                <head>
                </head>
                <body>
                </body>
            </html>
        "#,
            "https://www.example.com/whatever",
        )
        .unwrap();

        assert_eq!(html.canonical_url(), None);
        assert_eq!(
            html.url(),
            &Url::parse("https://www.example.com/whatever").unwrap()
        );
    }

    #[test]
    fn robots_meta_tag() {
        let html = Html::parse(
            r#"
            <html>
                <head>
                    <meta name="robots" content="noindex, nofollow" />
                </head>
                <body>
                </body>
            </html>
        "#,
            "https://www.example.com/whatever",
        )
        .unwrap();

        assert!(html.is_no_index());
        assert!(html.is_no_follow());

        let html = Html::parse(
            r#"
            <html>
                <head>
                    <meta name="robots" content="noindex,nofollow" />
                </head>
                <body>
                </body>
            </html>
        "#,
            "https://www.example.com/whatever",
        )
        .unwrap();

        assert!(html.is_no_index());
        assert!(html.is_no_follow());

        let html = Html::parse(
            r#"
            <html>
                <head>
                    <meta name="robots" content="noindex" />
                </head>
                <body>
                </body>
            </html>
        "#,
            "https://www.example.com/whatever",
        )
        .unwrap();

        assert!(html.is_no_index());
        assert!(!html.is_no_follow());

        let html = Html::parse(
            r#"
            <html>
                <head>
                    <meta name="robots" content="nofollow" />
                </head>
                <body>
                </body>
            </html>
        "#,
            "https://www.example.com/whatever",
        )
        .unwrap();

        assert!(!html.is_no_index());
        assert!(html.is_no_follow());

        let html = Html::parse(
            r#"
            <html>
                <head>
                </head>
                <body>
                </body>
            </html>
        "#,
            "https://www.example.com/whatever",
        )
        .unwrap();

        assert!(!html.is_no_index());
        assert!(!html.is_no_follow());
    }

    #[test]
    fn microformats() {
        let html = Html::parse(
            r#"
            <html>
                <head>
                </head>
                <body>
                    <article class="h-entry">
                        <h1 class="p-name">Microformats are amazing</h1>
                        <p class="e-content">This is the content of the article</p>
                        <a class="u-url" href="https://example.com/microformats">Permalink</a>
                        <a class="u-author" href="https://example.com">Author</a>
                        <p class="search-product">substrings should not match</p>
                        <time class="dt-published" datetime="2021-01-01T00:00:00+00:00">2021-01-01</time>
                    </article>

                    <div class="h-RECIPE">
                        For some reason this site also has a recipe
                    </div>
                </body>
            </html>
            "#,
            "https://www.example.com/",
        ).unwrap();

        let microformats = html.microformats();

        assert!(microformats.contains(Microformat::HEntry));
        assert!(microformats.contains(Microformat::HRecipe));
        assert!(!microformats.contains(Microformat::HCard));
        assert!(!microformats.contains(Microformat::HProduct));
    }
}
