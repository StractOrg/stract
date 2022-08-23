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
use crate::{schema_org::SchemaOrg, Error, Result};
use chrono::{DateTime, FixedOffset};
use itertools::Itertools;
use logos::{Lexer, Logos};
use regex::Regex;
use std::collections::HashMap;
use uuid::Uuid;

mod just_text;
mod lexer;
pub mod region;
mod url;

use crate::schema::{Field, ALL_FIELDS, CENTRALITY_SCALING};

pub use self::url::Url;
use self::{just_text::JustText, lexer::Token};

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

pub struct Preprocessor<const N: usize> {
    removed_tags: [&'static str; N],
    num_open_tags: [i64; N],
    open_comments: i64,
}

impl<const N: usize> Preprocessor<N> {
    pub fn new(removed_tags: [&'static str; N]) -> Self {
        Self {
            removed_tags,
            num_open_tags: [0; N],
            open_comments: 0,
        }
    }

    pub fn update(&mut self, tok: &Token) {
        match tok {
            Token::StartTag(tag) if !tag.raw().contains("/>") => {
                if let Some((_, n)) = self
                    .removed_tags
                    .iter()
                    .zip(self.num_open_tags.iter_mut())
                    .find(|(name, _)| **name == tag.name())
                {
                    *n += 1;
                }
            }
            Token::EndTag(tag) => {
                if let Some((_, n)) = self
                    .removed_tags
                    .iter()
                    .zip(self.num_open_tags.iter_mut())
                    .find(|(name, _)| **name == tag.name())
                {
                    *n -= 1;
                }
            }
            Token::BeginComment => self.open_comments += 1,
            Token::EndComment => self.open_comments -= 1,
            _ => {}
        }
    }

    pub fn is_inside_removed(&self) -> bool {
        self.num_open_tags.iter().any(|n| *n > 0) || self.open_comments > 0
    }
}

pub struct Webpage<'a> {
    pub html: Html<'a>,
    pub backlinks: Vec<Link>,
    pub centrality: f64,
    pub fetch_time_ms: u64,
    pub primary_image_uuid: Option<Uuid>,
}

impl<'a> Webpage<'a> {
    #[cfg(test)]
    pub fn new(
        html: &'a str,
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
            primary_image_uuid: None,
        }
    }

    pub fn into_tantivy(self, schema: &tantivy::schema::Schema) -> Result<tantivy::Document> {
        let mut doc = self.html.into_tantivy(schema)?;

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

        let uuid = self
            .primary_image_uuid
            .map(|uuid| uuid.to_string())
            .unwrap_or_default();
        doc.add_text(
            schema
                .get_field(Field::PrimaryImageUuid.as_str())
                .expect("Failed to get primary_image_uuid field"),
            uuid,
        );

        Ok(doc)
    }

    pub(crate) fn set_primary_image_uuid(&mut self, uuid: Uuid) {
        self.primary_image_uuid = Some(uuid);
    }
}

#[derive(Debug)]
pub struct Html<'a> {
    raw: &'a str,
    tokens: Lexer<'a, Token<'a>>,
    url: Url,
}

impl<'a> Html<'a> {
    pub fn parse(html: &'a str, url: &str) -> Self {
        let tokens = Token::lexer(html);

        Self {
            raw: html,
            tokens,
            url: url.to_string().into(),
        }
    }

    pub fn links(&self) -> Vec<Link> {
        let mut tokens = self.tokens.clone();
        let mut links = Vec::new();
        let mut open_links = Vec::new();
        let mut preprocessor = Preprocessor::new(["script", "style", "head"]);

        while let Some(tok) = tokens.next() {
            preprocessor.update(&tok);
            if preprocessor.is_inside_removed() {
                continue;
            }

            match tok {
                Token::StartTag(tag) if tag.name() == "a" => {
                    open_links.push((String::new(), tag.attributes()));
                }
                Token::EndTag(tag) if tag.name() == "a" => {
                    if let Some((text, attributes)) = open_links.pop() {
                        if let Some(dest) = attributes.get("href") {
                            links.push(Link {
                                source: self.url.clone(),
                                destination: dest.to_string().into(),
                                text,
                            })
                        }
                    }
                }
                Token::Error => {
                    for (text, _) in &mut open_links {
                        let span = tokens.span();
                        text.push_str(&self.raw[span]);
                    }
                }
                Token::SelfTerminatingTag(tag) if tag.name() == "a" => {
                    if let Some(dest) = tag.attributes().get("href") {
                        links.push(Link {
                            source: self.url.clone(),
                            destination: dest.to_string().into(),
                            text: String::new(),
                        })
                    }
                }
                Token::StartTag(_)
                | Token::EndTag(_)
                | Token::SelfTerminatingTag(_)
                | Token::BeginComment
                | Token::EndComment => {}
            }
        }

        links
    }

    pub fn favicon(&self) -> Option<FaviconLink> {
        let mut preprocessor = Preprocessor::new(["script", "style", "body"]);

        let tokens = self.tokens.clone();
        for tok in tokens {
            preprocessor.update(&tok);
            if preprocessor.is_inside_removed() {
                continue;
            }

            match tok {
                Token::StartTag(tag) | Token::SelfTerminatingTag(tag) if tag.name() == "link" => {
                    let rel = tag.attributes().get("rel").cloned();
                    if rel.is_none() {
                        continue;
                    }
                    if rel.unwrap() != "icon" {
                        continue;
                    }

                    if let Some(link) = tag.attributes().get("href") {
                        let (width, height) = match tag.attributes().get("sizes") {
                            Some(size) => {
                                if let Some((width, height)) = size.split_once('x') {
                                    (width.parse().ok(), height.parse().ok())
                                } else {
                                    (None, None)
                                }
                            }
                            _ => (None, None),
                        };

                        let image_type = tag.attributes().get("type").map(|t| t.to_string());
                        let mut link: Url = link.to_string().into();

                        if !link.is_full_path() {
                            link.prefix_with(&self.url);
                        }

                        let favicon = FaviconLink {
                            link,
                            image_type,
                            width,
                            height,
                        };
                        return Some(favicon);
                    }
                }
                Token::StartTag(_)
                | Token::EndTag(_)
                | Token::SelfTerminatingTag(_)
                | Token::BeginComment
                | Token::Error
                | Token::EndComment => {}
            }
        }

        None
    }

    pub fn clean_text(&self) -> Option<String> {
        let text = JustText::default().extract(self.raw);

        if text.is_empty() {
            None
        } else {
            Some(text)
        }
    }

    pub fn all_text(&self) -> Option<String> {
        let text = JustText {
            max_link_density: 2.0,
            length_low: 0,
            length_high: 0,
            stopwords_low: -1.0,
            stopwords_high: -1.0,
            max_heading_distance: 10000,
        }
        .extract(self.raw);

        if text.is_empty() {
            None
        } else {
            Some(text)
        }
    }

    pub fn title(&self) -> Option<String> {
        let mut tokens = self.tokens.clone();
        let mut title = None;
        let mut open_tags = 0;
        let mut preprocessor = Preprocessor::new(["script", "style"]);

        while let Some(tok) = tokens.next() {
            preprocessor.update(&tok);
            if preprocessor.is_inside_removed() {
                continue;
            }

            match tok {
                Token::StartTag(tag) if tag.name() == "title" => {
                    open_tags += 1;
                }
                Token::EndTag(tag) if tag.name() == "title" => {
                    open_tags -= 1;

                    if open_tags == 0 {
                        break;
                    }
                }
                Token::Error => {
                    if open_tags > 0 {
                        let span = tokens.span();
                        if let Some(cur_title) = title {
                            title = Some(cur_title + &self.raw[span]);
                        } else {
                            title = Some(self.raw[span].to_string());
                        }
                    }
                }
                Token::SelfTerminatingTag(_)
                | Token::StartTag(_)
                | Token::EndTag(_)
                | Token::BeginComment
                | Token::EndComment => {}
            }
        }

        title
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub fn host(&self) -> &str {
        self.url.host()
    }

    pub fn domain(&self) -> &str {
        self.url.domain()
    }

    pub fn is_homepage(&self) -> bool {
        self.url.is_homepage()
    }

    pub fn metadata(&self) -> Vec<Meta> {
        let tokens = self.tokens.clone();
        let mut metas = Vec::new();
        let mut preprocessor = Preprocessor::new(["script", "style"]);

        for tok in tokens {
            preprocessor.update(&tok);
            if preprocessor.is_inside_removed() {
                continue;
            }

            match tok {
                Token::StartTag(tag) if tag.name() == "meta" => {
                    metas.push(
                        tag.attributes()
                            .into_iter()
                            .map(|(key, value)| (key.to_string(), value.to_string()))
                            .collect(),
                    );
                }
                Token::SelfTerminatingTag(tag) if tag.name() == "meta" => {
                    metas.push(
                        tag.attributes()
                            .into_iter()
                            .map(|(key, value)| (key.to_string(), value.to_string()))
                            .collect(),
                    );
                }
                Token::StartTag(_)
                | Token::EndTag(_)
                | Token::SelfTerminatingTag(_)
                | Token::Error
                | Token::BeginComment
                | Token::EndComment => {}
            }
        }

        metas
    }

    pub fn into_tantivy(self, schema: &tantivy::schema::Schema) -> Result<tantivy::Document> {
        let mut doc = tantivy::Document::new();

        for field in &ALL_FIELDS {
            let tantivy_field = schema
                .get_field(field.as_str())
                .unwrap_or_else(|| panic!("Unknown field: {}", field.as_str()));

            match field {
                Field::Title | Field::StemmedTitle => {
                    let title = self.title();

                    if title.is_none() {
                        return Err(Error::EmptyField("title"));
                    }

                    doc.add_text(tantivy_field, title.unwrap());
                }
                Field::CleanBody | Field::StemmedCleanBody => {
                    let text = self.clean_text();

                    doc.add_text(tantivy_field, text.unwrap_or_default())
                }
                Field::Description => {
                    doc.add_text(tantivy_field, self.description().unwrap_or_default())
                }
                Field::Url => doc.add_text(tantivy_field, self.url()),
                Field::Host => doc.add_text(tantivy_field, self.host()),
                Field::Domain => doc.add_text(tantivy_field, self.domain()),
                Field::DomainIfHomepage => {
                    if self.is_homepage() {
                        doc.add_text(tantivy_field, self.domain())
                    } else {
                        doc.add_text(tantivy_field, "")
                    }
                }
                Field::IsHomepage => {
                    doc.add_u64(tantivy_field, if self.is_homepage() { 1 } else { 0 })
                }
                Field::LastUpdated => doc.add_u64(
                    tantivy_field,
                    self.updated_time()
                        .map(|time| time.timestamp().max(0) as u64)
                        .unwrap_or(0),
                ),
                Field::StemmedAllBody | Field::AllBody => {
                    let text = self.all_text();

                    if text.is_none() {
                        return Err(Error::EmptyField("all body"));
                    }

                    doc.add_text(tantivy_field, text.unwrap())
                }
                Field::NumTrackers => doc.add_u64(tantivy_field, self.trackers().len() as u64),
                Field::BacklinkText
                | Field::Centrality
                | Field::FetchTimeMs
                | Field::PrimaryImageUuid => {}
            }
        }

        Ok(doc)
    }

    pub fn schema_org(&self) -> Vec<SchemaOrg> {
        let mut tokens = self.tokens.clone();
        let mut schemas = Vec::new();
        let mut preprocessor = Preprocessor::new([]);
        let mut open_schemas = 0;
        let mut json = String::new();

        while let Some(tok) = tokens.next() {
            preprocessor.update(&tok);
            if preprocessor.is_inside_removed() {
                continue;
            }

            match tok {
                Token::StartTag(tag) if tag.name() == "script" => {
                    if let Some(&"application/ld+json") = tag.attributes().get("type") {
                        open_schemas += 1;
                    }
                }
                Token::EndTag(tag) if tag.name() == "script" => {
                    if open_schemas > 0 {
                        open_schemas -= 1;

                        if open_schemas == 0 {
                            if let Ok(schema) = serde_json::from_str(&json) {
                                schemas.push(schema);
                            }
                            json = String::new();
                        }
                    }
                }
                Token::Error => {
                    let span = tokens.span();
                    json.push_str(&self.raw[span]);
                }
                Token::StartTag(_)
                | Token::EndTag(_)
                | Token::SelfTerminatingTag(_)
                | Token::BeginComment
                | Token::EndComment => {}
            }
        }

        schemas
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

    fn og_image(&self) -> Option<Url> {
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
    }

    #[allow(unreachable_patterns)]
    fn schema_org_images(&self) -> Vec<Url> {
        self.schema_org()
            .into_iter()
            .filter(|schema| matches!(schema, SchemaOrg::ImageObject(_)))
            .flat_map(|schema| {
                match schema {
                    SchemaOrg::ImageObject(image) => image.content_url.map(|url| url.into()),
                    _ => None, // has been filtered, so only image is possible
                }
            })
            .collect()
    }

    pub fn updated_time(&self) -> Option<DateTime<FixedOffset>> {
        self.og_updated_time()
            .or_else(|| self.article_modified_time())
    }

    pub fn primary_image(&self) -> Option<Url> {
        self.og_image()
            .or_else(|| self.schema_org_images().first().cloned())
            .map(|mut url| {
                if !url.is_full_path() {
                    url.prefix_with(&self.url);
                }
                url
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

    pub fn trackers(&self) -> Vec<Url> {
        let mut tokens = self.tokens.clone();

        let mut links: Vec<Url> = Vec::new();
        let mut open_scripts = 0;
        let mut script_text = String::new();
        let mut preprocessor = Preprocessor::new([]);

        while let Some(tok) = tokens.next() {
            preprocessor.update(&tok);
            if preprocessor.is_inside_removed() {
                continue;
            }

            match tok {
                Token::StartTag(tag) if tag.name() == "script" => {
                    if !tag.raw().contains("/>") {
                        open_scripts += 1;
                    }
                    if let Some(link) = tag.attributes().get("src") {
                        links.push(link.to_string().into());
                    }
                }
                Token::EndTag(tag) if tag.name() == "script" => {
                    if open_scripts > 0 {
                        open_scripts -= 1;

                        if open_scripts == 0 {
                            for res in URL_REGEX.find_iter(script_text.as_str()) {
                                links.push(res.as_str().to_string().into());
                            }
                            script_text.clear();
                        }
                    }
                }
                _ if open_scripts > 0 => {
                    let span = tokens.span();
                    script_text.push_str(&self.raw[span]);
                }
                Token::Error => {
                    if open_scripts > 0 {
                        let span = tokens.span();
                        script_text.push_str(&self.raw[span]);
                    }
                }
                Token::SelfTerminatingTag(tag) if tag.name() == "script" => {
                    if let Some(link) = tag.attributes().get("src") {
                        links.push(link.to_string().into());
                    }
                }
                Token::StartTag(tag) | Token::SelfTerminatingTag(tag) if tag.name() == "link" => {
                    if let Some(link) = tag.attributes().get("href") {
                        links.push(link.to_string().into());
                    }
                }
                Token::StartTag(_)
                | Token::EndTag(_)
                | Token::SelfTerminatingTag(_)
                | Token::BeginComment
                | Token::EndComment => {}
            }
        }

        links
            .into_iter()
            .filter(|link| !link.host().is_empty())
            .filter(|link| link.host() != self.host())
            .unique_by(|link| link.host().to_string())
            .collect()
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

    use crate::schema_org::ImageObject;

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
        assert_eq!(webpage.host(), "www.example.com");
        assert_eq!(webpage.domain(), "example.com");
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
        assert_eq!(webpage.domain(), "domain.co.uk");
    }

    #[test]
    fn is_homepage() {
        let webpage = Html::parse("", "https://www.example.com");
        assert!(webpage.is_homepage());

        let webpage = Html::parse("", "https://www.example.com/");
        assert!(webpage.is_homepage());

        let webpage = Html::parse("", "https://www.example.com/test");
        assert!(!webpage.is_homepage());

        let webpage = Html::parse("", "https://example.com/test");
        assert!(!webpage.is_homepage());

        let webpage = Html::parse("", "https://example.com/");
        assert!(webpage.is_homepage());

        let webpage = Html::parse("", "https://example.com");
        assert!(webpage.is_homepage());

        let webpage = Html::parse("", "http://example.com");
        assert!(webpage.is_homepage());
    }

    #[test]
    #[ignore = "JustText doesn't find any content on the sites. How should we split into words for Japanese, Chinese etc.?"]
    fn hard_parsing() {
        let webpage = Html::parse(include_str!("../../testcases/parsing/yasudaya.html"), "");
        assert_eq!(
            webpage.title(),
            Some("パチンコ大当たり情報 - Ｐジューシーハニー３ 大当たり詳細ページ - やすだひばりヶ丘店".to_string())
        );
        assert!(webpage.clean_text().is_some());
        assert!(!webpage.clean_text().unwrap().is_empty());

        let webpage = Html::parse(include_str!("../../testcases/parsing/5390001.html"), "");
        assert_eq!(
            webpage.title(),
            Some("特效烟机系列_山东壹线文化传播有限公司".to_string())
        );
        assert!(webpage.clean_text().is_some());
        assert!(!webpage.clean_text().unwrap().is_empty());

        let webpage = Html::parse(
            include_str!("../../testcases/parsing/77p2p-7.live-105.html"),
            "",
        );
        assert_eq!(
            webpage.title(),
            Some("77p2pЅu¤WЖ[¬Э - ҐDјЅ :: іnєс ".to_string())
        );
        assert!(webpage.clean_text().is_some());
        assert!(!webpage.clean_text().unwrap().is_empty());
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
        )
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
        )
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

        assert!(html.schema_org().is_empty())
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
        </head>
        <body>
        </body>
    </html>
        "#;
        let html = Html::parse(html, "example.com");

        assert_eq!(
            html.primary_image(),
            Some("https://example.com/link_to_image.html".to_string().into())
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
            Some("https://example.com/mexico-beach.jpg".to_string().into())
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
                    <script src="https://thirdparty.com/js" />
                    <script src="https://example.com/js" />
                    <link href='//securepubads.g.doubleclick.net' rel='preconnect'>
                    <script src="https://thirdparty.com/js" />
                    <script src="/js/file" />
                    <meta property="article:modified_time" content="2022-06-22T19:37:34+00:00" />
                </head>
                <body>
                </body>
            </html>
        "#;
        let html = Html::parse(html, "example.com");

        assert_eq!(
            html.trackers()
                .into_iter()
                .map(|url| url.host().to_string())
                .collect::<Vec<_>>(),
            vec![
                "cdn.segment.com".to_string(),
                "static.hotjar.com".to_string(),
                "thirdparty.com".to_string(),
                "securepubads.g.doubleclick.net".to_string()
            ]
        )
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
                            <script src="https://thirdparty.com/js" />
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
        assert_eq!(html.all_text(), Some("test".to_string()))
    }
}
