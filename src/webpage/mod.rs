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
use crate::{Error, Result};
use logos::{Lexer, Logos};
use std::collections::HashMap;

mod just_text;
mod lexer;

use crate::schema::{Field, ALL_FIELDS, CENTRALITY_SCALING};

use self::{just_text::JustText, lexer::Token};

pub fn strip_protocol(url: &str) -> &'_ str {
    let mut start_host = 0;
    if url.starts_with("http://") || url.starts_with("https://") {
        start_host = url
            .find('/')
            .expect("It was checked that url starts with protocol");
        start_host += 2; // skip the two '/'
    }

    &url[start_host..]
}

pub fn strip_query(url: &str) -> &'_ str {
    let mut start_query = url.len();
    if url.contains('?') {
        start_query = url.find('?').expect("The url contains atleast 1 '?'");
    }

    &url[..start_query]
}

pub fn host(url: &str) -> &'_ str {
    let url = strip_protocol(url);

    let mut end_host = url.len();
    if url.contains('/') {
        end_host = url.find('/').expect("The url contains atleast 1 '/'");
    }

    &url[..end_host]
}

pub fn is_homepage(url: &str) -> bool {
    let url = strip_protocol(url);
    match url.find('/') {
        Some(idx) => idx == url.len() - 1,
        None => true,
    }
}

pub fn domain(url: &str) -> &'_ str {
    let host = host(url);
    let num_punctuations: usize = host.chars().map(|c| if c == '.' { 1 } else { 0 }).sum();
    if num_punctuations > 1 {
        let domain_index = host.rfind('.').unwrap();
        let mut start_index = host[..domain_index].rfind('.').unwrap();

        if &host[start_index + 1..] == "co.uk" {
            start_index = host[start_index..].rfind('.').unwrap();
        }

        &host[start_index + 1..]
    } else {
        host
    }
}

pub struct Preprocessor<const N: usize> {
    removed_tags: [&'static str; N],
    num_open_tags: [usize; N],
    open_comments: usize,
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
            Token::StartTag(tag) => {
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
            Token::SelfTerminatingTag(_) | Token::Error => {}
            Token::BeginComment => self.open_comments += 1,
            Token::EndComment => self.open_comments -= 1,
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
}

impl<'a> Webpage<'a> {
    pub fn new(html: &'a str, url: &str, backlinks: Vec<Link>, centrality: f64) -> Self {
        let html = Html::parse(html, url);

        Self {
            html,
            backlinks,
            centrality,
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

        Ok(doc)
    }
}

#[derive(Debug)]
pub struct Html<'a> {
    raw: &'a str,
    tokens: Lexer<'a, Token<'a>>,
    url: String,
}

impl<'a> Html<'a> {
    pub fn parse(html: &'a str, url: &str) -> Self {
        let tokens = Token::lexer(html);

        Self {
            raw: html,
            tokens,
            url: url.to_string(),
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
                                destination: dest.to_string(),
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
                            destination: dest.to_string(),
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

    pub fn text(&self) -> Option<String> {
        let text = JustText::default().extract(self.raw);

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

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn host(&self) -> &str {
        host(self.url())
    }

    pub fn domain(&self) -> &str {
        domain(self.url())
    }

    pub fn is_homepage(&self) -> bool {
        is_homepage(self.url())
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

                    doc.add_text(tantivy_field, title.unwrap())
                }
                Field::Body | Field::StemmedBody => {
                    let text = self.text();

                    if text.is_none() {
                        return Err(Error::EmptyField("body"));
                    }

                    doc.add_text(tantivy_field, text.unwrap())
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
                    doc.add_u64(tantivy_field, self.is_homepage().then(|| 1).unwrap_or(0))
                }
                Field::BacklinkText | Field::Centrality => {}
            }
        }

        Ok(doc)
    }
}

#[derive(Debug, PartialEq)]
pub struct Link {
    pub source: String,
    pub destination: String,
    pub text: String,
}

pub type Meta = HashMap<String, String>;

#[cfg(test)]
mod tests {
    // TODO: make test macro to test both dom parsers

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
                source: "https://www.example.com/whatever".to_string(),
                destination: "example.com".to_string(),
                text: "Link to example".to_string()
            }]
        );
        assert_eq!(webpage.text(), Some(CONTENT.to_string()));

        let mut expected_meta = HashMap::new();
        expected_meta.insert("name".to_string(), "meta1".to_string());
        expected_meta.insert("content".to_string(), "value".to_string());

        assert_eq!(webpage.metadata(), vec![expected_meta]);
        assert_eq!(webpage.url(), "https://www.example.com/whatever");
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

        assert_eq!(webpage.text(), Some(CONTENT.to_string()));
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

        assert!(!webpage.text().unwrap().contains("not"));
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

        assert!(!webpage.text().unwrap().contains("not"));
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
    #[ignore = "JustText doesn't find any content on the sites. Maybe we should tune parameters?"]
    fn hard_parsing() {
        let webpage = Html::parse(include_str!("../../testcases_parsing/yasudaya.html"), "");
        assert_eq!(
            webpage.title(),
            Some("パチンコ大当たり情報 - Ｐジューシーハニー３ 大当たり詳細ページ - やすだひばりヶ丘店".to_string())
        );
        assert!(webpage.text().is_some());
        assert!(!webpage.text().unwrap().is_empty());

        let webpage = Html::parse(include_str!("../../testcases_parsing/5390001.html"), "");
        assert_eq!(
            webpage.title(),
            Some("特效烟机系列_山东壹线文化传播有限公司".to_string())
        );
        assert!(webpage.text().is_some());
        assert!(!webpage.text().unwrap().is_empty());

        let webpage = Html::parse(
            include_str!("../../testcases_parsing/77p2p-7.live-105.html"),
            "",
        );
        assert_eq!(
            webpage.title(),
            Some("77p2pЅu¤WЖ[¬Э - ҐDјЅ :: іnєс ".to_string())
        );
        assert!(webpage.text().is_some());
        assert!(!webpage.text().unwrap().is_empty());
    }
}
