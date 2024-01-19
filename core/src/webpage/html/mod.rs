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

use crate::{enum_map::EnumSet, Result};
use chrono::{DateTime, FixedOffset, Utc};
use itertools::Itertools;
use kuchiki::{traits::TendrilSink, NodeRef};
use regex::Regex;
use url::Url;
use whatlang::Lang;

use self::robots_meta::RobotsMeta;

use super::{adservers::AD_SERVERS, schema_org, Meta, Script};

use super::url_ext::UrlExt;

mod into_tantivy;
mod links;
mod microformats;
mod parse_text;
mod robots_meta;

pub static URL_REGEX: once_cell::sync::Lazy<Regex> = once_cell::sync::Lazy::new(|| {
    Regex::new(r"(((http|ftp|https):/{2})+(([0-9a-z_-]+\.)+(aero|asia|biz|cat|com|coop|edu|gov|info|int|jobs|mil|mobi|museum|name|net|org|pro|tel|travel|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cu|cv|cx|cy|cz|cz|de|dj|dk|dm|do|dz|ec|ee|eg|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mn|mn|mo|mp|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|nom|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ra|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|sj|sk|sl|sm|sn|so|sr|st|su|sv|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw|arpa)(:[0-9]+)?((/([~0-9a-zA-Z\#\+%@\./_-]+))?(\?[0-9a-zA-Z\+%@/&\[\];=_-]+)?)?))\b").unwrap()
});

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

    #[cfg(test)]
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

        if let Some(canonical) = res.canonical_url() {
            if canonical.root_domain() == res.url.root_domain() {
                res.url = canonical;
            }
        }

        if res.url.query().unwrap_or_default().is_empty() {
            res.url.set_query(None);
        }

        res.robots = res.parse_robots_meta();

        Ok(res)
    }

    pub fn lang(&self) -> Option<&'_ Lang> {
        self.lang.as_ref()
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

    pub fn clean_text(&self) -> Option<&String> {
        self.clean_text.as_ref()
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
            .filter(|link| link.normalized_host().is_some())
            .filter(|link| link.normalized_host() != self.url().normalized_host())
            .unique_by(|link| link.normalized_host().unwrap().to_string())
            .collect()
    }

    pub fn likely_has_ads(&self) -> bool {
        for script in self.scripts() {
            if let Some(url) = script
                .attributes
                .get("src")
                .and_then(|url| Url::parse(url).ok())
            {
                if url.root_domain() == self.url().root_domain() {
                    continue;
                }

                if let Some(domain) = url.root_domain() {
                    if AD_SERVERS.is_adserver(domain) {
                        return true;
                    }
                }

                if let Some(host) = url.host_str() {
                    if AD_SERVERS.is_adserver(host) {
                        return true;
                    }
                }
            }
        }

        // check <link> tags
        for node in self.root.select("link").unwrap() {
            if let Some(url) = node
                .attributes
                .borrow()
                .get("href")
                .and_then(|url| Url::parse(url).ok())
            {
                if url.root_domain() == self.url().root_domain() {
                    continue;
                }

                if let Some(domain) = url.root_domain() {
                    if AD_SERVERS.is_adserver(domain) {
                        return true;
                    }
                }

                if let Some(host) = url.host_str() {
                    if AD_SERVERS.is_adserver(host) {
                        return true;
                    }
                }
            }
        }

        false
    }

    pub fn likely_has_paywall(&self) -> bool {
        for item in self.schema_org().into_iter().filter(|item| {
            item.types_contains("NewsArticle")
                || item.types_contains("Article")
                || item.types_contains("BlogPosting")
                || item.types_contains("WebPage")
                || item.types_contains("WebPageElement")
        }) {
            if let Some(free) = item
                .properties
                .get("isAccessibleForFree")
                .and_then(|value| value.clone().one().and_then(|v| v.try_into_string()))
                .map(|value| value.parse().ok().unwrap_or(false))
            {
                return !free;
            }
        }

        false
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
        self.url().is_homepage()
    }
}

fn find_recipe_first_ingredient_tag_id(
    schemas: &[schema_org::Item],
    root: &NodeRef,
) -> Option<String> {
    schemas.iter().find_map(|schema| {
        if let Some(ingredients) = schema.properties.get("recipeIngredient") {
            if let Some(ingredient) = ingredients.clone().many().first() {
                if let Some(ingredient) = ingredient.try_into_string() {
                    let ingredient = ingredient.trim();
                    // find first occurrence in html
                    if let Some(ingredient_node) = root
                        .select("body")
                        .unwrap()
                        .flat_map(|node| node.as_node().descendants())
                        .find(|node| {
                            if let Some(text) = node.as_text() {
                                text.borrow().trim() == ingredient
                            } else {
                                false
                            }
                        })
                    {
                        // find first parent that has an id
                        if let Some(id) = ingredient_node.ancestors().find_map(|node| {
                            node.as_element().and_then(|e| {
                                e.attributes.borrow().get("id").map(|s| s.to_string())
                            })
                        }) {
                            return Some(id.to_string());
                        }
                    }
                }
            }
        }

        None
    })
}

#[cfg(test)]
mod tests {
    // TODO: make test macro to test both dom parsers

    use std::collections::HashMap;

    use crate::{
        schema::create_schema,
        webpage::{url_ext::UrlExt, Link},
    };

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
            include_str!("../../../testcases/parsing/yasudaya.html"),
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
            include_str!("../../../testcases/parsing/5390001.html"),
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
            include_str!("../../../testcases/parsing/77p2p-7.live-105.html"),
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
            include_str!("../../../testcases/parsing/reddit.html"),
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
            include_str!("../../../testcases/parsing/byte_index_out_of_bounds.html"),
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
            include_str!("../../../testcases/schema_org/stackoverflow_with_code.html");
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

        assert_eq!(
            html.url(),
            &Url::parse("https://example.com/canonical.html").unwrap()
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

        let html = Html::parse(
            r#"
            <html>
                <head>
                    <link rel="canonical" href="https://another-example.com/canonical.html" />
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
            Some(Url::parse("https://another-example.com/canonical.html").unwrap())
        );

        assert_eq!(
            html.url(),
            &Url::parse("https://www.example.com/whatever").unwrap()
        );
    }

    #[test]
    fn recipe_first_ingredient_tag() {
        let html = Html::parse(
            r#"
            <html>
                <head>
                </head>
                <body>
                <script type="application/ld+json">
                {
                  "@context": "https://schema.org",
                  "@type": "Recipe",
                  "author": "John Smith",
                  "cookTime": "PT1H",
                  "datePublished": "2009-05-08",
                  "description": "This classic banana bread recipe comes from my mom -- the walnuts add a nice texture and flavor to the banana bread.",
                  "image": "bananabread.jpg",
                  "recipeIngredient": [
                    "3 or 4 ripe bananas, smashed",
                    "1 egg",
                    "3/4 cup of sugar"
                  ],
                  "interactionStatistic": {
                    "@type": "InteractionCounter",
                    "interactionType": "https://schema.org/Comment",
                    "userInteractionCount": "140"
                  },
                  "name": "Mom's World Famous Banana Bread",
                  "nutrition": {
                    "@type": "NutritionInformation",
                    "calories": "240 calories",
                    "fatContent": "9 grams fat"
                  },
                  "prepTime": "PT15M",
                  "recipeInstructions": "Preheat the oven to 350 degrees. Mix in the ingredients in a bowl. Add the flour last. Pour the mixture into a loaf pan and bake for one hour.",
                  "recipeYield": "1 loaf",
                  "suitableForDiet": "https://schema.org/LowFatDiet"
                }
                </script>

                <div id="ingredients">
                    <h2>Ingredients</h2>
                    <ul>
                        <li>3 or 4 ripe bananas, smashed</li>
                        <li>1 egg</li>
                        <li>3/4 cup of sugar</li>
                    </ul>
                </body>
            </html>
            "#,
            "https://www.example.com/",
        ).unwrap();

        let schemas = html.schema_org();

        assert_eq!(
            find_recipe_first_ingredient_tag_id(&schemas, &html.root),
            Some("ingredients".to_string())
        );
    }

    #[test]
    fn paywall() {
        let html = Html::parse(r##"
        <!DOCTYPE html>
<html lang="en" class="story nytapp-vi-article"  xmlns:og="http://opengraphprotocol.org/schema/">
  <head>
    <meta charset="utf-8" />
    <title data-rh="true">Russian Ships Near Data Cables Are Too Close for U.S. Comfort - The New York Times</title>
    <meta data-rh="true" name="robots" content="noarchive, max-image-preview:large"/><meta data-rh="true" name="description" content="Russian submarines are aggressively operating near the undersea cables that carry almost all global Internet communications, raising concerns in the U.S."/><meta data-rh="true" property="og:url" content="https://www.nytimes.com/2015/10/26/world/europe/russian-presence-near-undersea-cables-concerns-us.html"/><meta data-rh="true" property="og:type" content="article"/><meta data-rh="true" property="og:title" content="Russian Ships Near Data Cables Are Too Close for U.S. Comfort (Published 2015)"/><meta data-rh="true" property="og:image" content="https://static01.nyt.com/images/2015/10/26/world/JP-SUBS/JP-SUBS-facebookJumbo.jpg?year=2015&amp;h=550&amp;w=1050&amp;s=ceeb5555ae80dd2e2fe05553973d3948298bcba76c8d62a24907ae21e61140a7&amp;k=ZQJBKqZ0VN"/><meta data-rh="true" property="og:image:alt" content="Adm. Mark Ferguson said the intensity of Russian submarine patrols had risen by almost 50 percent over the last year."/><meta data-rh="true" property="og:description" content="Russian submarines are aggressively operating near the undersea cables that carry almost all global Internet communications, raising concerns in the U.S."/><meta data-rh="true" property="twitter:url" content="https://www.nytimes.com/2015/10/26/world/europe/russian-presence-near-undersea-cables-concerns-us.html"/><meta data-rh="true" property="twitter:title" content="Russian Ships Near Data Cables Are Too Close for U.S. Comfort (Published 2015)"/><meta data-rh="true" property="twitter:description" content="Russian submarines are aggressively operating near the undersea cables that carry almost all global Internet communications, raising concerns in the U.S."/><meta data-rh="true" property="twitter:image" content="https://static01.nyt.com/images/2015/10/26/world/JP-SUBS/JP-SUBS-articleLarge.jpg?year=2015&amp;h=400&amp;w=600&amp;s=1c25983f231db27a32cbc8063f067135d143de5f7c2d671b5798f0b76507c68f&amp;k=ZQJBKqZ0VN&amp;tw=1"/><meta data-rh="true" property="twitter:image:alt" content="Adm. Mark Ferguson said the intensity of Russian submarine patrols had risen by almost 50 percent over the last year."/><meta data-rh="true" property="twitter:card" content="summary_large_image"/> <link data-rh="true" rel="canonical" href="https://www.nytimes.com/2015/10/26/world/europe/russian-presence-near-undersea-cables-concerns-us.html"/><link data-rh="true" rel="alternate" href="nyt://article/a608af24-ea77-5f40-b5ee-e830ccd0f1fd"/><link data-rh="true" rel="alternate" type="application/json+oembed" href="https://www.nytimes.com/svc/oembed/json/?url=https%3A%2F%2Fwww.nytimes.com%2F2015%2F10%2F26%2Fworld%2Feurope%2Frussian-presence-near-undersea-cables-concerns-us.html" title="Russian Ships Near Data Cables Are Too Close for U.S. Comfort"/> <script data-rh="true" type="application/ld+json">{"@context":"http://schema.org","@type":"NewsArticle","description":"Russian submarines are aggressively operating near the undersea cables that carry almost all global Internet communications, raising concerns in the U.S.","image":{"@context":"http://schema.org","@type":"ImageObject","url":"https://static01.nyt.com/images/2015/10/26/world/JP-SUBS/JP-SUBS-superJumbo.jpg","height":1365,"width":2048,"contentUrl":"https://static01.nyt.com/images/2015/10/26/world/JP-SUBS/JP-SUBS-superJumbo.jpg","caption":"Adm. Mark Ferguson said the intensity of Russian submarine patrols had risen by almost 50 percent over the last year.","creditText":"Brendan Smialowski/Agence France-Presse — Getty Images"},"mainEntityOfPage":"https://www.nytimes.com/2015/10/26/world/europe/russian-presence-near-undersea-cables-concerns-us.html","url":"https://www.nytimes.com/2015/10/26/world/europe/russian-presence-near-undersea-cables-concerns-us.html","author":[{"@context":"http://schema.org","@type":"Person","url":"http://www.nytimes.com/by/david-e-sanger","name":"David E. Sanger"},{"@context":"http://schema.org","@type":"Person","url":"http://www.nytimes.com/by/eric-schmitt","name":"Eric Schmitt"}],"dateModified":"2015-10-25T17:02:34.000Z","datePublished":"2015-10-25T17:02:35.000Z","headline":"Russian Ships Near Data Cables Are Too Close for U.S. Comfort","alternativeHeadline":"Russian Ships Near Data Cables Are Too Close for U.S. Comfort","publisher":{"@id":"https://www.nytimes.com/#publisher","name":"The New York Times"},"hasPart":{"@type":"WebPageElement","isAccessibleForFree":false,"cssSelector":".meteredContent"},"copyrightHolder":{"@id":"https://www.nytimes.com/#publisher","name":"The New York Times"},"sourceOrganization":{"@id":"https://www.nytimes.com/#publisher","name":"The New York Times"},"copyrightYear":2024,"isAccessibleForFree":false,"isPartOf":{"@type":["CreativeWork","Product"],"name":"The New York Times","productID":"nytimes.com:basic"}}</script><script data-rh="true" type="application/ld+json">{"@context":"http://schema.org","@type":"NewsMediaOrganization","name":"The New York Times","logo":{"@context":"http://schema.org","@type":"ImageObject","url":"https://static01.nyt.com/images/icons/t_logo_291_black.png","height":291,"width":291,"contentUrl":"https://static01.nyt.com/images/icons/t_logo_291_black.png","creditText":"The New York Times"},"url":"https://www.nytimes.com/","@id":"https://www.nytimes.com/#publisher","diversityPolicy":"https://www.nytco.com/company/diversity-and-inclusion/","ethicsPolicy":"https://www.nytco.com/company/standards-ethics/","masthead":"https://www.nytimes.com/interactive/2023/01/28/admin/the-new-york-times-masthead.html","foundingDate":"1851-09-18","sameAs":"https://en.wikipedia.org/wiki/The_New_York_Times"}</script><script data-rh="true" type="application/ld+json">{"@context":"http://schema.org","@type":"BreadcrumbList","itemListElement":[{"@context":"http://schema.org","@type":"ListItem","name":"World","position":1,"item":"https://www.nytimes.com/section/world"},{"@context":"http://schema.org","@type":"ListItem","name":"Europe","position":2,"item":"https://www.nytimes.com/section/world/europe"}]}</script>
    <meta data-rh="true" property="article:published_time" content="2015-10-25T17:02:35.000Z"/><meta data-rh="true" property="article:modified_time" content="2015-10-25T17:02:34.000Z"/><meta data-rh="true" http-equiv="Content-Language" content="en"/><meta data-rh="true" name="articleid" content="100000003995797"/><meta data-rh="true" name="nyt_uri" content="nyt://article/a608af24-ea77-5f40-b5ee-e830ccd0f1fd"/><meta data-rh="true" name="pubp_event_id" content="pubp://event/7219283749a6486e8ec2777e0840ab41"/><meta data-rh="true" name="image" content="https://static01.nyt.com/images/2015/10/26/world/JP-SUBS/JP-SUBS-facebookJumbo.jpg?year=2015&amp;h=550&amp;w=1050&amp;s=ceeb5555ae80dd2e2fe05553973d3948298bcba76c8d62a24907ae21e61140a7&amp;k=ZQJBKqZ0VN"/><meta data-rh="true" name="byl" content="By David E. Sanger and Eric Schmitt"/><meta data-rh="true" name="news_keywords" content=",Russia,,Submarines and Submersibles,Wire and Cable,Computers and the Internet,Tech Industry,US,Homeland Security,Spying and Intelligence Agencies,US Military,Military,Mark Ferguson"/><meta data-rh="true" name="pdate" content="20151025"/><meta data-rh="true" property="article:section" content="World"/><meta data-rh="true" property="article:author" content="http://www.nytimes.com/by/david-e-sanger"/><meta data-rh="true" property="article:author" content="http://www.nytimes.com/by/eric-schmitt"/><meta data-rh="true" property="article:tag" content="Schmitt, Eric"/><meta data-rh="true" property="article:tag" content="Russia"/><meta data-rh="true" property="article:tag" content="Sanger, David E"/><meta data-rh="true" property="article:tag" content="Submarines and Submersibles"/><meta data-rh="true" property="article:tag" content="Wire and Cable"/><meta data-rh="true" property="article:tag" content="Computers and the Internet"/><meta data-rh="true" property="article:tag" content="United States"/><meta data-rh="true" property="article:tag" content="Homeland Security Department"/><meta data-rh="true" property="article:tag" content="Espionage and Intelligence Services"/><meta data-rh="true" property="article:tag" content="United States Defense and Military Forces"/><meta data-rh="true" property="article:tag" content="Defense and Military Forces"/><meta data-rh="true" property="article:tag" content="Ferguson, Mark E III"/><meta data-rh="true" property="article:opinion" content="false"/><meta data-rh="true" property="article:content_tier" content="metered"/><meta data-rh="true" name="CG" content="world"/><meta data-rh="true" name="SCG" content="europe"/><meta data-rh="true" name="CN" content=""/><meta data-rh="true" name="CT" content=""/><meta data-rh="true" name="PT" content="article"/><meta data-rh="true" name="PST" content="News"/><meta data-rh="true" name="url" content="https://www.nytimes.com/2015/10/26/world/europe/russian-presence-near-undersea-cables-concerns-us.html"/><meta data-rh="true" name="msapplication-starturl" content="https://www.nytimes.com"/><meta data-rh="true" property="al:android:url" content="nyt://article/a608af24-ea77-5f40-b5ee-e830ccd0f1fd"/><meta data-rh="true" property="al:android:package" content="com.nytimes.android"/><meta data-rh="true" property="al:android:app_name" content="NYTimes"/><meta data-rh="true" name="twitter:app:name:googleplay" content="NYTimes"/><meta data-rh="true" name="twitter:app:id:googleplay" content="com.nytimes.android"/><meta data-rh="true" name="twitter:app:url:googleplay" content="nyt://article/a608af24-ea77-5f40-b5ee-e830ccd0f1fd"/><meta data-rh="true" property="al:iphone:url" content="nytimes://www.nytimes.com/2015/10/26/world/europe/russian-presence-near-undersea-cables-concerns-us.html"/><meta data-rh="true" property="al:iphone:app_store_id" content="284862083"/><meta data-rh="true" property="al:iphone:app_name" content="NYTimes"/><meta data-rh="true" property="al:ipad:url" content="nytimes://www.nytimes.com/2015/10/26/world/europe/russian-presence-near-undersea-cables-concerns-us.html"/><meta data-rh="true" property="al:ipad:app_store_id" content="357066198"/><meta data-rh="true" property="al:ipad:app_name" content="NYTimes"/>
    
  </head>
  <body>
        TEST
  </body>
</html>
        "##, "https://www.example.com/whatever").unwrap();

        assert!(html.likely_has_paywall());
    }
}
