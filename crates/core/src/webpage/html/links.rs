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

use kuchiki::iter::NodeEdge;
use url::Url;

use crate::webpage::{url_ext::UrlExt, Link};

use super::Html;

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

impl Html {
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

    pub fn link_density(&self) -> f64 {
        (1.0 + self.anchor_links().len() as f64)
            / (1.0
                + self
                    .clean_text
                    .as_ref()
                    .map(|s| s.len())
                    .unwrap_or_default() as f64)
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
