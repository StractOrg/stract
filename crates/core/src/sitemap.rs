// Stract is an open source web search engine.
// Copyright (C) 2024 Stract ApS
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

use chrono::{DateTime, Utc};
use quick_xml::events::Event;
use url::Url;

use crate::dated_url::DatedUrl;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SitemapEntry {
    Url(DatedUrl),
    Sitemap(Url),
}

pub fn parse_sitemap(s: &str) -> Vec<SitemapEntry> {
    let mut reader = quick_xml::Reader::from_str(s);

    let mut res = vec![];

    let mut in_sitemap = false;
    let mut in_url = false;
    let mut in_loc = false;
    let mut in_lastmod = false;

    let mut current_url: Option<Url> = None;
    let mut current_lastmod: Option<DateTime<Utc>> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                if e.name().as_ref() == b"sitemap" {
                    in_sitemap = true;
                } else if e.name().as_ref() == b"url" {
                    in_url = true;
                } else if e.name().as_ref() == b"loc" {
                    in_loc = true;
                } else if e.name().as_ref() == b"lastmod" {
                    in_lastmod = true;
                }
            }
            Ok(Event::End(ref e)) => {
                if e.name().as_ref() == b"sitemap" {
                    in_sitemap = false;
                } else if e.name().as_ref() == b"url" {
                    in_url = false;
                    if let Some(url) = current_url.take() {
                        res.push(SitemapEntry::Url(DatedUrl {
                            url,
                            last_modified: current_lastmod.take(),
                        }));
                    }
                } else if e.name().as_ref() == b"loc" {
                    in_loc = false;
                } else if e.name().as_ref() == b"lastmod" {
                    in_lastmod = false;
                }
            }
            Ok(Event::Text(e)) => {
                if in_sitemap && in_loc {
                    if let Ok(url) = Url::parse(&e.unescape().unwrap()) {
                        res.push(SitemapEntry::Sitemap(url));
                    }
                } else if in_url && in_loc {
                    if let Ok(url) = Url::parse(&e.unescape().unwrap()) {
                        current_url = Some(url);
                    }
                } else if in_url && in_lastmod {
                    if let Ok(date) = DateTime::parse_from_rfc3339(&e.unescape().unwrap()) {
                        current_lastmod = Some(date.with_timezone(&Utc));
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                tracing::debug!("failed to parse sitemap: {}", e);
                break;
            }
            _ => (),
        }
    }

    res
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sitemap() {
        let dr = r#"<sitemapindex>
        <sitemap>
        <loc>https://www.dr.dk/drtv/sitemap.xml</loc>
        </sitemap>
        <sitemap>
        <loc>https://www.dr.dk/sitemap.tvguide.xml</loc>
        </sitemap>
        <sitemap>
        <loc>
        https://www.dr.dk/sitemap.kommunalvalg.resultater.xml
        </loc>
        </sitemap>
        <sitemap>
        <loc>https://www.dr.dk/sitemap.folketingsvalg2022.xml</loc>
        </sitemap>
        </sitemapindex>"#;

        let entries = super::parse_sitemap(dr);
        assert_eq!(
            entries,
            vec![
                super::SitemapEntry::Sitemap("https://www.dr.dk/drtv/sitemap.xml".parse().unwrap()),
                super::SitemapEntry::Sitemap(
                    "https://www.dr.dk/sitemap.tvguide.xml".parse().unwrap()
                ),
                super::SitemapEntry::Sitemap(
                    "https://www.dr.dk/sitemap.kommunalvalg.resultater.xml"
                        .parse()
                        .unwrap()
                ),
                super::SitemapEntry::Sitemap(
                    "https://www.dr.dk/sitemap.folketingsvalg2022.xml"
                        .parse()
                        .unwrap()
                ),
            ]
        );

        let dr = r#"<urlset>
        <url>
        <lastmod>2023-10-18T05:40:04.7435930+00:00</lastmod>
        <loc>https://www.dr.dk/drtv/serie/sleepover_6382</loc>
        </url>
        <url>
        <lastmod>2023-10-18T05:40:04.7435930+00:00</lastmod>
        <loc>https://www.dr.dk/drtv/saeson/sleepover_9673</loc>
        </url>
        <url>
        <lastmod>2023-10-18T05:40:04.7435930+00:00</lastmod>
        <loc>
        https://www.dr.dk/drtv/episode/sleepover_-zoologisk-museum_52239
        </loc>
        </url>
        <url>
        <lastmod>2023-10-18T05:40:04.7435930+00:00</lastmod>
        <loc>
        https://www.dr.dk/drtv/episode/sleepover_-koebenhavns-raadhus_52252
        </loc>
        </url>
        </urlset>"#;

        let entries = super::parse_sitemap(dr);
        assert_eq!(
            entries,
            vec![
                super::SitemapEntry::Url(DatedUrl {
                    url: "https://www.dr.dk/drtv/serie/sleepover_6382"
                        .parse()
                        .unwrap(),
                    last_modified: Some(
                        "2023-10-18T05:40:04.7435930+00:00"
                            .parse::<DateTime<Utc>>()
                            .unwrap()
                    ),
                }),
                super::SitemapEntry::Url(DatedUrl {
                    url: "https://www.dr.dk/drtv/saeson/sleepover_9673"
                        .parse()
                        .unwrap(),
                    last_modified: Some(
                        "2023-10-18T05:40:04.7435930+00:00"
                            .parse::<DateTime<Utc>>()
                            .unwrap()
                    ),
                }),
                super::SitemapEntry::Url(DatedUrl {
                    url: "https://www.dr.dk/drtv/episode/sleepover_-zoologisk-museum_52239"
                        .parse()
                        .unwrap(),
                    last_modified: Some(
                        "2023-10-18T05:40:04.7435930+00:00"
                            .parse::<DateTime<Utc>>()
                            .unwrap()
                    ),
                }),
                super::SitemapEntry::Url(DatedUrl {
                    url: "https://www.dr.dk/drtv/episode/sleepover_-koebenhavns-raadhus_52252"
                        .parse()
                        .unwrap(),
                    last_modified: Some(
                        "2023-10-18T05:40:04.7435930+00:00"
                            .parse::<DateTime<Utc>>()
                            .unwrap()
                    ),
                }),
            ]
        );
    }
}
