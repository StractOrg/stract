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

use anyhow::{anyhow, Result};
use quick_xml::events::Event;
use url::Url;

use super::{FeedKind, ParsedFeed};

fn parse_rss(feed: &str) -> Result<ParsedFeed> {
    let mut reader = quick_xml::Reader::from_str(feed);

    let mut buf = Vec::new();
    let mut links = Vec::new();
    let mut inside_link = false;
    let mut inside_item = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) if e.name().as_ref() == b"link" => {
                inside_link = true;
            }
            Ok(Event::End(ref e)) if e.name().as_ref() == b"link" => {
                inside_link = false;
            }
            Ok(Event::Start(ref e)) if e.name().as_ref() == b"item" => {
                inside_item = true;
            }
            Ok(Event::End(ref e)) if e.name().as_ref() == b"item" => {
                inside_item = false;
            }
            Ok(Event::Text(e)) if inside_item && inside_link => {
                let link = e.unescape()?;
                if let Ok(link) = Url::parse(&link) {
                    links.push(link);
                }
            }
            Err(e) => {
                tracing::warn!("Error parsing feed: {}", e);
                break;
            }
            Ok(Event::Eof) => break,
            _ => {}
        }
    }

    Ok(ParsedFeed { links })
}

fn parse_atom(feed: &str) -> Result<ParsedFeed> {
    let mut reader = quick_xml::Reader::from_str(feed);

    let mut buf = Vec::new();
    let mut links = Vec::new();

    let mut inside_entry = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) if e.name().as_ref() == b"entry" => {
                inside_entry = true;
            }
            Ok(Event::End(ref e)) if e.name().as_ref() == b"entry" => {
                inside_entry = false;
            }
            Ok(Event::Start(ref e)) | Ok(Event::Empty(ref e)) => {
                if inside_entry && e.name().as_ref() == b"link" {
                    if let Some(Ok(href)) = e
                        .attributes()
                        .filter(std::result::Result::is_ok)
                        .find(|attr| attr.as_ref().unwrap().key.as_ref() == b"href")
                    {
                        dbg!(&href);
                        if let Ok(href) = href
                            .unescape_value()
                            .map_err(|e| anyhow!(e))
                            .and_then(|v| Url::parse(&v).map_err(|e| anyhow!(e)))
                        {
                            links.push(href);
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Error parsing feed: {}", e);
                break;
            }
            Ok(Event::Eof) => break,
            _ => {}
        }
    }

    Ok(ParsedFeed { links })
}

pub fn parse(feed: &str, kind: FeedKind) -> Result<ParsedFeed> {
    // remember to only crawl urls that are not already in the index
    // and on the same root_domain as the feed.
    match kind {
        FeedKind::Atom => parse_atom(feed),
        FeedKind::Rss => parse_rss(feed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_rss() {
        let feed = r#"
        <?xml version="1.0" encoding="UTF-8"?>
        <rss xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:content="http://purl.org/rss/1.0/modules/content/" version="2.0">
            <channel>
                <title>Test title</title>
                <description>this is a description</description>
                <link>https://example.com/</link>
                <lastBuildDate>Mon, 30 Oct 2023 08:59:01 GMT</lastBuildDate>
                <item>
                    <title>First title</title>
                    <description>First desc></description>
                    <link>https://example.com/a</link>
                    <pubDate>Mon, 30 Oct 2023 08:55:00 GMT</pubDate>
                </item>
            </channel>
        </rss>
        "#;

        let parsed = parse_rss(feed).unwrap();

        assert_eq!(
            parsed.links,
            vec![Url::parse("https://example.com/a").unwrap()]
        );
    }

    #[test]
    fn test_parse_atom() {
        let feed = r#"
        <?xml version="1.0" encoding="UTF-8"?>
        <feed xmlns="http://www.w3.org/2005/Atom">
        <title>Example Feed</title>
        <entry>
            <link href="https://example.com/a"/>
            <updated>2003-12-13T18:30:02Z</updated>
        </entry>
        </feed>
        "#;

        let parsed = parse_atom(feed).unwrap();

        assert_eq!(
            parsed.links,
            vec![Url::parse("https://example.com/a").unwrap()]
        );
    }
}
