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

use std::time::Duration;

use crate::feed::{parse, Feed};
use crate::Result;

use super::{CheckIntervals, Checker, CrawlableUrl};

const CRAWL_DELAY: Duration = Duration::from_secs(5);

pub struct Feeds {
    feeds: Vec<Feed>,
    last_check: std::time::Instant,
    client: reqwest::Client,
}

impl Feeds {
    pub fn new(feeds: Vec<Feed>, client: reqwest::Client) -> Self {
        Self {
            feeds,
            last_check: std::time::Instant::now(),
            client,
        }
    }
}

impl Checker for Feeds {
    async fn get_urls(&mut self) -> Result<Vec<CrawlableUrl>> {
        let mut urls = Vec::new();

        for feed in &self.feeds {
            let resp = self.client.get(feed.url.clone()).send().await?;
            let text = resp.text().await?;
            let parsed_feed = parse(&text, feed.kind)?;

            for link in parsed_feed.links {
                urls.push(CrawlableUrl::from(link));
            }

            tokio::time::sleep(CRAWL_DELAY).await;
        }

        Ok(urls)
    }

    fn update_last_check(&mut self) {
        self.last_check = std::time::Instant::now();
    }

    fn should_check(&self, interval: &CheckIntervals) -> bool {
        self.last_check.elapsed() > interval.feeds
    }
}
