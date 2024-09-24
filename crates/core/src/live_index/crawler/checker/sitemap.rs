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

use url::Url;

use crate::dated_url::DatedUrl;
use crate::sitemap::{parse_sitemap, SitemapEntry};
use crate::Result;
use crate::{entrypoint::site_stats, webpage::url_ext::UrlExt};

use super::{CheckIntervals, Checker, CrawlableUrl};

const MAX_SITEMAP_DEPTH: usize = 10;
const SITEMAP_DELAY: Duration = Duration::from_secs(10);

pub struct Sitemap {
    robots_txt: Url,
    last_check: std::time::Instant,
    client: reqwest::Client,
}

impl Sitemap {
    pub fn new(site: &site_stats::Site, client: reqwest::Client) -> Result<Self> {
        let robots_txt = Url::robust_parse(&format!("{}/robots.txt", site.as_str()))?;

        Ok(Self {
            robots_txt,
            last_check: std::time::Instant::now(),
            client,
        })
    }

    async fn sitemap_urls(&self) -> Result<Vec<Url>> {
        let res = self.client.get(self.robots_txt.clone()).send().await?;
        let body = res.text().await?;

        // wildcard useragent is okay as we only use it to check for sitemap directive
        let robots = robotstxt::Robots::parse("*", &body)?;

        Ok(robots
            .sitemaps()
            .iter()
            .filter_map(|s| Url::parse(s).ok())
            .collect())
    }

    async fn urls_from_sitemap(&self, sitemap: Url) -> Vec<DatedUrl> {
        let mut stack = vec![(sitemap, 0)];
        let mut urls = vec![];

        while let Some((url, depth)) = stack.pop() {
            if depth >= MAX_SITEMAP_DEPTH {
                continue;
            }

            let res = self.client.get(url).send().await;
            tokio::time::sleep(SITEMAP_DELAY).await;

            if res.is_err() {
                continue;
            }

            let res = res.unwrap();

            if res.status() != reqwest::StatusCode::OK {
                continue;
            }

            let body = res.text().await;

            if body.is_err() {
                continue;
            }

            let body = body.unwrap();

            let entries = parse_sitemap(&body);

            for entry in entries {
                match entry {
                    SitemapEntry::Url(url) => {
                        urls.push(url);
                    }
                    SitemapEntry::Sitemap(url) => {
                        stack.push((url, depth + 1));
                    }
                }
            }
        }

        urls
    }
}

impl Checker for Sitemap {
    async fn get_urls(&mut self) -> Result<Vec<CrawlableUrl>> {
        let sitemap_urls = self.sitemap_urls().await?;
        let mut urls = vec![];

        for sitemap_url in sitemap_urls {
            urls.extend(
                self.urls_from_sitemap(sitemap_url)
                    .await
                    .into_iter()
                    .map(CrawlableUrl::from),
            );
        }

        self.last_check = std::time::Instant::now();

        Ok(urls)
    }

    fn should_check(&self, interval: &CheckIntervals) -> bool {
        self.last_check.elapsed() > interval.sitemap
    }
}
