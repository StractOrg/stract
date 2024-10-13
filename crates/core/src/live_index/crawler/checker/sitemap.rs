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

use crate::config::CrawlerConfig;
use crate::crawler::robot_client::RobotClient;
use crate::dated_url::DatedUrl;
use crate::sitemap::{parse_sitemap, SitemapEntry};
use crate::Result;
use crate::{entrypoint::site_stats, webpage::url_ext::UrlExt};

use super::{CheckIntervals, Checker, CrawlableUrl};

const MAX_SITEMAP_DEPTH: usize = 10;
const SITEMAP_DELAY: Duration = Duration::from_secs(60);

pub struct Sitemap {
    robots_url: Url,
    last_check: std::time::Instant,
    client: RobotClient,
}

impl Sitemap {
    pub fn new(site: &site_stats::Site, config: &CrawlerConfig) -> Result<Self> {
        let robots_url = Url::robust_parse(&format!("{}/robots.txt", site.as_str()))?;

        Ok(Self {
            robots_url,
            last_check: std::time::Instant::now(),
            client: RobotClient::new(config)?,
        })
    }

    async fn sitemap_urls(&self) -> Vec<Url> {
        self.client
            .robots_txt_manager()
            .sitemaps(&self.robots_url)
            .await
    }

    async fn urls_from_sitemap(&self, sitemap: Url) -> Vec<DatedUrl> {
        let mut stack = vec![(sitemap, 0)];
        let mut urls = vec![];

        while let Some((url, depth)) = stack.pop() {
            if depth >= MAX_SITEMAP_DEPTH {
                continue;
            }

            let Ok(req) = self.client.get(url).await else {
                continue;
            };
            let res = req.send().await;
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
    async fn get_urls(&self) -> Result<Vec<CrawlableUrl>> {
        let sitemap_urls = self.sitemap_urls().await;
        let mut urls = vec![];

        for sitemap_url in sitemap_urls {
            urls.extend(
                self.urls_from_sitemap(sitemap_url)
                    .await
                    .into_iter()
                    .map(CrawlableUrl::from),
            );
        }

        Ok(urls)
    }

    fn update_last_check(&mut self) {
        self.last_check = std::time::Instant::now();
    }

    fn should_check(&self, interval: &CheckIntervals) -> bool {
        self.last_check.elapsed() > interval.sitemap
    }
}
