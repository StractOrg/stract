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

use std::{collections::HashMap, time::Instant};

use robotstxt_with_cache::matcher::{
    CachingRobotsMatcher, LongestMatchRobotsMatchStrategy, RobotsMatcher,
};

use crate::webpage::Url;

use super::{Error, Result, Site};

pub struct RobotsTxtManager {
    cache: HashMap<Site, Option<RobotsTxt>>, // None if robots.txt does not exist
}

impl RobotsTxtManager {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    pub async fn is_allowed(&mut self, url: &Url, user_agent: &str) -> bool {
        match self.get_mut(url).await {
            Ok(Some(robots_txt)) => robots_txt
                .matcher
                .one_agent_allowed_by_robots(user_agent, &url.full()),
            _ => true,
        }
    }

    async fn fetch_robots_txt(&self, site: &Site) -> Result<RobotsTxt> {
        let res = reqwest::get(&format!("https://{}/robots.txt", site.0)).await?;

        if res.status() != reqwest::StatusCode::OK {
            return Err(Error::FetchFailed(res.status()));
        }

        let body = res.text().await?;

        RobotsTxt::new(body)
    }

    async fn get_mut(&mut self, url: &Url) -> Result<Option<&mut RobotsTxt>> {
        let site = Site(url.site().to_string());

        let should_fetch = match self.cache.get(&site) {
            Some(Some(robots_txt)) => Instant::now() > robots_txt.valid_until,
            Some(None) => false, // robots.txt does not exist
            None => true,
        };

        if should_fetch {
            match self.fetch_robots_txt(&site).await {
                Ok(robots_txt) => {
                    self.cache.insert(site.clone(), Some(robots_txt));
                }
                Err(Error::FetchFailed(status)) if status == reqwest::StatusCode::NOT_FOUND => {
                    self.cache.insert(site.clone(), None);
                }
                Err(err) => {
                    tracing::warn!("failed to fetch robots.txt for {}: {}", site.0, err);
                }
            }
        }

        match self.cache.get_mut(&site) {
            Some(Some(robot)) => Ok(Some(robot)),
            _ => Ok(None),
        }
    }

    pub async fn sitemap(&mut self, url: &Url) -> Result<Option<Url>> {
        Ok(self
            .get_mut(url)
            .await?
            .and_then(|robots_txt| robots_txt.sitemap.clone()))
    }
}

struct RobotsTxt {
    valid_until: Instant,
    matcher: CachingRobotsMatcher<LongestMatchRobotsMatchStrategy>,
    sitemap: Option<Url>,
}

impl RobotsTxt {
    fn new(body: String) -> Result<Self> {
        let mut matcher = CachingRobotsMatcher::new(RobotsMatcher::default());

        matcher.parse(&body);

        let sitemap = body
            .to_ascii_lowercase()
            .lines()
            .find(|line| line.starts_with("sitemap:"))
            .map(|line| line.split(':').nth(1).unwrap().trim().to_string())
            .map(Url::from);

        Ok(Self {
            valid_until: Instant::now() + chrono::Duration::hours(24).to_std()?,
            matcher,
            sitemap,
        })
    }
}
