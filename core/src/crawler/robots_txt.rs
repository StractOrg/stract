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

use std::{collections::HashMap, panic};

use robotstxt_with_cache::matcher::{
    CachingRobotsMatcher, LongestMatchRobotsMatchStrategy, RobotsMatcher,
};

use url::Url;

use super::{Error, Result, Site};

pub struct RobotsTxtManager {
    cache: HashMap<Site, Option<RobotsTxt>>, // None if robots.txt does not exist
    client: reqwest::Client,
}

impl RobotsTxtManager {
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            cache: HashMap::new(),
        }
    }

    pub async fn is_allowed(&mut self, url: &Url, user_agent: &str) -> bool {
        match self.get_mut(url).await {
            Ok(Some(robots_txt)) => robots_txt
                .matcher
                .one_agent_allowed_by_robots(user_agent, url.as_str()),
            _ => true,
        }
    }

    async fn fetch_robots_txt(&self, site: &Site) -> Result<RobotsTxt> {
        let res = self
            .client
            .get(&format!("http://{}/robots.txt", site.0))
            .send()
            .await?;

        if res.status() != reqwest::StatusCode::OK {
            return Err(Error::FetchFailed(res.status()).into());
        }

        let body = res.text().await?;

        match panic::catch_unwind(|| RobotsTxt::new(body)) {
            Ok(r) => Ok(r),
            Err(_) => Err(Error::FetchFailed(reqwest::StatusCode::IM_A_TEAPOT).into()),
        }
    }

    async fn get_mut(&mut self, url: &Url) -> Result<Option<&mut RobotsTxt>> {
        let site = Site(url.host_str().unwrap_or_default().to_string());

        if self.cache.get(&site).is_none() {
            match self.fetch_robots_txt(&site).await {
                Ok(robots_txt) => {
                    self.cache.insert(site.clone(), Some(robots_txt));
                }
                Err(err) => match err.downcast_ref() {
                    Some(Error::FetchFailed(status))
                        if *status == reqwest::StatusCode::IM_A_TEAPOT =>
                    {
                        self.cache.insert(site.clone(), None);
                    }
                    _ => {
                        self.cache.insert(site.clone(), None);
                        tracing::warn!("failed to fetch robots.txt for {}: {}", site.0, err);
                    }
                },
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
    matcher: CachingRobotsMatcher<LongestMatchRobotsMatchStrategy>,
    sitemap: Option<Url>,
}

impl RobotsTxt {
    fn new(body: String) -> Self {
        let mut matcher = CachingRobotsMatcher::new(RobotsMatcher::default());

        matcher.parse(&body);

        let sitemap = body
            .to_ascii_lowercase()
            .lines()
            .find(|line| line.starts_with("sitemap:"))
            .map(|line| line.split(':').nth(1).unwrap().trim())
            .and_then(|s| Url::parse(s).ok());

        Self { matcher, sitemap }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let ua_token = "StractSearch";
        let mut robots_txt = RobotsTxt::new(
            r#"User-agent: StractSearch
            Disallow: /test"#
                .to_string(),
        );

        assert!(!robots_txt
            .matcher
            .one_agent_allowed_by_robots(ua_token, "http://example.com/test"));
        assert!(robots_txt
            .matcher
            .one_agent_allowed_by_robots(ua_token, "http://example.com/example"));
    }
}
