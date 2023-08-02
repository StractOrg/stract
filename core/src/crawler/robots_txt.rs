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

use std::{collections::HashMap, panic, time::Duration};

use robotstxt_with_cache::matcher::{
    CachingRobotsMatcher, LongestMatchRobotsMatchStrategy, RobotsMatcher,
};

use url::Url;

use super::{Error, Result, Site};

enum Lookup<T> {
    Found(T),
    NotFound,
}

pub struct RobotsTxtManager {
    cache: HashMap<Site, Lookup<RobotsTxt>>,
    client: reqwest::Client,
    cache_expiration: Duration,
}

impl RobotsTxtManager {
    pub fn new(client: reqwest::Client, cache_expiration: Duration) -> Self {
        Self {
            client,
            cache_expiration,
            cache: HashMap::new(),
        }
    }

    pub async fn is_allowed(&mut self, url: &Url, user_agent: &str) -> bool {
        match self.get_mut(url).await {
            Lookup::Found(robots_txt) => robots_txt
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

    async fn get_mut(&mut self, url: &Url) -> &mut Lookup<RobotsTxt> {
        let site = Site(url.host_str().unwrap_or_default().to_string());

        let cache_should_update = match self.cache.get_mut(&site) {
            Some(Lookup::Found(robots_txt)) => robots_txt.is_expired(&self.cache_expiration),
            Some(Lookup::NotFound) => false,
            _ => true,
        };

        if cache_should_update {
            match self.fetch_robots_txt(&site).await {
                Ok(robots_txt) => {
                    self.cache.insert(site.clone(), Lookup::Found(robots_txt));
                }
                Err(err) => match err.downcast_ref() {
                    Some(Error::FetchFailed(status))
                        if *status == reqwest::StatusCode::IM_A_TEAPOT =>
                    {
                        self.cache.insert(site.clone(), Lookup::NotFound);
                    }
                    _ => {
                        self.cache.insert(site.clone(), Lookup::NotFound);
                        tracing::warn!("failed to fetch robots.txt for {}: {}", site.0, err);
                    }
                },
            }
        }

        self.cache.get_mut(&site).unwrap()
    }

    pub async fn sitemap(&mut self, url: &Url) -> Option<Url> {
        match self.get_mut(url).await {
            Lookup::Found(robotstxt) => robotstxt.sitemap.clone(),
            Lookup::NotFound => None,
        }
    }
}

struct RobotsTxt {
    download_time: std::time::Instant,
    matcher: CachingRobotsMatcher<LongestMatchRobotsMatchStrategy>,
    sitemap: Option<Url>,
}

impl RobotsTxt {
    fn new(body: String) -> Self {
        let mut s = Self {
            matcher: CachingRobotsMatcher::new(RobotsMatcher::default()),
            sitemap: None,
            download_time: std::time::Instant::now(),
        };

        s.update(body);

        s
    }

    fn is_expired(&self, expiration: &Duration) -> bool {
        self.download_time.elapsed() > *expiration
    }

    fn update(&mut self, body: String) {
        self.matcher.parse(&body);

        self.sitemap = body
            .to_ascii_lowercase()
            .lines()
            .find(|line| line.starts_with("sitemap:"))
            .map(|line| line.split(':').nth(1).unwrap().trim())
            .and_then(|s| Url::parse(s).ok());

        self.download_time = std::time::Instant::now();
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
