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

use std::{collections::BTreeMap, panic, time::Duration};

use url::Url;

use super::{Error, Result, Site};

enum Lookup<T> {
    Found(T),
    /// 404
    Unavailable,
    /// 5xx
    Unreachable,
}

pub struct RobotsTxtManager {
    cache: BTreeMap<Site, Lookup<RobotsTxt>>,
    last_prune: std::time::Instant,
    client: reqwest::Client,
    cache_expiration: Duration,
    user_agent: String,
}

impl RobotsTxtManager {
    pub fn new(client: reqwest::Client, cache_expiration: Duration, user_agent: &str) -> Self {
        Self {
            client,
            cache_expiration,
            last_prune: std::time::Instant::now(),
            cache: BTreeMap::new(),
            user_agent: user_agent.to_string(),
        }
    }

    pub async fn is_allowed(&mut self, url: &Url) -> bool {
        match self.get_mut(url).await {
            Lookup::Found(robots_txt) => robots_txt.is_allowed(url),
            Lookup::Unavailable => true,
            Lookup::Unreachable => false,
        }
    }

    async fn fetch_robots_txt_from_url(&self, url: &str) -> Result<RobotsTxt> {
        let res = self
            .client
            .get(url)
            .timeout(Duration::from_secs(60))
            .send()
            .await;

        let res = res?;

        if res.status() != reqwest::StatusCode::OK {
            return Err(Error::FetchFailed(res.status()).into());
        }

        if !res
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .map(|h| h.to_str().unwrap_or_default().starts_with("text/plain"))
            .unwrap_or(false)
        {
            return Err(Error::FetchFailed(reqwest::StatusCode::IM_A_TEAPOT).into());
        }

        let body = res.text().await?;

        let self_user_agent = self.user_agent.clone();
        match panic::catch_unwind(|| RobotsTxt::new(&self_user_agent, body)) {
            Ok(Ok(r)) => Ok(r),
            _ => Err(Error::FetchFailed(reqwest::StatusCode::IM_A_TEAPOT).into()),
        }
    }

    async fn fetch_robots_txt(&self, site: &Site) -> Result<RobotsTxt> {
        if let Ok(robots_txt) = self
            .fetch_robots_txt_from_url(&format!("http://{}/robots.txt", site.0))
            .await
        {
            return Ok(robots_txt);
        }

        match self
            .fetch_robots_txt_from_url(&format!("https://{}/robots.txt", site.0))
            .await
        {
            Ok(robots_txt) => Ok(robots_txt),
            _ if !site.0.starts_with("www.")
                && site.0.chars().filter(|&c| c == '.').count() == 1 =>
            {
                self.fetch_robots_txt_from_url(&format!("https://www.{}/robots.txt", &site.0))
                    .await
            }
            Err(err) => Err(err),
        }
    }

    fn maybe_prune(&mut self) {
        if self.last_prune.elapsed() < Duration::from_secs(60) {
            return;
        }

        self.cache.retain(|_, v| match v {
            Lookup::Found(robots_txt) => !robots_txt.is_expired(&self.cache_expiration),
            _ => true,
        });

        self.last_prune = std::time::Instant::now();
    }

    async fn get_mut(&mut self, url: &Url) -> &mut Lookup<RobotsTxt> {
        self.maybe_prune();
        let site = Site(url.host_str().unwrap_or_default().to_string());

        let cache_should_update = match self.cache.get_mut(&site) {
            Some(Lookup::Found(robots_txt)) => robots_txt.is_expired(&self.cache_expiration),
            Some(Lookup::Unavailable) => false,
            _ => true,
        };

        if cache_should_update {
            match self.fetch_robots_txt(&site).await {
                Ok(robots_txt) => {
                    self.cache.insert(site.clone(), Lookup::Found(robots_txt));
                }
                Err(err) => match err.downcast_ref() {
                    Some(Error::FetchFailed(status))
                        if *status == reqwest::StatusCode::NOT_FOUND =>
                    {
                        self.cache.insert(site.clone(), Lookup::Unavailable);
                    }
                    _ => {
                        self.cache.insert(site.clone(), Lookup::Unreachable);
                        tracing::warn!("failed to fetch robots.txt for {}: {}", site.0, err);
                    }
                },
            }
        }

        self.cache.get_mut(&site).unwrap()
    }

    pub async fn sitemaps(&mut self, url: &Url) -> Vec<Url> {
        match self.get_mut(url).await {
            Lookup::Found(robotstxt) => robotstxt
                .sitemaps()
                .iter()
                .filter_map(|s| Url::parse(s).ok())
                .collect(),
            Lookup::Unavailable => vec![],
            Lookup::Unreachable => vec![],
        }
    }
}

struct RobotsTxt {
    download_time: std::time::Instant,
    robots: robotstxt::Robots,
}

impl RobotsTxt {
    fn new(user_agent: &str, body: String) -> Result<Self> {
        Ok(Self {
            robots: robotstxt::Robots::parse(user_agent, &body)?,
            download_time: std::time::Instant::now(),
        })
    }

    fn is_expired(&self, expiration: &Duration) -> bool {
        self.download_time.elapsed() > *expiration
    }

    fn is_allowed(&self, url: &Url) -> bool {
        self.robots.is_allowed(url)
    }

    fn sitemaps(&self) -> &[String] {
        self.robots.sitemaps()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let ua_token = "StractBot";
        let robots_txt = RobotsTxt::new(
            ua_token,
            r#"User-agent: StractBot
            Disallow: /test"#
                .to_string(),
        )
        .unwrap();

        assert!(!robots_txt.is_allowed(&Url::parse("http://example.com/test").unwrap()));
        assert!(robots_txt.is_allowed(&Url::parse("http://example.com/example").unwrap()));
    }

    #[test]
    fn lowercase() {
        let ua_token = "StractBot";
        let robots_txt = RobotsTxt::new(
            ua_token,
            r#"User-agent: stractbot
            Disallow: /test"#
                .to_string(),
        )
        .unwrap();

        assert!(!robots_txt.is_allowed(&Url::parse("http://example.com/test").unwrap()));
        assert!(robots_txt.is_allowed(&Url::parse("http://example.com/example").unwrap()));
    }

    #[test]
    fn test_extra_newline() {
        let ua_token = "StractBot";
        let robots_txt = RobotsTxt::new(
            ua_token,
            r#"User-agent: StractBot


            Disallow: /test"#
                .to_string(),
        )
        .unwrap();

        assert!(!robots_txt.is_allowed(&Url::parse("http://example.com/test").unwrap()));
        assert!(robots_txt.is_allowed(&Url::parse("http://example.com/example").unwrap()));
    }

    #[test]
    fn test_multiple_agents() {
        let ua_token = "StractBot";

        let robots_txt = RobotsTxt::new(
            ua_token,
            r#"User-Agent: GoogleBot
User-Agent: StractBot
Disallow: /

User-Agent: *
Allow: /"#
                .to_string(),
        )
        .unwrap();

        assert!(!robots_txt.is_allowed(&Url::parse("http://example.com/test").unwrap()));

        let ua_token = "StractBot";

        let robots_txt = RobotsTxt::new(
            ua_token,
            r#"User-Agent: GoogleBot, StractBot
Disallow: /

User-Agent: *
Allow: /"#
                .to_string(),
        )
        .unwrap();

        assert!(!robots_txt.is_allowed(&Url::parse("http://example.com/test").unwrap()));
    }

    #[test]
    fn test_sitemap() {
        let ua_token = "StractBot";
        let robots_txt = RobotsTxt::new(
            ua_token,
            r#"User-agent: *
Disallow: /test

Sitemap: http://example.com/sitemap.xml"#
                .to_string(),
        )
        .unwrap();

        assert_eq!(robots_txt.sitemaps(), &["http://example.com/sitemap.xml"]);

        let robots_txt = RobotsTxt::new(
            ua_token,
            r#"User-agent: *
Disallow: /test

SiTeMaP: http://example.com/sitemap.xml"#
                .to_string(),
        )
        .unwrap();

        assert_eq!(robots_txt.sitemaps(), &["http://example.com/sitemap.xml"]);
    }

    #[test]
    fn wildcard() {
        let ua_token = "StractBot";

        let robots_txt = RobotsTxt::new(
            ua_token,
            r#"User-agent: StractBot
Disallow: /test/*
"#
            .to_string(),
        )
        .unwrap();

        assert!(!robots_txt.is_allowed(&Url::parse("http://example.com/test/").unwrap()));
        assert!(!robots_txt.is_allowed(&Url::parse("http://example.com/test/foo").unwrap()));
        assert!(!robots_txt.is_allowed(&Url::parse("http://example.com/test/foo/bar").unwrap()));
        assert!(robots_txt.is_allowed(&Url::parse("http://example.com/test").unwrap()));
        assert!(robots_txt.is_allowed(&Url::parse("http://example.com/testfoo").unwrap()));

        let robots_txt = RobotsTxt::new(
            ua_token,
            r#"User-agent: StractBot
    Disallow: /test/*/bar
    "#
            .to_string(),
        )
        .unwrap();

        assert!(robots_txt.is_allowed(&Url::parse("http://example.com/test/").unwrap()));
        assert!(robots_txt.is_allowed(&Url::parse("http://example.com/test/foo").unwrap()));
        assert!(!robots_txt.is_allowed(&Url::parse("http://example.com/test/foo/bar").unwrap()));
        assert!(!robots_txt.is_allowed(&Url::parse("http://example.com/test/foo/baz/bar").unwrap()));
        assert!(robots_txt.is_allowed(&Url::parse("http://example.com/test").unwrap()));
        assert!(robots_txt.is_allowed(&Url::parse("http://example.com/testfoo").unwrap()));
    }
}
