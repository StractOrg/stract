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

use crate::{config::CrawlerConfig, crawler};

use super::{encoded_body, Result, Site};

const RETRY_ROBOTSTXT_UNREACHABLE: bool = false;

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
    min_crawl_delay: Duration,
    max_crawl_delay: Duration,
}

impl RobotsTxtManager {
    pub fn new(config: &CrawlerConfig) -> Self {
        let client = crawler::reqwest_client(config).unwrap();
        let cache_expiration = Duration::from_secs(config.robots_txt_cache_sec);
        let user_agent = config.user_agent.token.clone();
        let min_crawl_delay = Duration::from_millis(config.min_crawl_delay_ms);
        let max_crawl_delay = Duration::from_millis(config.max_crawl_delay_ms);

        Self {
            client,
            cache_expiration,
            last_prune: std::time::Instant::now(),
            cache: BTreeMap::new(),
            user_agent: user_agent.to_string(),
            min_crawl_delay,
            max_crawl_delay,
        }
    }

    pub async fn is_allowed(&mut self, url: &Url) -> bool {
        match self.get_mut(url).await {
            Lookup::Found(robots_txt) => robots_txt.is_allowed(url),
            Lookup::Unavailable => true,
            Lookup::Unreachable => false,
        }
    }

    pub async fn crawl_delay(&mut self, url: &Url) -> Option<Duration> {
        match self.get_mut(url).await {
            Lookup::Found(robots_txt) => robots_txt.robots.crawl_delay(),
            Lookup::Unavailable | Lookup::Unreachable => None,
        }
    }

    async fn fetch_robots_txt_from_url(&self, url: &str) -> Lookup<RobotsTxt> {
        let res = match self
            .client
            .get(url)
            .timeout(Duration::from_secs(60))
            .send()
            .await
        {
            Ok(res) => {
                if res.status() != reqwest::StatusCode::OK {
                    match res.status() {
                        reqwest::StatusCode::NOT_FOUND => return Lookup::Unavailable,
                        _ => return Lookup::Unreachable,
                    }
                }

                let body = match encoded_body(res).await {
                    Ok(body) => body,
                    Err(_) => return Lookup::Unreachable,
                };

                let self_user_agent = self.user_agent.clone();
                match panic::catch_unwind(|| RobotsTxt::new(&self_user_agent, body)) {
                    Ok(Ok(r)) => Lookup::Found(r),
                    _ => Lookup::Unreachable,
                }
            }
            Err(_) => Lookup::Unreachable,
        };

        tokio::time::sleep(self.min_crawl_delay).await;

        res
    }

    async fn fetch_robots_txt_without_retry(&self, site: &Site) -> Lookup<RobotsTxt> {
        match self
            .fetch_robots_txt_from_url(&format!("http://{}/robots.txt", site.0))
            .await
        {
            Lookup::Unavailable => {
                match self
                    .fetch_robots_txt_from_url(&format!("https://{}/robots.txt", site.0))
                    .await
                {
                    Lookup::Found(robots_txt) => Lookup::Found(robots_txt),
                    Lookup::Unreachable => Lookup::Unreachable,
                    Lookup::Unavailable
                        if !site.0.starts_with("www.")
                            && site.0.chars().filter(|&c| c == '.').count() == 1 =>
                    {
                        self.fetch_robots_txt_from_url(&format!(
                            "https://www.{}/robots.txt",
                            &site.0
                        ))
                        .await
                    }
                    Lookup::Unavailable => Lookup::Unavailable,
                }
            }
            res => res,
        }
    }

    async fn fetch_robots_txt(&self, site: &Site) -> Lookup<RobotsTxt> {
        if !RETRY_ROBOTSTXT_UNREACHABLE {
            return self.fetch_robots_txt_without_retry(site).await;
        }

        for _ in 0..3 {
            match self.fetch_robots_txt_without_retry(site).await {
                Lookup::Found(robots_txt) => return Lookup::Found(robots_txt),
                Lookup::Unavailable => return Lookup::Unavailable,
                Lookup::Unreachable => {}
            }

            tokio::time::sleep(self.max_crawl_delay).await;
        }

        Lookup::Unreachable
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
            Some(Lookup::Unavailable) | Some(Lookup::Unreachable) => false,
            None => true,
        };

        if cache_should_update {
            self.cache
                .insert(site.clone(), self.fetch_robots_txt(&site).await);
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
