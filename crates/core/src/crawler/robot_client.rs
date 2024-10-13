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

use super::{Error, Result};
use crate::config::CrawlerConfig;
use crate::crawler::robots_txt::RobotsTxtManager;
use anyhow::anyhow;
use std::time::Duration;
use url::Url;

pub(super) fn reqwest_client(config: &CrawlerConfig) -> Result<reqwest::Client> {
    let timeout = Duration::from_secs(config.timeout_seconds);

    let mut headers = reqwest::header::HeaderMap::default();
    headers.insert(
        reqwest::header::ACCEPT,
        reqwest::header::HeaderValue::from_static("text/html"),
    );
    headers.insert(
        reqwest::header::ACCEPT_LANGUAGE,
        reqwest::header::HeaderValue::from_static("en-US,en;q=0.9,*;q=0.8"),
    );

    reqwest::Client::builder()
        .timeout(timeout)
        .connect_timeout(timeout)
        .http2_keep_alive_interval(None)
        .default_headers(headers)
        .redirect(reqwest::redirect::Policy::limited(0))
        .user_agent(&config.user_agent.full)
        .build()
        .map_err(|e| Error::from(anyhow!(e)))
}

#[derive(Clone)]
pub struct RobotClient {
    robots_txt_manager: RobotsTxtManager,
    client: reqwest::Client,
}

impl RobotClient {
    pub fn new(config: &CrawlerConfig) -> Result<Self> {
        Ok(Self {
            client: reqwest_client(config)?,
            robots_txt_manager: RobotsTxtManager::new(config),
        })
    }

    pub fn robots_txt_manager(&self) -> &RobotsTxtManager {
        &self.robots_txt_manager
    }

    pub async fn get(&self, url: Url) -> Result<reqwest::RequestBuilder> {
        if !self.robots_txt_manager.is_allowed(&url).await {
            return Err(Error::DisallowedPath);
        }

        Ok(self.client.get(url))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_errs_disallowed_path() {
        let config = CrawlerConfig::for_tests();
        let client = RobotClient::new(&config).unwrap();

        let robots_txt =
            robotstxt::Robots::parse("TestBot", "User-agent: *\nDisallow: /test\nAllow: /example")
                .unwrap();

        client
            .robots_txt_manager()
            .insert("example.com".to_string(), robots_txt);

        let url = Url::parse("http://example.com/test").unwrap();
        assert!(client.get(url).await.is_err());

        let url = Url::parse("http://example.com/example").unwrap();
        assert!(client.get(url).await.is_ok());
    }
}
