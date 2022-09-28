// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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

use std::{fmt::Display, time::Duration};

use serde::{Deserialize, Serialize};
use tracing::debug;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Hash)]
pub struct Url(String);

impl Display for Url {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0)
    }
}

impl From<String> for Url {
    fn from(url: String) -> Self {
        Url(url)
    }
}

impl Url {
    pub fn strip_protocol(&self) -> &str {
        let mut start_host = 0;
        let url = &self.0;
        if url.starts_with("http://") || url.starts_with("https://") || url.starts_with("//") {
            start_host = url
                .find('/')
                .expect("It was checked that url starts with protocol");
            start_host += 2; // skip the two '/'
        }

        &url[start_host..]
    }

    pub fn strip_query(&self) -> &str {
        let url = &self.0;
        let mut start_query = url.len();
        if url.contains('?') {
            start_query = url.find('?').expect("The url contains atleast 1 '?'");
        }

        &url[..start_query]
    }

    pub fn site(&self) -> &str {
        let url = self.strip_protocol();

        let mut end_site = url.len();
        if url.contains('/') {
            end_site = url.find('/').expect("The url contains atleast 1 '/'");
        }

        &url[..end_site]
    }

    pub fn domain(&self) -> &str {
        let site = self.site();
        let num_punctuations: usize = site.chars().map(|c| (c == '.') as usize).sum();
        if num_punctuations > 1 {
            let domain_index = site.rfind('.').unwrap();
            let mut start_index = site[..domain_index].rfind('.').unwrap() + 1;

            if &site[start_index..] == "co.uk" {
                if let Some(new_start_index) = site[..start_index - 1].rfind('.') {
                    start_index = new_start_index + 1;
                } else {
                    start_index = 0;
                }
            }

            &site[start_index..]
        } else {
            site
        }
    }

    pub fn domain_name(&self) -> &str {
        let domain = self.domain();

        if let Some(tld_start) = domain.find('.') {
            &domain[..tld_start]
        } else {
            domain
        }
    }

    pub fn subdomain(&self) -> Option<&str> {
        if let Some(subdomain) = self.site().strip_suffix(self.domain()) {
            if subdomain.is_empty() || subdomain == "." {
                None
            } else {
                Some(&subdomain[..subdomain.len() - 1])
            }
        } else {
            None
        }
    }

    pub fn is_homepage(&self) -> bool {
        let url = self.strip_protocol();
        match url.find('/') {
            Some(idx) => idx == url.len() - 1,
            None => true,
        }
    }

    fn find_protocol_end(&self) -> usize {
        let mut start_host = 0;
        let url = &self.0;
        if url.starts_with("http://") || url.starts_with("https://") {
            start_host = url
                .find(':')
                .expect("It was checked that url starts with protocol");
        } else if url.starts_with("//") {
            start_host = url
                .find('/')
                .expect("It was checked that url starts with protocol")
                + 1;
        }
        start_host
    }
    pub fn protocol(&self) -> &str {
        &self.0[..self.find_protocol_end()]
    }

    pub fn is_full_path(&self) -> bool {
        matches!(self.protocol(), "http" | "https" | "pdf")
    }

    pub fn prefix_with(&mut self, url: &Url) {
        self.0 = match (url.0.ends_with('/'), self.0.starts_with('/')) {
            (true, true) => url.protocol().to_string() + "://" + url.site() + &self.0,
            (true, false) => url.full() + &self.0,
            (false, true) => url.protocol().to_string() + "://" + url.site() + &self.0,
            (false, false) => url.full() + "/" + &self.0,
        };
    }

    pub fn full(&self) -> String {
        if self.find_protocol_end() == 0 {
            "https://".to_string() + &self.0
        } else {
            self.0.clone()
        }
    }

    pub async fn download_bytes(&self, timeout: Duration) -> Option<Vec<u8>> {
        let client = reqwest::Client::builder().timeout(timeout).build().unwrap();

        debug!("downloading {:?}", self.full());

        match client.get(self.full()).send().await {
            Ok(res) => {
                let bytes = res.bytes().await.ok()?.to_vec();
                Some(bytes)
            }
            Err(_) => None,
        }
    }

    pub fn raw(&self) -> &str {
        &self.0
    }

    pub fn is_valid_uri(&self) -> bool {
        self.full().as_str().parse::<http::Uri>().is_ok()
    }

    pub(crate) fn host_without_specific_subdomains(&self) -> &str {
        if let Some(subdomain) = self.subdomain() {
            if subdomain == "www" {
                self.domain()
            } else {
                self.site()
            }
        } else {
            self.site()
        }
    }

    pub fn without_query(&self) -> &str {
        if let Some(query_begin) = self.0.find('?') {
            &self.0[..query_begin]
        } else {
            &self.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn double_slash_start() {
        let url: Url = "//scripts.dailymail.co.uk".to_string().into();

        assert_eq!(url.domain(), "dailymail.co.uk");
        assert_eq!(url.domain_name(), "dailymail");
        assert_eq!(url.site(), "scripts.dailymail.co.uk");
    }

    #[test]
    fn co_uk_edgecase() {
        let url: Url = "dailymail.co.uk".to_string().into();

        assert_eq!(url.domain(), "dailymail.co.uk");
        assert_eq!(url.site(), "dailymail.co.uk");
        assert_eq!(url.full().as_str(), "https://dailymail.co.uk");
    }

    #[test]
    fn full() {
        let url: Url = "https://example.com".to_string().into();
        assert_eq!(url.full().as_str(), "https://example.com");

        let url: Url = "http://example.com".to_string().into();
        assert_eq!(url.full().as_str(), "http://example.com");
    }

    #[test]
    fn prefix_with() {
        let mut a: Url = "/test".to_string().into();
        let b: Url = "https://example.com".to_string().into();
        a.prefix_with(&b);
        assert_eq!(a.full().as_str(), "https://example.com/test");

        let mut a: Url = "test".to_string().into();
        let b: Url = "https://example.com".to_string().into();
        a.prefix_with(&b);
        assert_eq!(a.full().as_str(), "https://example.com/test");

        let mut a: Url = "test".to_string().into();
        let b: Url = "https://example.com/".to_string().into();
        a.prefix_with(&b);
        assert_eq!(a.full().as_str(), "https://example.com/test");

        let mut a: Url = "/test".to_string().into();
        let b: Url = "https://example.com/".to_string().into();
        a.prefix_with(&b);
        assert_eq!(a.full().as_str(), "https://example.com/test");
    }

    #[test]
    fn is_full_path() {
        let url: Url = "https://dailymail.co.uk".to_string().into();
        assert!(url.is_full_path());
    }

    #[test]
    fn is_valid() {
        let url: Url = "https://dailymail.co.uk".to_string().into();
        assert!(url.is_valid_uri());

        let url: Url = "da<>ilymail.co.uk".to_string().into();
        assert!(!url.is_valid_uri());
    }

    #[test]
    fn subdomain() {
        let url: Url = "https://test.example.com".to_string().into();
        assert_eq!(url.subdomain(), Some("test"));

        let url: Url = "https://test1.test2.example.com".to_string().into();
        assert_eq!(url.subdomain(), Some("test1.test2"));

        let url: Url = "https://example.com".to_string().into();
        assert_eq!(url.subdomain(), None);
    }

    #[test]
    fn url_without_query() {
        let url: Url = "https://test.example.com?key=val&key2=val2"
            .to_string()
            .into();

        assert_eq!(url.without_query(), "https://test.example.com");
    }
}
