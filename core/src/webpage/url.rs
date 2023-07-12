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

use std::{fmt::Display, time::Duration};

use publicsuffix::Psl;
use serde::{Deserialize, Serialize};
use tracing::debug;

use super::URL_REGEX;

pub static LIST: once_cell::sync::Lazy<publicsuffix::List> = once_cell::sync::Lazy::new(|| {
    include_str!("../../public_suffix_list.dat")
        .parse()
        .expect("Failed to parse public suffix list")
});

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Hash)]
pub struct Url(String);

impl Display for Url {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0)
    }
}

impl From<String> for Url {
    fn from(url: String) -> Self {
        let url = if url.ends_with('/') {
            &url[..url.len() - 1]
        } else {
            &url
        };

        Url(url.trim().to_string())
    }
}

impl From<&str> for Url {
    fn from(url: &str) -> Self {
        Self::from(url.to_string())
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
        let site = self.site().as_bytes();
        match LIST.domain(site) {
            Some(domain) => match std::str::from_utf8(domain.as_bytes()) {
                Ok(res) => res,
                Err(_) => "",
            },
            None => "",
        }
    }

    fn tld(&self) -> &str {
        let site = self.site().as_bytes();
        match LIST.suffix(site) {
            Some(tld) => match std::str::from_utf8(tld.as_bytes()) {
                Ok(res) => res,
                Err(_) => "",
            },
            None => "",
        }
    }

    pub fn domain_name(&self) -> &str {
        let domain = self.domain();
        let tld = self.tld();

        if domain.is_empty() || tld.is_empty() {
            ""
        } else {
            if tld.len() + 1 > domain.len() {
                return "";
            }

            &domain[..domain.len() - tld.len() - 1]
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
        matches!(self.protocol(), "http" | "https" | "pdf") || self.0.starts_with("//")
    }

    fn prefix_with(&mut self, url: &Url) {
        self.0 = match (url.0.ends_with('/'), self.0.starts_with('/')) {
            (true, true) => {
                let prot = url.protocol().to_string();
                if prot.is_empty() {
                    url.full() + self.0.strip_prefix('/').unwrap()
                } else {
                    prot + "://" + url.site() + self.0.as_str()
                }
            }
            (true, false) => url.full() + self.0.as_str(),
            (false, true) => {
                let prot = url.protocol().to_string();
                if prot.is_empty() {
                    url.full() + self.0.as_str()
                } else {
                    prot + "://" + url.site() + self.0.as_str()
                }
            }
            (false, false) => url.full() + "/" + self.0.as_str(),
        };
    }

    pub fn full(&self) -> String {
        if self.0.starts_with("//") {
            return "http:".to_string() + self.0.as_str();
        }

        if self.find_protocol_end() == 0 {
            "http://".to_string() + self.0.as_str()
        } else {
            self.0.clone()
        }
    }

    pub fn into_absolute(self, base: &Url) -> Self {
        let mut url = self;
        if !url.is_full_path() {
            url.prefix_with(base);
        }
        url
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

    pub(crate) fn host_without_specific_subdomains_and_query(&self) -> &str {
        let res = if let Some(subdomain) = self.subdomain() {
            if subdomain == "www" {
                self.domain()
            } else {
                self.site()
            }
        } else {
            self.site()
        };

        if let Some(query_begin) = res.find('?') {
            &res[..query_begin]
        } else {
            res
        }
    }

    pub fn without_query(&self) -> &str {
        if let Some(query_begin) = self.0.find('?') {
            &self.0[..query_begin]
        } else {
            &self.0
        }
    }

    pub fn full_without_id_tags(&self) -> String {
        let full = self.full();
        if let Some(id_begin) = full.find('#') {
            full[..id_begin].to_string()
        } else {
            full
        }
    }

    pub fn matches_url_regex(&self) -> bool {
        URL_REGEX.is_match(&self.full())
    }

    pub fn without_protocol(&self) -> &str {
        self.0[self.find_protocol_end()..]
            .strip_prefix("://")
            .unwrap_or(&self.0)
    }

    pub fn path_ends_with(&self, ending: &str) -> bool {
        if self.is_homepage() {
            false
        } else {
            self.without_query().ends_with(ending)
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn path_and_query(&self) -> &str {
        let without_prot = self.without_protocol();
        without_prot
            .find('/')
            .map_or("", |idx| without_prot.get(idx..).map_or("", |path| path))
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
        assert_eq!(url.full(), "http://scripts.dailymail.co.uk");
    }

    #[test]
    fn co_uk_edgecase() {
        let url: Url = "dailymail.co.uk".to_string().into();

        assert_eq!(url.domain(), "dailymail.co.uk");
        assert_eq!(url.site(), "dailymail.co.uk");
        assert_eq!(url.full().as_str(), "http://dailymail.co.uk");
        assert!(url.matches_url_regex());
    }

    #[test]
    fn full() {
        let url: Url = "https://example.com".to_string().into();
        assert_eq!(url.full().as_str(), "https://example.com");
        assert!(url.matches_url_regex());

        let url: Url = "http://example.com".to_string().into();
        assert_eq!(url.full().as_str(), "http://example.com");
    }

    #[test]
    fn into_absolute() {
        let mut a: Url = "/test".to_string().into();
        let b: Url = "https://example.com".to_string().into();
        a = a.into_absolute(&b);
        assert_eq!(a.full().as_str(), "https://example.com/test");

        let mut a: Url = "test".to_string().into();
        let b: Url = "https://example.com".to_string().into();
        a = a.into_absolute(&b);
        assert_eq!(a.full().as_str(), "https://example.com/test");

        let mut a: Url = "test".to_string().into();
        let b: Url = "https://example.com/".to_string().into();
        a = a.into_absolute(&b);
        assert_eq!(a.full().as_str(), "https://example.com/test");

        let mut a: Url = "/test".to_string().into();
        let b: Url = "https://example.com/".to_string().into();
        a = a.into_absolute(&b);
        assert_eq!(a.full().as_str(), "https://example.com/test");

        let mut a: Url = "https://example.com/test".to_string().into();
        let b: Url = "https://example.com/".to_string().into();
        a = a.into_absolute(&b);
        assert_eq!(a.full().as_str(), "https://example.com/test");

        let mut a: Url = "https://example.com/test".to_string().into();
        let b: Url = "example.com".to_string().into();
        a = a.into_absolute(&b);
        assert_eq!(a.full().as_str(), "https://example.com/test");

        let mut a: Url = "https://a.com/test".to_string().into();
        let b: Url = "b.com".to_string().into();
        a = a.into_absolute(&b);
        assert_eq!(a.full().as_str(), "https://a.com/test");
    }

    #[test]
    fn is_full_path() {
        let url: Url = "https://dailymail.co.uk".to_string().into();
        assert!(url.is_full_path());

        let url: Url = "//dailymail.co.uk".to_string().into();
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

    #[test]
    fn url_without_id() {
        let url: Url = "https://test.example.com#id".to_string().into();

        assert_eq!(&url.full_without_id_tags(), "https://test.example.com");
    }

    #[test]
    fn url_without_protocol() {
        let url: Url = "https://test.example.com/test/test".to_string().into();
        assert_eq!(url.without_protocol(), "test.example.com/test/test");

        let url: Url = "test.example.com/test/test".to_string().into();
        assert_eq!(url.without_protocol(), "test.example.com/test/test");
    }

    #[test]
    fn url_is_homepage() {
        let url: Url = "https://test.example.com".to_string().into();
        assert!(url.is_homepage());

        let url: Url = "https://test.example.com/".to_string().into();
        assert!(url.is_homepage());

        let url: Url = "https://test.example.com/test".to_string().into();
        assert!(!url.is_homepage());

        let url: Url = "https://podcasts.apple.com/fr/podcast/beyond-2-cest-quoi-un-planneur-strat%C3%A9gique/id1492683918?i=1000460534325".to_string().into();
        assert!(!url.is_homepage());

        let url: Url = "https://podcasts.apple.com".to_string().into();
        assert!(url.is_homepage());

        let url: Url = "podcasts.apple.com".to_string().into();
        assert!(url.is_homepage());
    }

    #[test]
    fn path_ends_with() {
        let url: Url = "https://test.example.com/test/test".to_string().into();
        assert!(url.path_ends_with("test"));

        let url: Url = "https://test.example.com/test/test".to_string().into();
        assert!(url.path_ends_with("test/test"));

        let url: Url = "https://test.example.com".to_string().into();
        assert!(!url.path_ends_with("/"));

        let url: Url = "https://test.example.zip".to_string().into();
        assert!(!url.path_ends_with(".zip"));

        let url: Url = "https://test.example.com".to_string().into();
        assert!(!url.path_ends_with(".com"));

        let url: Url = "https://test.example.com/.com".to_string().into();
        assert!(url.path_ends_with(".com"));

        let url: Url = "https://test.example.com/test.png".to_string().into();
        assert!(url.path_ends_with(".png"));
    }

    #[test]
    fn path_and_query() {
        let url: Url = "https://example.com".to_string().into();
        assert!(url.path_and_query().is_empty());

        let url: Url = "https://example.com/".to_string().into();
        assert!(url.path_and_query().is_empty());

        let url: Url = "https://example.com/test".to_string().into();
        assert_eq!(url.path_and_query(), "/test");

        let url: Url = "https://example.com/test?a=b".to_string().into();
        assert_eq!(url.path_and_query(), "/test?a=b");

        let url: Url = "example.com/test?a=b".to_string().into();
        assert_eq!(url.path_and_query(), "/test?a=b");
    }
}
