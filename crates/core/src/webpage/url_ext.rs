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

use crate::Result;
use publicsuffix::Psl;

static PUBLIC_SUFFIX_LIST: once_cell::sync::Lazy<publicsuffix::List> =
    once_cell::sync::Lazy::new(|| {
        include_str!("../../public_suffix_list.dat")
            .parse()
            .expect("Failed to parse public suffix list")
    });
static ICANN_LIST: once_cell::sync::Lazy<publicsuffix::List> = once_cell::sync::Lazy::new(|| {
    include_str!("../../public_icann_suffix.dat")
        .parse()
        .expect("Failed to parse public icann suffix list")
});

pub trait UrlExt {
    fn parse_with_base_url(base_url: &url::Url, url: &str) -> Result<url::Url> {
        url::Url::parse(url).or_else(|_| base_url.join(url).map_err(|e| e.into()))
    }
    fn icann_domain(&self) -> Option<&str>;
    fn root_domain(&self) -> Option<&str>;
    fn normalized_host(&self) -> Option<&str>;
    fn normalize(&mut self);
    fn subdomain(&self) -> Option<&str>;
    fn is_homepage(&self) -> bool;
    fn tld(&self) -> Option<&str>;
}

impl UrlExt for url::Url {
    fn icann_domain(&self) -> Option<&str> {
        let host = self.host_str()?;
        let suffix = std::str::from_utf8(ICANN_LIST.domain(host.as_bytes())?.as_bytes()).ok()?;
        Some(suffix)
    }

    fn root_domain(&self) -> Option<&str> {
        let host = self.host_str()?;
        let suffix =
            std::str::from_utf8(PUBLIC_SUFFIX_LIST.domain(host.as_bytes())?.as_bytes()).ok()?;
        Some(suffix)
    }

    fn normalized_host(&self) -> Option<&str> {
        self.host_str().map(|host| host.trim_start_matches("www."))
    }

    fn normalize(&mut self) {
        self.set_fragment(None); // remove fragment (e.g. #comments

        let queries: Vec<_> = self
            .query_pairs()
            .filter(|(key, _)| {
                !key.starts_with("utm_")
                    && !key.starts_with("fbclid")
                    && !key.starts_with("gclid")
                    && !key.starts_with("msclkid")
            })
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();

        {
            let mut query_mut = self.query_pairs_mut();
            query_mut.clear();
            if !queries.is_empty() {
                query_mut.extend_pairs(queries);
            }
        }

        if self.query().unwrap_or_default().is_empty() {
            self.set_query(None);
        }

        if !self.username().is_empty() {
            let _ = self.set_username("");
        }

        if self.password().is_some() {
            let _ = self.set_password(None);
        }
    }

    fn subdomain(&self) -> Option<&str> {
        let domain = self.root_domain()?;
        let host = self.host_str()?;

        let mut subdomain = host.strip_suffix(domain)?;

        if let Some(s) = subdomain.strip_suffix('.') {
            subdomain = s;
        }

        Some(subdomain)
    }

    fn is_homepage(&self) -> bool {
        self.path() == "/" && self.query().is_none()
    }

    fn tld(&self) -> Option<&str> {
        let host = self.host_str()?;
        let suffix = std::str::from_utf8(ICANN_LIST.suffix(host.as_bytes())?.as_bytes()).ok()?;
        Some(suffix)
    }
}

#[cfg(test)]
mod tests {
    use url::Url;

    use super::*;

    #[test]
    fn domain_from_domain_url() {
        let url: Url = Url::parse("http://example.com").unwrap();
        assert_eq!(url.root_domain().unwrap(), "example.com");
        assert_eq!(url.icann_domain().unwrap(), "example.com");

        let url: Url = Url::parse("http://test.example.com").unwrap();
        assert_eq!(url.root_domain().unwrap(), "example.com");
        assert_eq!(url.icann_domain().unwrap(), "example.com");

        assert_eq!(url.subdomain().unwrap(), "test");
    }

    #[test]
    fn icann_domains() {
        let url: Url = Url::parse("http://example.blogspot.com").unwrap();
        assert_eq!(url.domain().unwrap(), "example.blogspot.com");
        assert_eq!(url.icann_domain().unwrap(), "blogspot.com");
    }

    #[test]
    fn suffix() {
        let url: Url = Url::parse("http://example.blogspot.com").unwrap();
        assert_eq!(url.tld().unwrap(), "com");

        let url: Url = Url::parse("http://example.com").unwrap();
        assert_eq!(url.tld().unwrap(), "com");
    }
}
