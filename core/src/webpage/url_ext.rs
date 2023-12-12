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

use publicsuffix::Psl;

static FULL_LIST: once_cell::sync::Lazy<publicsuffix::List> = once_cell::sync::Lazy::new(|| {
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
    fn icann_domain(&self) -> Option<&str>;
    fn root_domain(&self) -> Option<&str>;
    fn normalized_host(&self) -> Option<&str>;
    fn subdomain(&self) -> Option<&str>;
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
        let suffix = std::str::from_utf8(FULL_LIST.domain(host.as_bytes())?.as_bytes()).ok()?;
        Some(suffix)
    }

    fn normalized_host(&self) -> Option<&str> {
        self.host_str().map(|host| host.trim_start_matches("www."))
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
