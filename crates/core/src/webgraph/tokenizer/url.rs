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

use crate::tokenizer::{add_space_last::AddSpaceLast, split_preserve::StrSplitPreserve};
use crate::{webpage::url_ext::UrlExt, Result};
use std::collections::VecDeque;

use tantivy::tokenizer::{Token, TokenStream};

#[derive(Default)]
struct ParsedUrl {
    scheme: VecDeque<String>,
    host: VecDeque<String>,
    path: VecDeque<String>,
}

impl ParsedUrl {
    fn parse_scheme(url: &url::Url) -> VecDeque<String> {
        url.scheme()
            .split_preserve(|c| c == ':')
            .map(|s| {
                let mut s = s.to_string();
                s.push(':');
                s
            })
            .add_space_last()
            .collect()
    }

    fn parse_host(host: &str) -> VecDeque<String> {
        host.split_preserve(|c| c == '.')
            .map(|s| s.to_string())
            .add_space_last()
            .collect()
    }

    fn parse_path(path: &str) -> VecDeque<String> {
        path.split_preserve(|c| matches!(c, '/' | '-' | '_'))
            .filter(|s| !(*s).is_empty())
            .map(|s| s.to_string())
            .collect()
    }

    fn parse_non_http(raw_url: &str, parsed_url: url::Url) -> Result<Self> {
        let scheme: VecDeque<String> = if raw_url.starts_with(&format!("{}:", parsed_url.scheme()))
        {
            Self::parse_scheme(&parsed_url)
        } else {
            VecDeque::new()
        };

        let (host, path) = match parsed_url.path().split_once('@') {
            Some((rest, host_from_path)) => {
                let host = Self::parse_host(host_from_path);
                let path = Self::parse_path(rest);
                (host, path)
            }
            None => {
                let host = Self::parse_host(parsed_url.path());
                let path = VecDeque::new();
                (host, path)
            }
        };

        Ok(Self { scheme, host, path })
    }

    fn parse(url: &str) -> Result<Self> {
        let url = url.replace(" ", "%20");

        if !url.contains('.') && !url.contains(':') {
            let mut host = VecDeque::new();
            host.push_back(url.to_string());

            return Ok(Self {
                scheme: VecDeque::new(),
                host,
                path: VecDeque::new(),
            });
        }

        let parsed_url = url::Url::robust_parse(&url)?;

        if parsed_url.scheme() != "http" && parsed_url.scheme() != "https" {
            return Self::parse_non_http(&url, parsed_url);
        }
        let scheme = if url.starts_with(&format!("{}:", parsed_url.scheme())) {
            Self::parse_scheme(&parsed_url)
        } else {
            VecDeque::new()
        };

        let normalized_host = parsed_url.normalized_host().unwrap_or_default();

        let host = if normalized_host != parsed_url.scheme() {
            Self::parse_host(normalized_host)
        } else {
            VecDeque::new()
        };

        let path = if normalized_host != parsed_url.scheme() {
            Self::parse_path(parsed_url.path())
        } else {
            VecDeque::new()
        };

        Ok(Self { scheme, host, path })
    }

    pub fn next(&mut self) -> Option<String> {
        self.scheme
            .pop_front()
            .or_else(|| self.host.pop_front())
            .or_else(|| self.path.pop_front())
    }

    pub fn is_empty(&self) -> bool {
        self.scheme.is_empty() && self.host.is_empty() && self.path.is_empty()
    }
}

pub struct UrlTokenStream {
    urls: VecDeque<ParsedUrl>,
    token: Token,
}

impl UrlTokenStream {
    pub fn new(text: &str) -> Self {
        let mut urls = VecDeque::new();

        if let Ok(url) = url::Url::robust_parse(text) {
            if url.scheme() != "http" && url.scheme() != "https" {
                let scheme = url.scheme().to_string();

                for url in text.split(',') {
                    let mut url = url.to_string();

                    if !url.starts_with(&format!("{}:", scheme)) {
                        url = format!("{}:{}", scheme, url);
                    }

                    if let Ok(parsed_url) = ParsedUrl::parse(&url) {
                        urls.push_back(parsed_url);
                    }
                }
            } else if let Ok(parsed_url) = ParsedUrl::parse(text) {
                urls.push_back(parsed_url);
            }
        }

        Self {
            urls,
            token: Token::default(),
        }
    }
}

impl TokenStream for UrlTokenStream {
    fn advance(&mut self) -> bool {
        if let Some(url) = self.urls.front() {
            if url.is_empty() {
                self.urls.pop_front();
            }
        }

        match self.urls.front_mut() {
            Some(url) => match url.next() {
                Some(token) => {
                    self.token.position = self.token.position.wrapping_add(1);
                    self.token.offset_from = self.token.offset_to;
                    self.token.offset_to = self.token.offset_from + token.len();
                    self.token.text = token;
                    true
                }
                None => false,
            },
            None => false,
        }
    }

    fn token(&self) -> &Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.token
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokenize(url: &str) -> Vec<String> {
        let mut url = UrlTokenStream::new(url);
        let mut tokens = Vec::new();
        while url.advance() {
            tokens.push(url.token().text.clone());
        }
        tokens
    }

    #[test]
    fn test_parse() {
        assert_eq!(
            tokenize("https://example.com/path/to/resource"),
            ["https: ", "example", ".", "com ", "/", "path", "/", "to", "/", "resource"]
        );
        assert_eq!(tokenize("example.com"), ["example", ".", "com ", "/"]);
        assert_eq!(tokenize(".com"), [".", "com ", "/"]);
        assert_eq!(tokenize("example"), ["example"]);
        assert_eq!(tokenize("example-site"), ["example-site"]);
    }
}
