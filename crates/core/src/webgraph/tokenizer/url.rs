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
    fn parse(url: &str) -> Result<Self> {
        let url = url::Url::robust_parse(url)?;
        let scheme: VecDeque<String> = url
            .scheme()
            .split_preserve(|c| c == ':')
            .map(|s| {
                let mut s = s.to_string();
                s.push(':');
                s
            })
            .add_space_last()
            .collect();

        let mut host: VecDeque<String> = url
            .normalized_host()
            .unwrap_or_default()
            .split_preserve(|c| c == '.')
            .map(|s| s.to_string())
            .add_space_last()
            .collect();

        let mut path: VecDeque<_> = url
            .path()
            .split_preserve(|c| matches!(c, '/' | '-' | '_'))
            .filter(|s| !(*s).is_empty())
            .map(|s| s.to_string())
            .collect();

        if host.is_empty() {
            if let Some(maybe_host) = path.pop_front() {
                let host_from_path = match maybe_host.split_once('@') {
                    Some((rest, host)) => {
                        path.push_front(rest.to_string());
                        host.to_string()
                    }
                    None => maybe_host.to_string(),
                };

                host = host_from_path
                    .split_preserve(|c| c == '.')
                    .map(|s| s.to_string())
                    .add_space_last()
                    .collect();
            }
        }

        Ok(Self { scheme, host, path })
    }

    pub fn next(&mut self) -> Option<String> {
        self.scheme
            .pop_front()
            .or_else(|| self.host.pop_front())
            .or_else(|| self.path.pop_front())
    }
}

pub struct UrlTokenStream {
    url: ParsedUrl,
    token: Token,
}

impl UrlTokenStream {
    pub fn new(text: &str) -> Self {
        let url = ParsedUrl::parse(text).unwrap_or_default();

        Self {
            url,
            token: Token::default(),
        }
    }
}

impl TokenStream for UrlTokenStream {
    fn advance(&mut self) -> bool {
        match self.url.next() {
            Some(token) => {
                self.token.position = self.token.position.wrapping_add(1);
                self.token.offset_from = self.token.offset_to;
                self.token.offset_to = self.token.offset_from + token.len();
                self.token.text = token;

                true
            }
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
        assert_eq!(
            tokenize("example.com"),
            ["https: ", "example", ".", "com ", "/"]
        );
        assert_eq!(tokenize(".com"), ["https: ", ".", "com ", "/"]);

        assert_eq!(
            tokenize("mailto:hello@example.com"),
            ["mailto: ", "example", ".", "com ", "hello"]
        );

        assert_eq!(
            tokenize("mailto:example.com"),
            ["mailto: ", "example", ".", "com "]
        );

        assert_eq!(tokenize("tel:+4512345678"), ["tel: ", "+4512345678 "]);
    }
}
