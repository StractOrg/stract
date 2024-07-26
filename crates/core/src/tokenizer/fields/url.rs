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

use std::collections::VecDeque;

use tantivy::tokenizer::BoxTokenStream;

use crate::{
    tokenizer::{add_space_last::AddSpaceLast, split_preserve::StrSplitPreserve},
    webpage::url_ext::UrlExt,
};

#[derive(Clone, Default)]
struct ParsedUrl {
    protocol: Option<VecDeque<String>>,
    domain: Option<VecDeque<String>>,
    path: VecDeque<String>,
}

#[derive(Debug, Clone)]
pub struct UrlTokenizer;

impl UrlTokenizer {
    pub fn as_str() -> &'static str {
        "url_tokenizer"
    }

    fn parse_url(text: &str) -> ParsedUrl {
        url::Url::parse(text)
            .or_else(|_| url::Url::parse(&format!("http://{}", text)))
            .map(|url| {
                let domain = Some(
                    url.normalized_host()
                        .unwrap_or("")
                        .split_preserve(|c| matches!(c, '.'))
                        .filter(|s| !(*s).is_empty())
                        .map(|s| s.to_string())
                        .add_space_last()
                        .collect(),
                );
                let path: VecDeque<_> = url
                    .path()
                    .split_preserve(|c| matches!(c, '/' | '-' | '_'))
                    .filter(|s| !(*s).is_empty())
                    .map(|s| s.to_string())
                    .collect();

                if matches!(url.scheme(), "http" | "https") {
                    ParsedUrl {
                        protocol: None,
                        domain,
                        path,
                    }
                } else {
                    let mut v = VecDeque::new();
                    v.push_back(url.scheme().to_string());

                    ParsedUrl {
                        protocol: Some(v),
                        domain,
                        path,
                    }
                }
            })
            .unwrap_or_default()
    }
}

impl tantivy::tokenizer::Tokenizer for UrlTokenizer {
    type TokenStream<'a> = BoxTokenStream<'a>;

    fn token_stream<'a>(&mut self, text: &'a str) -> Self::TokenStream<'a> {
        let text = text.replace(' ', "%20");

        let urls = text
            .split('\n')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_lowercase())
            .map(|s| Self::parse_url(&s))
            .collect();

        BoxTokenStream::new(SiteOperatorUrlTokenStream::new(urls))
    }
}

pub struct SiteOperatorUrlTokenStream {
    urls: VecDeque<ParsedUrl>,
    current_url: ParsedUrl,
    token: tantivy::tokenizer::Token,
}

impl SiteOperatorUrlTokenStream {
    fn new(mut urls: VecDeque<ParsedUrl>) -> Self {
        let current_url = urls.pop_front().unwrap_or_default();

        Self {
            urls,
            current_url,
            token: tantivy::tokenizer::Token::default(),
        }
    }

    fn advance_current_url(&mut self) -> bool {
        if let Some(protocol) = self.current_url.protocol.as_mut() {
            self.token.position = self.token.position.wrapping_add(1);
            self.token.text.clear();

            if let Some(s) = protocol.pop_front() {
                self.token.text.push_str(&s);
                self.token.offset_from = 0;
                self.token.offset_to = s.len();
            } else {
                self.token.offset_from = self.token.offset_to;
                self.token.text.push_str("://");
                self.token.offset_to += self.token.text.len();

                self.current_url.protocol = None;
            }

            return true;
        }

        if let Some(domain) = self.current_url.domain.as_mut() {
            if let Some(s) = domain.pop_front() {
                self.token.text.clear();
                self.token.position = self.token.position.wrapping_add(1);

                self.token.text.push_str(&s);

                self.token.offset_from = self.token.offset_to;
                self.token.offset_to += self.token.text.len();
                return true;
            }
        }

        if let Some(s) = self.current_url.path.pop_front() {
            self.token.text.clear();
            self.token.position = self.token.position.wrapping_add(1);

            self.token.text.push_str(&s);
            self.token.offset_from = self.token.offset_to;
            self.token.offset_to += self.token.text.len();

            return true;
        }

        false
    }

    fn next_url(&mut self) -> bool {
        if let Some(url) = self.urls.pop_front() {
            self.current_url = url;

            self.token.position = self.token.position.wrapping_add(1);
            self.token.text.clear();
            self.token.text.push('\n');

            self.token.offset_from = self.token.offset_to;
            self.token.offset_to += self.token.text.len();

            true
        } else {
            false
        }
    }
}

impl tantivy::tokenizer::TokenStream for SiteOperatorUrlTokenStream {
    fn advance(&mut self) -> bool {
        if self.advance_current_url() {
            return true;
        }

        self.next_url()
    }

    fn token(&self) -> &tantivy::tokenizer::Token {
        &self.token
    }

    fn token_mut(&mut self) -> &mut tantivy::tokenizer::Token {
        &mut self.token
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lending_iter::LendingIterator;
    use proptest::prelude::*;
    use tantivy::tokenizer::Tokenizer as _;

    fn tokenize_url(s: &str) -> Vec<String> {
        let mut res = Vec::new();
        let mut tokenizer = UrlTokenizer;
        let mut stream = tokenizer.token_stream(s);
        let mut it = tantivy::tokenizer::TokenStream::iter(&mut stream);

        while let Some(token) = it.next() {
            res.push(token.text.clone());
        }

        res
    }

    #[test]
    fn url() {
        assert_eq!(
            tokenize_url("https://www.example.com"),
            vec!["example", ".", "com ", "/"]
        );

        assert_eq!(
            tokenize_url("https://www.example.com/test"),
            vec!["example", ".", "com ", "/", "test",]
        );

        assert_eq!(
            tokenize_url("example.com"),
            vec!["example", ".", "com ", "/"]
        );

        assert_eq!(
            tokenize_url("example.com/another/path"),
            vec!["example", ".", "com ", "/", "another", "/", "path",]
        );

        assert_eq!(tokenize_url(".com"), vec![".", "com ", "/"])
    }

    #[test]
    fn multiple_urls() {
        assert_eq!(
            tokenize_url("https://www.example.com\nhttps://www.example.com"),
            vec!["example", ".", "com ", "/", "\n", "example", ".", "com ", "/"]
        );

        assert_eq!(
            tokenize_url("https://www.example.com/test\nhttps://www.abcd.com"),
            vec!["example", ".", "com ", "/", "test", "\n", "abcd", ".", "com ", "/"]
        );

        assert_eq!(
            tokenize_url("https://example.com/test\nhttps://www.abcd.com/test"),
            vec!["example", ".", "com ", "/", "test", "\n", "abcd", ".", "com ", "/", "test",]
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(4096))]

        #[test]
        fn test_single_space(url: String) {
            let tokens = tokenize_url(&url);

            let num_spaces = tokens.iter().filter(|s| s.contains(' ')).count();
            prop_assert!(num_spaces <= 1);
        }
    }
}
