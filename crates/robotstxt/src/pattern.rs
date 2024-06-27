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

use std::cmp::Ordering;

#[derive(Debug)]
pub struct Pattern {
    pattern: String,
    len: usize,
}

impl Ord for Pattern {
    fn cmp(&self, other: &Self) -> Ordering {
        self.len().cmp(&other.len()).reverse()
    }
}

impl PartialOrd for Pattern {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Pattern {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern
    }
}

impl Eq for Pattern {}

impl Pattern {
    pub fn new(pattern: &str) -> Self {
        let len = pattern.len();
        let pattern = percent_encode(pattern);
        if pattern.contains('$') {
            return Self {
                pattern: pattern.split('$').next().unwrap().to_string() + "$",
                len,
            };
        }

        Self {
            pattern: pattern.to_string(),
            len,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn matches(&self, path: &str) -> bool {
        let path = percent_encode(path);
        let parts = self.pattern.split('*');

        let mut start = 0;

        for (idx, part) in parts.enumerate() {
            if part.ends_with('$') {
                if idx > 0 && part.chars().all(|c| c == '$') {
                    return true;
                }

                let part = part.trim_end_matches('$');

                if idx == 0 {
                    return path == part;
                }

                // rfind because the previous '*' would have matched whatever it could
                match path[start..].rfind(part) {
                    Some(idx) => start += idx + part.len(),
                    _ => {
                        return false;
                    }
                }

                return start == path.len();
            }

            if idx == 0 {
                if !path.starts_with(part) {
                    return false;
                }
                start += part.len();
            } else {
                match path[start..].find(part) {
                    Some(idx) => start += idx + part.len(),
                    None => {
                        return false;
                    }
                }
            }
        }

        true
    }
}

pub(crate) fn percent_encode(input: &str) -> String {
    const FRAGMENT: percent_encoding::AsciiSet = percent_encoding::CONTROLS
        .add(b' ')
        .add(b'"')
        .add(b'<')
        .add(b'>')
        .add(b'`');

    percent_encoding::utf8_percent_encode(
        &percent_encoding::percent_decode_str(input).decode_utf8_lossy(),
        &FRAGMENT,
    )
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn test_empty_match() {
        let rule = Pattern::new("");
        assert!(rule.matches(""));
        assert!(rule.matches("foo"));
    }

    #[test]
    fn test_prefix_match() {
        let rule = Pattern::new("/foo/bar");
        assert!(rule.matches("/foo/bar"));
        assert!(rule.matches("/foo/bar/"));
        assert!(rule.matches("/foo/bar/baz"));
        assert!(rule.matches("/foo/barbaz"));
        assert!(!rule.matches("/foo"));
        assert!(!rule.matches("/foo/baz"));
    }

    #[test]
    fn test_wildcard_match() {
        let rule = Pattern::new("/foo/*/bar");
        assert!(rule.matches("/foo/baz/bar"));
        assert!(rule.matches("/foo/baz/bar/baz"));
        assert!(rule.matches("/foo/baz/baz/bar/baz"));
        assert!(!rule.matches("/foo/bar"));
        assert!(!rule.matches("/foo/baz"));

        let rule = Pattern::new("/foo/bar*");
        assert!(rule.matches("/foo/bar"));
        assert!(rule.matches("/foo/barbaz"));
        assert!(rule.matches("/foo/bar/baz"));
        assert!(!rule.matches("/foo"));

        let rule = Pattern::new("*/bar");

        assert!(rule.matches("foo/bar"));
        assert!(rule.matches("foo/bar/"));
        assert!(rule.matches("foo/bar/baz"));
        assert!(rule.matches("foo/barbaz"));
        assert!(!rule.matches("foo"));
        assert!(!rule.matches("foo/baz"));

        let rule = Pattern::new("*/bar*");
        assert!(rule.matches("foo/bar"));
        assert!(rule.matches("foo/barbaz"));
        assert!(rule.matches("foo/bar/baz"));
        assert!(!rule.matches("foo"));
    }

    #[test]
    fn test_end_match() {
        let rule = Pattern::new("/foo/bar$");
        assert!(rule.matches("/foo/bar"));
        assert!(!rule.matches("/foo/bar/"));
        assert!(!rule.matches("/foo/bar/baz"));
        assert!(!rule.matches("/foo"));
        assert!(!rule.matches("/foo/baz"));
        assert!(!rule.matches("/foo/barbaz"));
    }

    #[test]
    fn test_wildcard_end_match() {
        let rule = Pattern::new("/foo/*/bar$");
        assert!(rule.matches("/foo/baz/bar"));
        assert!(rule.matches("/foo/baz/baz/bar"));
        assert!(!rule.matches("/foo/baz/baz/bar/baz"));
        assert!(!rule.matches("/foo/bar"));
        assert!(!rule.matches("/foo/baz/bar/"));
        assert!(!rule.matches("/foo/bar/"));
        assert!(!rule.matches("/foo/baz/bar/baz"));
        assert!(!rule.matches("/foo/baz"));
        assert!(!rule.matches("/foo/baz/bar/baz/baz"));

        let rule = Pattern::new("/foo/*$");
        assert!(rule.matches("/foo/bar"));
        assert!(rule.matches("/foo/baz"));
        assert!(rule.matches("/foo/baz/bar"));
        assert!(rule.matches("/foo/baz/baz"));
        assert!(!rule.matches("/foo"));
        assert!(!rule.matches("/bar/bar/"));

        let rule = Pattern::new("*A$");
        assert!(rule.matches("AAA"));
    }

    #[test]
    fn test_multi_wildcard() {
        let rule = Pattern::new("/foo/*/bar/*/baz");
        assert!(rule.matches("/foo/baz/bar/baz/baz"));
        assert!(rule.matches("/foo/baz/bar/baz/baz/baz"));
        assert!(!rule.matches("/foo/bar/baz/baz"));
        assert!(!rule.matches("/foo/baz/bar/baz"));

        let rule = Pattern::new("/foo/******/bar");
        assert!(rule.matches("/foo/baz/bar"));
        assert!(rule.matches("/foo/baz/baz/bar"));
        assert!(rule.matches("/foo/baz/baz/baz/bar"));
        assert!(!rule.matches("/foo/bar"));
    }

    #[test]
    fn test_end_mid_pattern() {
        let rule = Pattern::new("/foo/bar$/baz");
        assert!(rule.matches("/foo/bar"));
        assert!(!rule.matches("/foo/bar/"));
        assert!(!rule.matches("/foo/bar/baz"));
        assert!(!rule.matches("/foo/bar/baz/baz"));
        assert!(!rule.matches("/foo/barbaz"));

        let rule = Pattern::new("$");
        assert!(rule.matches(""));
        assert!(!rule.matches("/foo"));
        assert!(!rule.matches("/foo/bar"));
        assert!(!rule.matches("/foo/bar/"));
    }

    #[test]
    fn test_multi_end() {
        let rule = Pattern::new("/foo/bar$/baz$");
        assert!(rule.matches("/foo/bar"));
        assert!(!rule.matches("/foo/bar/"));
        assert!(!rule.matches("/foo/bar/baz"));
        assert!(!rule.matches("/foo/bar/baz/baz"));
        assert!(!rule.matches("/foo/barbaz"));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(8192))]

        #[test]
        fn proptest_identity(s in "[a-zA-Z0-9]*") {
            let rule = Pattern::new(&s);
            prop_assert!(rule.matches(&s));
        }

        #[test]
        fn wildcard_end_matches_anything(path in "[a-zA-Z0-9]*") {
            let rule = Pattern::new("*$");
            prop_assert!(rule.matches(&path));
        }

        #[test]
        fn proptest_regex(pattern: String, path: String) {
            let mut pattern = percent_encode(&pattern);
            let path = percent_encode(&path);

            if pattern.contains('$') {
                pattern = pattern.split('$').next().unwrap().to_string() + "$";
            }

            let rule = Pattern::new(&pattern);

            let pattern = regex::escape(&pattern).replace("\\*", ".*").replace("\\$", "$");
            let pattern = "^".to_string() + &pattern;
            let re = regex::Regex::new(&pattern).unwrap();

            let path = percent_encode(&path);

            prop_assert_eq!(rule.matches(&path), re.is_match(&path));
        }

        #[test]
        fn percent_encode_idempotent(s: String) {
            let encoded = percent_encode(&s);
            prop_assert_eq!(percent_encode(&encoded), encoded);
        }
    }
}
