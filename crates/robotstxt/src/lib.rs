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

//! A robots.txt parser and matcher compliant with [RFC 9309](https://www.rfc-editor.org/rfc/rfc9309.html)
//! and [Google's Robots.txt parser](https://github.com/google/robotstxt) (with some extensions).

const MAX_CHAR_LIMIT_DEFAULT: usize = 512_000; // 512 KB

mod parser;
mod pattern;

use std::time::Duration;

use itertools::Itertools;
use pattern::Pattern;
use url::Url;

use crate::parser::Line;

#[derive(Debug, PartialEq, Eq)]
enum Directive {
    Allow,
    Disallow,
}

impl Ord for Directive {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Self::Allow, Self::Disallow) => std::cmp::Ordering::Less,
            (Self::Disallow, Self::Allow) => std::cmp::Ordering::Greater,
            _ => std::cmp::Ordering::Equal,
        }
    }
}

impl PartialOrd for Directive {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Rule {
    pattern: Pattern,
    directive: Directive,
}

impl Ord for Rule {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.pattern
            .cmp(&other.pattern)
            .then(self.directive.cmp(&other.directive))
    }
}

impl PartialOrd for Rule {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Params {
    pub char_limit: usize,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            char_limit: MAX_CHAR_LIMIT_DEFAULT,
        }
    }
}

pub struct Robots {
    rules: Vec<Rule>,
    crawl_delay: Option<f32>,
    sitemaps: Vec<String>,
}

impl Robots {
    fn is_valid_user_agent(useragent: &str) -> bool {
        useragent
            .chars()
            .all(|c| c.is_ascii_alphabetic() || c == '-' || c == '_')
            && !useragent.is_empty()
    }

    pub fn parse_with_params(
        useragent: &str,
        robotstxt: &str,
        params: Params,
    ) -> Result<Self, anyhow::Error> {
        if !Self::is_valid_user_agent(useragent) {
            return Err(anyhow::anyhow!("Invalid user agent"));
        }

        let robotstxt = robotstxt
            .chars()
            .take(params.char_limit)
            .collect::<String>();

        let robotstxt = robotstxt.replace('\0', "\n");
        let (_, lines) = parser::parse(&robotstxt).map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let mut useragent = useragent.to_lowercase();

        if !lines.iter().any(|line| {
            if let Line::UserAgent(agents) = line {
                agents.iter().any(|agent| agent.to_lowercase() == useragent)
            } else {
                false
            }
        }) {
            useragent = "*".to_string();
        }

        let mut rules = Vec::new();
        let mut crawl_delay = None;
        let mut sitemaps = Vec::new();
        let mut idx = 0;
        let mut useragent_lines = 0;

        while idx < lines.len() {
            let line = &lines[idx];

            if let Line::UserAgent(agents) = &line {
                useragent_lines += 1;
                if agents.iter().any(|agent| agent.to_lowercase() == useragent) {
                    let mut has_captured_directive = false;
                    while idx + 1 < lines.len() {
                        idx += 1;
                        match &lines[idx] {
                            Line::Allow(path) => {
                                has_captured_directive = true;

                                if !path.is_empty() {
                                    rules.push(Rule {
                                        pattern: Pattern::new(path),
                                        directive: Directive::Allow,
                                    });
                                }
                            }
                            Line::Disallow(path) => {
                                has_captured_directive = true;

                                if !path.is_empty() {
                                    rules.push(Rule {
                                        pattern: Pattern::new(path),
                                        directive: Directive::Disallow,
                                    });
                                }
                            }
                            Line::UserAgent(_) if has_captured_directive => break,
                            Line::CrawlDelay(Some(delay)) => {
                                has_captured_directive = true;
                                crawl_delay = Some(*delay);
                            }
                            _ => {}
                        }
                    }
                    continue;
                }
            } else if useragent_lines == 0 {
                // add preceding rules as global rules
                match line {
                    Line::Allow(path) => {
                        rules.push(Rule {
                            pattern: Pattern::new(path),
                            directive: Directive::Allow,
                        });
                    }
                    Line::Disallow(path) => {
                        rules.push(Rule {
                            pattern: Pattern::new(path),
                            directive: Directive::Disallow,
                        });
                    }
                    Line::CrawlDelay(Some(delay)) => {
                        crawl_delay = Some(*delay);
                    }
                    _ => {}
                }
            }

            if let Line::Sitemap(url) = line {
                sitemaps.push(url.to_string());
            }

            idx += 1;
        }

        Ok(Self {
            rules,
            crawl_delay,
            sitemaps,
        })
    }

    pub fn parse(useragent: &str, robotstxt: &str) -> Result<Self, anyhow::Error> {
        Self::parse_with_params(useragent, robotstxt, Params::default())
    }

    pub fn is_allowed(&self, url: &Url) -> bool {
        let path = &Self::prepare_path(url);
        self.is_path_allowed(path)
    }

    fn prepare_path(url: &Url) -> String {
        let path = url.path();

        // replace multiple slashes with a single slash
        let path = path
            .chars()
            .coalesce(|a, b| {
                if a == '/' && b == '/' {
                    Ok(a)
                } else {
                    Err((a, b))
                }
            })
            .collect::<String>();

        if let Some(query) = url.query() {
            format!("{}?{}", path, query)
        } else {
            path
        }
    }

    fn is_precise_path_allowed(&self, path: &str) -> bool {
        let mut path = path.to_string();

        if path.is_empty() {
            path = "/".to_string();
        }

        if path == "/robots.txt" {
            return true;
        }

        let mut matches: Vec<_> = self
            .rules
            .iter()
            .filter(|rule| rule.pattern.matches(&path))
            .collect();

        matches.sort();

        matches
            .first()
            .map(|rule| rule.directive == Directive::Allow)
            .unwrap_or(true)
    }

    pub fn is_path_allowed(&self, path: &str) -> bool {
        let res = self.is_precise_path_allowed(path);

        if !res && path.ends_with('/') {
            self.is_precise_path_allowed(format!("{}index.html", path).as_str())
        } else {
            res
        }
    }

    pub fn crawl_delay(&self) -> Option<Duration> {
        self.crawl_delay.map(Duration::from_secs_f32)
    }

    pub fn sitemaps(&self) -> &[String] {
        &self.sitemaps
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_user_agent_allowed(robots_txt: &str, user_agent: &str, url: &str) -> bool {
        let robots = Robots::parse(user_agent, robots_txt).unwrap();
        if let Ok(url) = Url::parse(url) {
            robots.is_allowed(&url)
        } else {
            robots.is_path_allowed(url)
        }
    }

    fn is_valid_user_agent(user_agent: &str) -> bool {
        Robots::parse(user_agent, "").is_ok()
    }

    fn test_path(url: &str, expected: &str) {
        let url = Url::parse(url)
            .unwrap_or_else(|_| Url::parse(&format!("http://foo.bar/{}", url)).unwrap());

        assert_eq!(Robots::prepare_path(&url), expected);
    }

    fn test_escape(url: &str, expected: &str) {
        assert_eq!(pattern::percent_encode(url), expected);
    }

    // Tese test are based on the tests from
    // https://github.com/google/robotstxt/blob/455b1583103d13ad88fe526bc058d6b9f3309215/robots_test.cc#L399
    #[test]
    fn simple() {
        let robotstxt = r#"
user-agent: FooBot
disallow: /
"#;

        assert!(is_user_agent_allowed("", "FooBot", ""));
        assert!(is_user_agent_allowed(robotstxt, "BarBot", ""));
        assert!(!is_user_agent_allowed(robotstxt, "FooBot", ""));
        assert!(is_user_agent_allowed("", "FooBot", ""));
    }

    #[test]
    fn test_line_syntax_line() {
        let robotstxt_correct = r#"
user-agent: FooBot
disallow: /
"#;

        let robotstxt_incorrect = r#"
fooL FooBot
bar: /
"#;

        let robotstxt_incorrect_accepted = r#"
user-agent: FooBot
disallow /
"#;

        let url = "http://foo.bar/x/y";

        assert!(!is_user_agent_allowed(robotstxt_correct, "FooBot", url));
        assert!(is_user_agent_allowed(robotstxt_incorrect, "FooBot", url));
        assert!(!is_user_agent_allowed(
            robotstxt_incorrect_accepted,
            "FooBot",
            url
        ));
    }

    #[test]
    fn test_line_syntax_groups() {
        let robotstxt = r#"
allow: /foo/bar/

user-agent: FooBot
disallow: /
allow: /x/
user-agent: BarBot
disallow: /
allow: /y/


allow: /w/
user-agent: BazBot

user-agent: FooBot
allow: /z/
disallow: /
"#;

        let w = "http://foo.bar/w/a";
        let x = "http://foo.bar/x/b";
        let y = "http://foo.bar/y/c";
        let z = "http://foo.bar/z/d";

        assert!(is_user_agent_allowed(robotstxt, "FooBot", x));
        assert!(is_user_agent_allowed(robotstxt, "FooBot", z));
        assert!(!is_user_agent_allowed(robotstxt, "FooBot", y));
        assert!(is_user_agent_allowed(robotstxt, "BarBot", y));
        assert!(is_user_agent_allowed(robotstxt, "BarBot", w));
        assert!(!is_user_agent_allowed(robotstxt, "BarBot", z));
        assert!(is_user_agent_allowed(robotstxt, "BazBot", z));

        // // lines with rules outside groups are ignored
        // let foo = "http://foo.bar/foo/bar/";
        // assert!(!is_user_agent_allowed(robotstxt, "FooBot", foo));
        // assert!(!is_user_agent_allowed(robotstxt, "BarBot", foo));
        // assert!(!is_user_agent_allowed(robotstxt, "BazBot", foo));
    }

    #[test]
    fn test_line_syntax_groups_other_rules() {
        let robotstxt = r#"
User-agent: BarBot
Sitemap: https://foo.bar/sitemap
User-agent: *
Disallow: /
"#;

        let url = "http://foo.bar/";

        assert!(!is_user_agent_allowed(robotstxt, "FooBot", url));
        assert!(!is_user_agent_allowed(robotstxt, "BarBot", url));
    }

    #[test]
    fn test_repl_line_names_case_insensitive() {
        let robotstxt_upper = r#"
USER-AGENT: FooBot
ALLOW: /x/
DISALLOW: /
"#;
        let robotstxt_lower = r#"
user-agent: FooBot
allow: /x/
disallow: /
"#;

        let robotstxt_mixed = r#"
uSeR-aGeNt: FooBot
AlLoW: /x/
dIsAlLoW: /
"#;

        let url = "http://foo.bar/x/y";

        assert!(is_user_agent_allowed(robotstxt_upper, "FooBot", url));
        assert!(is_user_agent_allowed(robotstxt_lower, "FooBot", url));
        assert!(is_user_agent_allowed(robotstxt_mixed, "FooBot", url));

        let url = "http://foo.bar/a/b";

        assert!(!is_user_agent_allowed(robotstxt_upper, "FooBot", url));
        assert!(!is_user_agent_allowed(robotstxt_lower, "FooBot", url));
        assert!(!is_user_agent_allowed(robotstxt_mixed, "FooBot", url));
    }

    #[test]
    fn test_verify_valid_user_agents() {
        assert!(is_valid_user_agent("FooBot"));
        assert!(is_valid_user_agent("Foobot-Bar"));
        assert!(is_valid_user_agent("Foo_Bar"));

        assert!(!is_valid_user_agent(""));
        assert!(!is_valid_user_agent(" "));
        assert!(!is_valid_user_agent("🦀"));

        assert!(!is_valid_user_agent("Foobot*"));
        assert!(!is_valid_user_agent(" FooBot "));
        assert!(!is_valid_user_agent("FooBot/1.0"));

        assert!(!is_valid_user_agent("Foobot Bar"));
    }

    #[test]
    fn test_user_agent_case_insensitive() {
        let robotstxt_upper = r#"
User-Agent: FOOBAR
Allow: /x/
Disallow: /
"#;
        let robotstxt_lower = r#"
User-Agent: foobar
Allow: /x/
Disallow: /
"#;

        let robotstxt_mixed = r#"
User-Agent: fOoBaR
Allow: /x/
Disallow: /
"#;

        let url_allowed = "http://foo.bar/x/y";
        let url_disallowed = "http://foo.bar/a/b";

        assert!(is_user_agent_allowed(
            robotstxt_upper,
            "FooBar",
            url_allowed
        ));
        assert!(is_user_agent_allowed(
            robotstxt_lower,
            "FooBar",
            url_allowed
        ));
        assert!(is_user_agent_allowed(
            robotstxt_mixed,
            "FooBar",
            url_allowed
        ));

        assert!(!is_user_agent_allowed(
            robotstxt_upper,
            "FooBar",
            url_disallowed
        ));
        assert!(!is_user_agent_allowed(
            robotstxt_lower,
            "FooBar",
            url_disallowed
        ));
        assert!(!is_user_agent_allowed(
            robotstxt_mixed,
            "FooBar",
            url_disallowed
        ));

        assert!(is_user_agent_allowed(
            robotstxt_upper,
            "foobar",
            url_allowed
        ));
        assert!(is_user_agent_allowed(
            robotstxt_lower,
            "foobar",
            url_allowed
        ));
        assert!(is_user_agent_allowed(
            robotstxt_mixed,
            "foobar",
            url_allowed
        ));

        assert!(!is_user_agent_allowed(
            robotstxt_upper,
            "foobar",
            url_disallowed
        ));
        assert!(!is_user_agent_allowed(
            robotstxt_lower,
            "foobar",
            url_disallowed
        ));
        assert!(!is_user_agent_allowed(
            robotstxt_mixed,
            "foobar",
            url_disallowed
        ));
    }

    #[test]
    fn test_specific_user_agent() {
        let robotstxt = r#"
User-Agent: FooBot
Allow: /

User-Agent: *
Disallow: /
"#;

        let url = "http://foo.bar/x/y";

        assert!(is_user_agent_allowed(robotstxt, "FooBot", url));
        assert!(!is_user_agent_allowed(robotstxt, "BarBot", url));
    }

    // this is the test from google
    //
    //     #[test]
    //     fn test_accept_user_agent_up_to_first_space() {
    //         let robotstxt = r#"
    // User-Agent: *
    // Disallow: /
    // User-Agent: Foo Bar
    // Allow: /x/
    // Disallow: /
    // "#;
    //
    //         let url = "http://foo.bar/x/y";
    //
    //         assert!(is_user_agent_allowed(robotstxt, "Foo", url));
    //         assert!(!is_user_agent_allowed(robotstxt, "Foo Bar", url));
    //     }
    //
    // yet I think it makes more sense to allow the user agent "Foo" and "Bar" to access the url
    #[test]
    fn test_accept_user_agent_with_space() {
        let robotstxt = r#"
User-Agent: *
Disallow: /
User-Agent: Foo Bar
Allow: /x/
Disallow: /
"#;

        let url = "http://foo.bar/x/y";

        assert!(is_user_agent_allowed(robotstxt, "Foo", url));
        assert!(is_user_agent_allowed(robotstxt, "Bar", url));
        assert!(!is_user_agent_allowed(robotstxt, "Baz", url));
    }

    #[test]
    fn test_global_groups_secondary() {
        let robotstxt_empty = "";
        let robotstxt_global = r#"
user-agent: *
allow: /
user-agent: FooBot
disallow: /
"#;
        let robotstxt_only_specific = r#"
user-agent: FooBot
allow: /
user-agent: BarBot
disallow: /
user-agent: BazBot
disallow: /
"#;

        let url = "http://foo.bar/x/y";

        assert!(is_user_agent_allowed(robotstxt_empty, "FooBot", url));
        assert!(!is_user_agent_allowed(robotstxt_global, "FooBot", url));
        assert!(is_user_agent_allowed(robotstxt_global, "BarBot", url));
        assert!(is_user_agent_allowed(
            robotstxt_only_specific,
            "QuxBot",
            url
        ));
    }

    #[test]
    fn test_allow_disallow_value_case_sensitive() {
        let robotstxt_lower = r#"
user-agent: FooBot
disallow: /x/
"#;
        let robotstxt_upper = r#"
user-agent: FooBot
disallow: /X/
"#;

        let url = "http://foo.bar/x/y";

        assert!(!is_user_agent_allowed(robotstxt_lower, "FooBot", url));
        assert!(is_user_agent_allowed(robotstxt_upper, "FooBot", url));
    }

    #[test]
    fn test_longest_match() {
        let url = "http://foo.bar/x/page.html";
        let robotstxt = r#"
user-agent: FooBot
disallow: /x/page.html
allow: /x/
"#;

        assert!(!is_user_agent_allowed(robotstxt, "FooBot", url));

        let robotstxt = r#"
user-agent: FooBot
allow: /x/page.html
disallow: /x/
"#;

        assert!(is_user_agent_allowed(robotstxt, "FooBot", url));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/x/"
        ));

        let robotstxt = r#"
user-agent: FooBot
disallow:
allow:
"#;

        assert!(is_user_agent_allowed(robotstxt, "FooBot", url));

        let robotstxt = r#"
user-agent: FooBot
disallow: /
allow: /
"#;

        assert!(is_user_agent_allowed(robotstxt, "FooBot", url));

        let robotstxt = r#"
user-agent: FooBot
disallow: /x
allow: /x/
"#;

        let url_a = "http://foo.bar/x";
        let url_b = "http://foo.bar/x/";

        assert!(!is_user_agent_allowed(robotstxt, "FooBot", url_a));
        assert!(is_user_agent_allowed(robotstxt, "FooBot", url_b));

        let robotstxt = r#"
user-agent: FooBot
disallow: /x/page.html
allow: /x/page.html
"#;

        assert!(is_user_agent_allowed(robotstxt, "FooBot", url));

        let robotstxt = r#"
user-agent: FooBot
allow: /page
disallow: /*.html
"#;

        let url_a = "http://foo.bar/page";
        let url_b = "http://foo.bar/page.html";

        assert!(is_user_agent_allowed(robotstxt, "FooBot", url_a));
        assert!(!is_user_agent_allowed(robotstxt, "FooBot", url_b));

        let robotstxt = r#"
user-agent: FooBot
allow: /x/page.
disallow: /*.html
"#;
        assert!(is_user_agent_allowed(robotstxt, "FooBot", url));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/x/y.html"
        ));

        let robotstxt = r#"
User-agent: *
Disallow: /x/
User-agent: FooBot
Disallow: /y/
"#;

        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/x/page"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/y/page"
        ));
    }

    #[test]
    fn test_encoding() {
        let robotstxt = r#"
User-agent: FooBot
Disallow: /
Allow: /foo/bar?qux=taz&baz=http://foo.bar?tar&par
"#;
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/foo/bar?qux=taz&baz=http://foo.bar?tar&par"
        ));

        let robotstxt = r#"
User-agent: FooBot
Disallow: /
Allow: /foo/bar/ツ
"#;
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/foo/bar/%E3%83%84"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/foo/bar/ツ"
        ));

        let robotstxt = r#"
User-agent: FooBot
Disallow: /
Allow: /foo/bar/%E3%83%84
"#;
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/foo/bar/%E3%83%84"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/foo/bar/ツ"
        ));

        let robotstxt = r#"
User-agent: FooBot
Disallow: /
Allow: /foo/bar/%62%61%7A
"#;
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/foo/bar/baz"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/foo/bar/%62%61%7A"
        ));

        let robotstxt = r#"
User-agent: FooBot
Disallow: /
Allow: /path/file-with-a-%2A
"#;
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/path/file-with-a-%2A"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/path/file-with-a-*"
        ));
    }

    #[test]
    fn test_special_characters() {
        let robotstxt = r#"
User-agent: FooBot
Disallow: /foo/bar/quz
Allow: /foo/*/quz
"#;

        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/foo/bar/quz"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/foo/quz"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/foo//quz"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/foo/bax/quz"
        ));

        let robotstxt = r#"
User-agent: FooBot
Disallow: /foo/bar$
Allow: /foo/bar/qux
"#;

        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/foo/bar"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/foo/bar/qux"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/foo/bar/"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/foo/bar/baz"
        ));

        let robotstxt = r#"
User-agent: FooBot
# Disallow: /
Disallow: /foo/quz#qux
Allow: /
"#;
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/foo/bar"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/foo/quz"
        ));
    }

    #[test]
    fn test_index_html_is_directory() {
        let robotstxt = r#"
User-agent: *
Allow: /allowed-slash/index.html
Disallow: /
"#;

        assert!(is_user_agent_allowed(
            robotstxt,
            "foobot",
            "http://foo.bar/allowed-slash/"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "foobot",
            "http://foo.bar/allowed-slash/index.htm"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "foobot",
            "http://foo.bar/allowed-slash/index.html"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "foobot",
            "http://foo.bar/another-url/"
        ));
    }

    #[test]
    fn test_google_documentation() {
        let robotstxt = r#"
user-agent: FooBot
disallow: /
allow: /fish
"#;
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/bar"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/fish"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/fish.html"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/fish/salmon.html"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/fishheads"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/fishheads/yummy.html"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/fish.html?id=anything"
        ));

        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/Fish.asp"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/catfish"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/?id=fish"
        ));

        let robotstxt = r#"
user-agent: FooBot
disallow: /
allow: /fish*
"#;
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/bar"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/fish"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/fish.html"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/fish/salmon.html"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/fishheads"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/fishheads/yummy.html"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/fish.html?id=anything"
        ));

        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/Fish.asp"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/catfish"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/?id=fish"
        ));

        // "/fish/" does not equal "/fish"
        let robotstxt = r#"
user-agent: FooBot
disallow: /
allow: /fish/
"#;
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/bar/"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/fish/"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/fish/salmon"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/fish/?salmon"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/fish/salmon.html"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/fish/?id=anything"
        ));

        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/fish"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/fish.html"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/Fish/Salmon.html"
        ));

        // "*.php"
        let robotstxt = r#"
user-agent: FooBot
disallow: /
allow: /*.php
"#;
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/bar"
        ));

        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/filename.php"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/folder/filename.php"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/folder/filename.php?parameters"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar//folder/any.php.file.html"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/filename.php/"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/index?f=filename.php/"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/php/"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/index?php"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/windows.PHP"
        ));

        // "/*.php$"
        let robotstxt = r#"
user-agent: FooBot
disallow: /
allow: /*.php$
"#;
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/bar"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/filename.php"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/folder/filename.php"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/filename.php?parameters"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/filename.php/"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/filename.php5"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/php/"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/filename?php"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/aaaphpaaa"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar//windows.PHP"
        ));

        // "/fish*.php"
        let robotstxt = r#"
user-agent: FooBot
disallow: /
allow: /fish*.php
"#;
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/bar"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/fish.php"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/fishheads/catfish.php?parameters"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/fishheads/Fish.PHP"
        ));

        // section "order of precedence for group-member records"
        assert!(is_user_agent_allowed(
            r#"
            user-agent: FooBot
            allow: /p
            disallow: /
            "#,
            "FooBot",
            "http://foo.bar/page"
        ));
        assert!(is_user_agent_allowed(
            r#"
            user-agent: FooBot
            allow: /folder
            disallow: /folder
            "#,
            "FooBot",
            "http://foo.bar/folder/page"
        ));
        assert!(!is_user_agent_allowed(
            r#"
            user-agent: FooBot
            allow: /page
            disallow: /*.htm
            "#,
            "FooBot",
            "http://foo.bar/page.htm"
        ));
        let robotstxt = r#"
            user-agent: FooBot
            allow: /$
            disallow: /
            "#;
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/page.html"
        ));
    }

    #[test]
    fn test_comma_separated_user_agents() {
        // this is not part of the spec, but it is a reasonable
        // assumption made by some websites and should therefore be respected
        let robotstxt = r#"
User-Agent: FooBot, BarBot
Disallow: /

User-Agent: BazBot
Allow: /
"#;

        let url = "http://foo.bar/x/y";

        assert!(!is_user_agent_allowed(robotstxt, "FooBot", url));
        assert!(!is_user_agent_allowed(robotstxt, "BarBot", url));
        assert!(is_user_agent_allowed(robotstxt, "BazBot", url));
    }

    #[test]
    fn test_non_standard_line_example_sitemap() {
        let robotstxt = r#"
        user-agent: FooBot
        allow: /some/path
        user-agent: BarBot

        Sitemap: http://foo.bar/sitemap.xml
        "#;

        let robots = Robots::parse("FooBot", robotstxt).unwrap();
        assert_eq!(robots.sitemaps(), &["http://foo.bar/sitemap.xml"]);

        let robotstxt = r#"
        sitemap: http://foo.bar/sitemap.xml

        user-agent: FooBot
        allow: /some/path
        user-agent: BarBot
        "#;

        let robots = Robots::parse("FooBot", robotstxt).unwrap();
        assert_eq!(robots.sitemaps(), &["http://foo.bar/sitemap.xml"]);
    }

    #[test]
    fn test_path_params_query() {
        test_path("", "/");
        test_path("http://www.example.com", "/");
        test_path("http://www.example.com/", "/");
        test_path("http://www.example.com/a", "/a");
        test_path("http://www.example.com/a/", "/a/");
        test_path(
            "http://www.example.com/a/b?c=http://d.e/",
            "/a/b?c=http://d.e/",
        );
        test_path(
            "http://www.example.com/a/b?c=d&e=f#fragment",
            "/a/b?c=d&e=f",
        );
        test_path("a", "/a");
        test_path("a/", "/a/");
        test_path("/a", "/a");
        test_path("a/b", "/a/b");
        test_path("http://example.com?a", "/?a");
        test_path("http://example.com/a;b#c", "/a;b");
        test_path("http://example.com///a/b/c", "/a/b/c");
    }

    #[test]
    fn test_maybe_escape_pattern() {
        test_escape("http://www.example.com", "http://www.example.com");
        test_escape("/a/b/c", "/a/b/c");
        test_escape("á", "%C3%A1");
        test_escape("%C3%A1", "%C3%A1");
        test_escape("aá", "a%C3%A1");
    }

    #[test]
    fn test_params_respected() {
        let robotstxt = r#"
      user-agent: FooBot
      disallow: /*?searchTerm=
      "#;

        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/?searchTerm=someTerm"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/bar/?searchTerm=someTerm"
        ));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/bar"
        ));
    }

    #[test]
    fn test_url_fragments() {
        let robotstxt = r#"
      user-agent: FooBot
      disallow: /#fragment
      "#;

        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/#fragment"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/#fragment"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "http://foo.bar/#fragment"
        ));
    }

    #[test]
    fn test_forgiveness_disallow_variations() {
        let robotstxt = "user-agent: FooBot
        disallow: /a
        dissallow: /b
        dissalow: /c
        disalow: /d
        diasllow: /e
        disallaw: /f\n";

        for path in ["/a", "/b", "/c", "/d", "/e", "/f"] {
            assert!(!is_user_agent_allowed(robotstxt, "FooBot", path));
        }
    }

    #[test]
    fn test_forgiveness_ensure_not_too_forgiving() {
        let robotstxt = "user-agent: FooBot
        disallow:/a
        dissallow/b
        disallow    /c\n";
        assert!(!is_user_agent_allowed(robotstxt, "FooBot", "/a"));
        assert!(is_user_agent_allowed(robotstxt, "FooBot", "/b"));
        assert!(!is_user_agent_allowed(robotstxt, "FooBot", "/c"));
    }

    #[test]
    fn test_forgiveness_sitemap_variations() {
        let robotstxt = "user-agent: FooBot
        site-map: /a
        sitemap: /b
        site map: /c\n";

        let robots = Robots::parse("BarBot", robotstxt).unwrap();
        assert_eq!(robots.sitemaps(), &["/a", "/b", "/c"]);
    }

    #[test]
    fn test_forgiveness_crawl_delay_variations() {
        let robotstxt = "user-agent: FooBot
        crawl-delay: 42
        user-agent: BarBot
        crawl delay: 420
        user-agent: BazBot
        crawldelay: 360
        \n";

        let robots = Robots::parse("FooBot", robotstxt).unwrap();
        assert_eq!(robots.crawl_delay(), Some(Duration::from_secs(42)));

        let robots = Robots::parse("BarBot", robotstxt).unwrap();
        assert_eq!(robots.crawl_delay(), Some(Duration::from_secs(420)));

        let robots = Robots::parse("BazBot", robotstxt).unwrap();
        assert_eq!(robots.crawl_delay(), Some(Duration::from_secs(360)));
    }

    #[test]
    fn test_forgiveness_user_agent_variations() {
        let robotstxt = "user-agent: FooBot
        disallow: /a
        user agent: BarBot
        disallow: /b
        useragent: BazBot
        disallow: /e\n";

        assert!(!is_user_agent_allowed(robotstxt, "FooBot", "/a"));
        assert!(!is_user_agent_allowed(robotstxt, "BarBot", "/b"));
        assert!(!is_user_agent_allowed(robotstxt, "BazBot", "/e"));
    }

    #[test]
    fn test_no_leading_useragent() {
        let robotstxt = "disallow: /a
        allow: /b";

        assert!(!is_user_agent_allowed(robotstxt, "FooBot", "/a"));
        assert!(is_user_agent_allowed(robotstxt, "FooBot", "/b"));
    }

    #[test]
    fn test_leading_wildcard() {
        let robotstxt = "user-agent: *
        disallow: */a
        allow: /b";

        assert!(!is_user_agent_allowed(robotstxt, "FooBot", "/a"));
        assert!(!is_user_agent_allowed(robotstxt, "FooBot", "/c/a"));
        assert!(is_user_agent_allowed(robotstxt, "FooBot", "/b"));
    }

    #[test]
    fn test_robot_crazy_long_regex() {
        let robotstxt = "User-agent: *
        Disallow: /basket*
        # Longest string takes priority. This is necessary due to conflicting Allow rules:
        Disallow: /*?************************************************************************************donotindex=1*";

        assert!(!is_user_agent_allowed(robotstxt, "FooBot", "/basket"));
        assert!(!is_user_agent_allowed(robotstxt, "FooBot", "/basket/ball"));
        assert!(is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "/example/file?xyz=42"
        ));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "/example/file?xyz=42&donotindex=1"
        ));
    }

    #[test]
    fn test_double_return_newline() {
        let robotstxt = "\r
        User-agent: *\r\r
        Disallow: /en-AU/party\r\r\r\n\n\r\n
        User-Agent: FooBot
        Disallow: /fi-FI/party\r\r\n
        Disallow: /en-US/party\r\r\n
        \r\n\r\r\r\n\n
        Crawl-Delay: 4";

        assert!(!is_user_agent_allowed(robotstxt, "BarBot", "/en-AU/party"));

        assert!(is_user_agent_allowed(robotstxt, "FooBot", "/en-AU/party"));
        assert!(!is_user_agent_allowed(robotstxt, "FooBot", "/fi-FI/party"));
        assert!(!is_user_agent_allowed(robotstxt, "FooBot", "/en-US/party"));
    }

    #[test]
    fn test_robot_parses_crazy_long_lines() {
        let mut txt = b"Disallow: /".to_vec();
        let ending = b"AAAAAAAAAA".to_vec();
        // 10 bytes * 1_000_000 = 10MB
        for _ in 0..1_000_000 {
            txt.extend(&ending);
        }

        let robotstxt = String::from_utf8(txt).unwrap();
        assert!(Robots::parse("FooBot", &robotstxt).is_ok());
    }

    #[test]
    fn test_robot_doesnt_do_full_regex() {
        let robotstxt = "User-agent: *
        Disallow: /(Cat|Dog).html";

        assert!(is_user_agent_allowed(robotstxt, "FooBot", "/Cat.html"));
        assert!(is_user_agent_allowed(robotstxt, "FooBot", "/Dog.html"));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "/(Cat|Dog).html"
        ));
    }

    #[test]
    fn test_robot_starts_with_crawl_delay() {
        let robotstxt = "Crawl-Delay: 42
        User-Agent: *
        Disallow: /blah
        User-Agent: BarBot
        Allow: /
        Crawl-Delay: 1";

        let robots = Robots::parse("FooBot", robotstxt).unwrap();
        assert_eq!(robots.crawl_delay(), Some(Duration::from_secs(42)));

        let robots = Robots::parse("BarBot", robotstxt).unwrap();
        assert_eq!(robots.crawl_delay(), Some(Duration::from_secs(1)));
    }

    #[test]
    fn test_robot_handles_random_nulls() {
        let robotstxt = "User-Agent: *
        \x00\x00Allow: /family\x00\x00
        Disallow: /family/photos\x00\x00\x00";

        assert!(is_user_agent_allowed(robotstxt, "FooBot", "/family"));
        assert!(!is_user_agent_allowed(
            robotstxt,
            "FooBot",
            "/family/photos"
        ));
    }

    #[test]
    fn test_robot_crawl_delay_not_integer() {
        let robotstxt = b"User-Agent: FooBot
        Crawl-Delay: 4.2
        User-Agent: BarBot
        Crawl-Delay: \x41\xc2\xc3\xb1\x42";

        let robotstxt = String::from_utf8_lossy(robotstxt);

        let robots = Robots::parse("FooBot", &robotstxt).unwrap();
        assert_eq!(robots.crawl_delay(), Some(Duration::from_secs_f32(4.2)));

        let robots = Robots::parse("BarBot", &robotstxt).unwrap();
        assert_eq!(robots.crawl_delay(), None);
    }

    #[test]
    fn test_empty_disallow() {
        let robotstxt = "User-Agent: FooBot
        Disallow:
        ";

        assert!(is_user_agent_allowed(robotstxt, "FooBot", "/"));
    }
}
