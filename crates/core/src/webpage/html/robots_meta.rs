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

use std::str::FromStr;

use crate::{
    enum_map::{EnumSet, InsertEnumMapKey},
    Error, Result,
};

use super::Html;

#[derive(Debug)]
pub enum RobotsMeta {
    NoIndex,
    NoFollow,
}

impl FromStr for RobotsMeta {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "noindex" => Ok(RobotsMeta::NoIndex),
            "nofollow" => Ok(RobotsMeta::NoFollow),
            _ => Err(Error::UnknownRobotsMetaTag.into()),
        }
    }
}

impl InsertEnumMapKey for RobotsMeta {
    fn into_usize(self) -> usize {
        match self {
            RobotsMeta::NoIndex => 0,
            RobotsMeta::NoFollow => 1,
        }
    }
}

impl Html {
    pub fn parse_robots_meta(&self) -> Option<EnumSet<RobotsMeta>> {
        let mut robots = EnumSet::new();

        for node in self.root.select("meta").unwrap() {
            if let Some(element) = node.as_node().as_element() {
                if let Some(name) = element.attributes.borrow().get("name") {
                    if name == "robots" {
                        if let Some(content) = element.attributes.borrow().get("content") {
                            for part in content.split(',') {
                                let part = part.trim();
                                if let Ok(meta) = part.parse::<RobotsMeta>() {
                                    robots.insert(meta);
                                }
                            }
                        }
                    }
                }
            }
        }

        if robots.is_empty() {
            None
        } else {
            Some(robots)
        }
    }

    pub fn is_no_index(&self) -> bool {
        self.robots
            .as_ref()
            .map(|robots| robots.contains(RobotsMeta::NoIndex))
            .unwrap_or(false)
    }

    pub fn is_no_follow(&self) -> bool {
        self.robots
            .as_ref()
            .map(|robots| robots.contains(RobotsMeta::NoFollow))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn robots_meta_tag() {
        let html = Html::parse(
            r#"
            <html>
                <head>
                    <meta name="robots" content="noindex, nofollow" />
                </head>
                <body>
                </body>
            </html>
        "#,
            "https://www.example.com/whatever",
        )
        .unwrap();

        assert!(html.is_no_index());
        assert!(html.is_no_follow());

        let html = Html::parse(
            r#"
            <html>
                <head>
                    <meta name="robots" content="noindex,nofollow" />
                </head>
                <body>
                </body>
            </html>
        "#,
            "https://www.example.com/whatever",
        )
        .unwrap();

        assert!(html.is_no_index());
        assert!(html.is_no_follow());

        let html = Html::parse(
            r#"
            <html>
                <head>
                    <meta name="robots" content="noindex" />
                </head>
                <body>
                </body>
            </html>
        "#,
            "https://www.example.com/whatever",
        )
        .unwrap();

        assert!(html.is_no_index());
        assert!(!html.is_no_follow());

        let html = Html::parse(
            r#"
            <html>
                <head>
                    <meta name="robots" content="nofollow" />
                </head>
                <body>
                </body>
            </html>
        "#,
            "https://www.example.com/whatever",
        )
        .unwrap();

        assert!(!html.is_no_index());
        assert!(html.is_no_follow());

        let html = Html::parse(
            r#"
            <html>
                <head>
                </head>
                <body>
                </body>
            </html>
        "#,
            "https://www.example.com/whatever",
        )
        .unwrap();

        assert!(!html.is_no_index());
        assert!(!html.is_no_follow());
    }
}
