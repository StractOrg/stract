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

use crate::Error;
use crate::Result as CrateResult;
use lalrpop_util::lalrpop_mod;
use regex::Regex;

lalrpop_mod!(pub parser, "/ranking/goggles/parser.rs");

pub static PARSER: once_cell::sync::Lazy<parser::BlocksParser> =
    once_cell::sync::Lazy::new(parser::BlocksParser::new);

#[derive(Debug, PartialEq, Eq)]
pub enum Target {
    Signal(String),
    Field(String),
}

#[derive(Debug, PartialEq, Eq)]
pub struct RawAlteration {
    pub target: Target,
    pub score: String,
}

#[derive(Debug, PartialEq, Eq)]
pub struct RawGoggle {
    pub comments: Vec<Comment>,
    pub instructions: Vec<RawInstruction>,
    pub alterations: Vec<RawAlteration>,
}

impl From<Vec<GoggleBlock>> for RawGoggle {
    fn from(blocks: Vec<GoggleBlock>) -> Self {
        let mut alterations = Vec::new();
        let mut comments = Vec::new();
        let mut instructions = Vec::new();

        for block in blocks {
            match block {
                GoggleBlock::Comment(comment) => comments.push(comment),
                GoggleBlock::Instruction(instruction) => instructions.push(instruction),
                GoggleBlock::Alteration(alteration) => alterations.push(alteration),
            }
        }

        RawGoggle {
            comments,
            instructions,
            alterations,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum GoggleBlock {
    Comment(Comment),
    Instruction(RawInstruction),
    Alteration(RawAlteration),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Comment {
    Basic(String),
    Header { key: String, value: String },
}

#[derive(Debug, PartialEq, Eq)]
pub struct RawInstruction {
    pub patterns: Vec<RawPatternPart>,
    pub options: Vec<RawPatternOption>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RawPatternPart {
    Raw(String),
    Wildcard,
    Delimeter,
    Anchor,
}

#[derive(Debug, PartialEq, Eq)]
pub enum RawPatternOption {
    Site(String),
    InUrl,
    InTitle,
    InDescription,
    InContent,
    Action(RawAction),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RawAction {
    Boost(String),
    Downrank(String),
    Discard,
}

pub fn parse(goggle: &str) -> CrateResult<RawGoggle> {
    let newlines = Regex::new(r"[\n]+").unwrap();
    let clean = newlines.replace_all(goggle.trim(), "\n").to_string();
    let clean = clean.trim().replace('\n', ";").replace('\r', ";");

    match PARSER.parse(clean.as_str()) {
        Ok(blocks) => Ok(RawGoggle::from(blocks)),
        Err(_) => Err(Error::Parse),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let goggle = parse(
            r#"
            ! name: test
            ! this is a normal comment
            @host_centrality = 3
            ! this is a normal comment
            @bm25 = 100
            @field_url = 2
            /this/is/*/pattern
            /blog/$site=example.com
        "#,
        )
        .unwrap();

        assert_eq!(
            goggle.alterations,
            vec![
                RawAlteration {
                    target: Target::Signal("host_centrality".to_string()),
                    score: "3".to_string(),
                },
                RawAlteration {
                    target: Target::Signal("bm25".to_string()),
                    score: "100".to_string()
                },
                RawAlteration {
                    target: Target::Field("url".to_string()),
                    score: "2".to_string()
                }
            ]
        );

        assert_eq!(
            goggle.comments,
            vec![
                Comment::Header {
                    key: "name".to_string(),
                    value: "test".to_string()
                },
                Comment::Basic("! this is a normal comment".to_string()),
                Comment::Basic("! this is a normal comment".to_string()),
            ]
        );

        assert_eq!(
            goggle.instructions,
            vec![
                RawInstruction {
                    patterns: vec![
                        RawPatternPart::Raw("/this/is/".to_string()),
                        RawPatternPart::Wildcard,
                        RawPatternPart::Raw("/pattern".to_string())
                    ],
                    options: vec![]
                },
                RawInstruction {
                    patterns: vec![RawPatternPart::Raw("/blog/".to_string()),],
                    options: vec![RawPatternOption::Site("example.com".to_string())]
                },
            ]
        )
    }

    #[test]
    fn advanced_urls() {
        let goggle = parse(
            r#"
            https://www.example.com?@hej
        "#,
        )
        .unwrap();

        assert_eq!(
            goggle.instructions,
            vec![RawInstruction {
                patterns: vec![RawPatternPart::Raw(
                    "https://www.example.com?@hej".to_string()
                ),],
                options: vec![]
            },]
        );

        let goggle = parse(
            r#"
            https://www.example.com?@hej$site=https://www.example.com
        "#,
        )
        .unwrap();

        assert_eq!(
            goggle.instructions,
            vec![RawInstruction {
                patterns: vec![RawPatternPart::Raw(
                    "https://www.example.com?@hej".to_string()
                ),],
                options: vec![RawPatternOption::Site(
                    "https://www.example.com".to_string()
                )]
            },]
        );
    }

    #[test]
    fn ignore_consecutive_newlines() {
        let goggle = parse(
            r#"
            |pattern1|




            pattern2^
        "#,
        )
        .unwrap();
        assert_eq!(
            goggle.instructions,
            vec![
                RawInstruction {
                    patterns: vec![
                        RawPatternPart::Anchor,
                        RawPatternPart::Raw("pattern1".to_string()),
                        RawPatternPart::Anchor,
                    ],
                    options: vec![]
                },
                RawInstruction {
                    patterns: vec![
                        RawPatternPart::Raw("pattern2".to_string()),
                        RawPatternPart::Delimeter,
                    ],
                    options: vec![]
                },
            ]
        )
    }

    #[test]
    fn quickstart_parse() {
        assert!(parse(include_str!("../../../testcases/goggles/quickstart.goggle")).is_ok());
    }

    #[test]
    fn hacker_news_parse() {
        assert!(parse(include_str!(
            "../../../testcases/goggles/hacker_news.goggle"
        ))
        .is_ok());
    }

    #[test]
    fn copycats_parse() {
        assert!(parse(include_str!(
            "../../../testcases/goggles/copycats_removal.goggle"
        ))
        .is_ok());
    }
}
