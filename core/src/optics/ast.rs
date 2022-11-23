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

use super::lexer;

lalrpop_mod!(pub parser, "/optics/parser.rs");

pub static PARSER: once_cell::sync::Lazy<parser::BlocksParser> =
    once_cell::sync::Lazy::new(parser::BlocksParser::new);

#[derive(Debug, PartialEq)]
pub enum RankingTarget {
    Signal(String),
    Field(String),
}

#[derive(Debug, PartialEq)]
pub struct RankingCoeff {
    pub target: RankingTarget,
    pub score: f64,
}

#[derive(Debug, PartialEq)]
pub struct RawOptic {
    pub rules: Vec<RawRule>,
    pub rankings: Vec<RankingCoeff>,
    pub site_preferences: Vec<RawSitePreference>,
    pub discard_non_matching: bool,
}

impl From<Vec<RawOpticBlock>> for RawOptic {
    fn from(blocks: Vec<RawOpticBlock>) -> Self {
        let mut rules = Vec::new();
        let mut rankings = Vec::new();
        let mut site_preferences = Vec::new();
        let mut discard_non_matching = false;

        for block in blocks {
            match block {
                RawOpticBlock::Ranking(ranking) => rankings.push(ranking),
                RawOpticBlock::Rule(rule) => rules.push(rule),
                RawOpticBlock::SitePreference(pref) => site_preferences.push(pref),
                RawOpticBlock::DiscardNonMatching => discard_non_matching = true,
            }
        }

        RawOptic {
            rankings,
            rules,
            site_preferences,
            discard_non_matching,
        }
    }
}

#[derive(Debug)]
pub enum RawOpticBlock {
    Rule(RawRule),
    SitePreference(RawSitePreference),
    Ranking(RankingCoeff),
    DiscardNonMatching,
}

#[derive(Debug, PartialEq)]
pub struct RawRule {
    pub matches: RawMatchBlock,
    pub action: Option<RawAction>,
}

#[derive(Debug, PartialEq)]
pub enum RawSitePreference {
    Like(String),
    Dislike(String),
}

#[derive(Debug, PartialEq, Clone)]
pub struct RawMatchBlock(pub Vec<RawMatchPart>);

#[derive(Debug, PartialEq, Clone)]
pub enum RawMatchPart {
    Site(String),
    Url(String),
    Domain(String),
    Title(String),
    Description(String),
    Content(String),
}

// #[derive(Debug, Clone)]
// pub enum RawStringMatchPattern {
//     Raw(String),
//     Wildcard,
//     Delimeter,
//     Anchor,
// }

#[derive(Debug, PartialEq, Clone)]
pub enum RawAction {
    Boost(u64),
    Downrank(u64),
    Discard,
}

pub fn parse(optic: &str) -> CrateResult<RawOptic> {
    match PARSER.parse(lexer::lex(optic)) {
        Ok(blocks) => Ok(RawOptic::from(blocks)),
        Err(_) => Err(Error::Parse),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple() {
        let optic = parse(
            r#"
            // this is a normal comment
            Ranking(Signal("host_centrality"), 3);
            /*
                this is a block comment
             */
            Ranking(Signal("bm25"), 100);
            Ranking(Field("url"), 2);
            Rule {
                Matches {
                    Url("/this/is/a/*/pattern")
                }
            };
            Rule {
                Matches {
                    Url("/this/is/a/pattern"),
                    Site("example.com")
                }
            }
        "#,
        )
        .unwrap();

        assert_eq!(
            optic,
            RawOptic {
                rules: vec![
                    RawRule {
                        matches: RawMatchBlock(vec![RawMatchPart::Url(
                            "/this/is/a/*/pattern".to_string()
                        )]),
                        action: None,
                    },
                    RawRule {
                        matches: RawMatchBlock(vec![
                            RawMatchPart::Url("/this/is/a/pattern".to_string()),
                            RawMatchPart::Site("example.com".to_string()),
                        ],),
                        action: None,
                    },
                ],
                rankings: vec![
                    RankingCoeff {
                        target: RankingTarget::Signal("host_centrality".to_string()),
                        score: 3.0,
                    },
                    RankingCoeff {
                        target: RankingTarget::Signal("bm25".to_string()),
                        score: 100.0,
                    },
                    RankingCoeff {
                        target: RankingTarget::Field("url".to_string()),
                        score: 2.0,
                    },
                ],
                site_preferences: vec![],
                discard_non_matching: false,
            }
        );
    }

    #[test]
    fn actions() {
        let optic = parse(
            r#"
            Rule {
                Matches {
                    Url("/this/is/a/*/pattern")
                },
                Action(Boost(2))
            };
            Rule {
                Matches {
                    Site("example.com"),
                },
                Action(Downrank(4))
            };
        "#,
        )
        .unwrap();

        assert_eq!(
            optic,
            RawOptic {
                rules: vec![
                    RawRule {
                        matches: RawMatchBlock(vec![RawMatchPart::Url(
                            "/this/is/a/*/pattern".to_string()
                        )]),
                        action: Some(RawAction::Boost(2)),
                    },
                    RawRule {
                        matches: RawMatchBlock(vec![RawMatchPart::Site("example.com".to_string())],),
                        action: Some(RawAction::Downrank(4)),
                    },
                ],
                rankings: vec![],
                site_preferences: vec![],
                discard_non_matching: false,
            }
        );
    }

    #[test]
    fn discard_non_matching() {
        let optic = parse(
            r#"
            DiscardNonMatching;
            Rule {
                Matches {
                    Url("/this/is/a/*/pattern")
                },
                Action(Boost(2))
            };
            Rule {
                Matches {
                    Site("example.com"),
                },
                Action(Downrank(4))
            };
        "#,
        )
        .unwrap();

        assert_eq!(
            optic,
            RawOptic {
                rules: vec![
                    RawRule {
                        matches: RawMatchBlock(vec![RawMatchPart::Url(
                            "/this/is/a/*/pattern".to_string()
                        )]),
                        action: Some(RawAction::Boost(2)),
                    },
                    RawRule {
                        matches: RawMatchBlock(vec![RawMatchPart::Site("example.com".to_string())],),
                        action: Some(RawAction::Downrank(4)),
                    },
                ],
                rankings: vec![],
                site_preferences: vec![],
                discard_non_matching: true,
            }
        );
    }

    #[test]
    fn quickstart_parse() {
        assert!(parse(include_str!("../../testcases/optics/quickstart.optic")).is_ok());
    }

    #[test]
    fn hacker_news_parse() {
        assert!(parse(include_str!("../../testcases/optics/hacker_news.optic")).is_ok());
    }

    #[test]
    fn copycats_parse() {
        assert!(parse(include_str!(
            "../../testcases/optics/copycats_removal.optic"
        ))
        .is_ok());
    }
}
