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

pub mod ast;
mod lexer;

use ast::RankingCoeff;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use thiserror::Error;
use utoipa::ToSchema;

use self::ast::{RawAction, RawMatchPart, RawOptic, RawRule};
pub use lexer::lex;
pub use lexer::Token;

pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("Unexpected EOF")]
    UnexpectedEof {
        /// expected one of these tokens but got EOF
        expected: Vec<String>,
    },

    #[error("Unexpected token")]
    UnexpectedToken {
        /// got this token
        token: (usize, String, usize),
        /// expected one of these tokens
        expected: Vec<String>,
    },

    #[error("Unrecognized token")]
    UnrecognizedToken {
        /// got this token
        token: (usize, String, usize),
    },

    #[error("Could not parse as a number")]
    NumberParse { token: (usize, String, usize) },

    #[error("Unknown parse error")]
    Unknown(usize, usize),

    #[error("Ranking stages mismatch")]
    RankingStagesMismatch,

    #[error("Unsupported pattern")]
    Pattern,
}

pub fn parse(optic: &str) -> Result<Optic> {
    let raw_optic = ast::parse(optic)?;

    Optic::try_from(raw_optic)
}

impl TryFrom<RawOptic> for Optic {
    type Error = Error;

    fn try_from(raw: RawOptic) -> Result<Self> {
        let mut rules = Vec::new();

        for rule in raw.rules {
            rules.push(Rule::try_from(rule)?);
        }

        let mut liked_hosts = Vec::new();
        let mut disliked_hosts = Vec::new();

        for pref in raw.host_preferences {
            match pref {
                ast::RawHostPreference::Like(host) => liked_hosts.push(host),
                ast::RawHostPreference::Dislike(host) => disliked_hosts.push(host),
            }
        }

        Ok(Self {
            rules,
            rankings: raw.rankings,
            discard_non_matching: raw.discard_non_matching,
            host_rankings: HostRankings {
                liked: liked_hosts,
                disliked: disliked_hosts,
                blocked: Vec::new(), // blocked hosts are handled by `$discard` syntax.
            },
        })
    }
}

impl TryFrom<RawRule> for Rule {
    type Error = Error;

    fn try_from(raw: RawRule) -> Result<Self> {
        let mut matches = Vec::new();

        for matching in raw.matches.0 {
            matches.push(matching.try_into()?);
        }

        Ok(Rule {
            matches,
            action: raw.action.map(Action::from).unwrap_or(Action::Boost(0)),
        })
    }
}

impl From<RawAction> for Action {
    fn from(value: RawAction) -> Self {
        match value {
            RawAction::Boost(boost) => Action::Boost(boost),
            RawAction::Downrank(down_boost) => Action::Downrank(down_boost),
            RawAction::Discard => Action::Discard,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Matching {
    pub pattern: Vec<PatternPart>,
    pub location: MatchLocation,
}

impl ToString for Matching {
    fn to_string(&self) -> String {
        let mut s = String::new();
        match self.location {
            MatchLocation::Site => s.push_str("Site"),
            MatchLocation::Url => s.push_str("Url"),
            MatchLocation::Domain => s.push_str("Domain"),
            MatchLocation::Title => s.push_str("Title"),
            MatchLocation::Description => s.push_str("Description"),
            MatchLocation::Content => s.push_str("Content"),
            MatchLocation::MicroformatTag => s.push_str("MicroformatTag"),
            MatchLocation::Schema => s.push_str("Schema"),
        }
        s.push('(');
        s.push('"');

        for part in &self.pattern {
            s.push_str(&part.to_string());
        }

        s.push('"');
        s.push(')');

        s
    }
}

impl TryFrom<RawMatchPart> for Matching {
    type Error = Error;

    fn try_from(raw: RawMatchPart) -> Result<Self> {
        let (s, loc) = match raw {
            RawMatchPart::Site(s) => (s, MatchLocation::Site),
            RawMatchPart::Url(s) => (s, MatchLocation::Url),
            RawMatchPart::Domain(s) => (s, MatchLocation::Domain),
            RawMatchPart::Title(s) => (s, MatchLocation::Title),
            RawMatchPart::Description(s) => (s, MatchLocation::Description),
            RawMatchPart::Content(s) => (s, MatchLocation::Content),
            RawMatchPart::MicroformatTag(s) => (s, MatchLocation::MicroformatTag),
            RawMatchPart::Schema(s) => (s, MatchLocation::Schema),
        };

        let mut pattern = Vec::new();

        if matches!(&loc, MatchLocation::Schema) {
            for tok in PatternToken::lex(&s) {
                match tok {
                    PatternToken::Raw(s) => pattern.push(PatternPart::Raw(s)),
                    PatternToken::Wildcard => return Err(Error::Pattern),
                    PatternToken::Anchor => return Err(Error::Pattern),
                }
            }
        } else {
            for tok in PatternToken::lex(&s) {
                match tok {
                    PatternToken::Raw(s) => pattern.push(PatternPart::Raw(s)),
                    PatternToken::Wildcard => pattern.push(PatternPart::Wildcard),
                    PatternToken::Anchor => pattern.push(PatternPart::Anchor),
                }
            }
        }

        Ok(Self {
            location: loc,
            pattern,
        })
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
enum PatternToken {
    Raw(String),

    Wildcard,

    Anchor,
}
impl PatternToken {
    fn lex(s: &str) -> Vec<Self> {
        let mut tokens = Vec::new();

        let mut raw: Option<String> = None;

        for c in s.chars() {
            match c {
                '*' => {
                    if let Some(cur_raw) = raw {
                        let cur_raw = cur_raw.trim().to_string();
                        tokens.push(Self::Raw(cur_raw));
                        raw = None;
                    }

                    tokens.push(Self::Wildcard);
                }
                '|' => {
                    if let Some(cur_raw) = raw {
                        let cur_raw = cur_raw.trim().to_string();
                        tokens.push(Self::Raw(cur_raw));
                        raw = None;
                    }

                    tokens.push(Self::Anchor);
                }
                _ => {
                    if raw.is_none() {
                        raw = Some(String::new());
                    }

                    raw.as_mut().unwrap().push(c);
                }
            }
        }

        if let Some(raw) = raw {
            let raw = raw.trim().to_string();
            tokens.push(Self::Raw(raw));
        }

        tokens
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum PatternPart {
    Raw(String),
    Wildcard,
    Anchor,
}

impl ToString for PatternPart {
    fn to_string(&self) -> String {
        match self {
            PatternPart::Raw(s) => s.to_string(),
            PatternPart::Wildcard => "*".to_string(),
            PatternPart::Anchor => "|".to_string(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum MatchLocation {
    Site,
    Url,
    Domain,
    Title,
    Description,
    Content,
    MicroformatTag,
    Schema,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
pub enum Action {
    Boost(u64),
    Downrank(u64),
    Discard,
}

impl ToString for Action {
    fn to_string(&self) -> String {
        let mut res = String::new();
        res.push_str("Action(");

        match self {
            Action::Boost(b) => res.push_str(&format!("Boost({})", b)),
            Action::Downrank(d) => res.push_str(&format!("Downrank({})", d)),
            Action::Discard => res.push_str("Discard"),
        }

        res.push(')');

        res
    }
}

#[derive(Debug, PartialEq, Default, Clone, Serialize, Deserialize)]
pub struct Optic {
    pub rankings: Vec<RankingCoeff>,
    pub host_rankings: HostRankings,
    pub rules: Vec<Rule>,
    pub discard_non_matching: bool,
}

impl Optic {
    pub fn parse(optic: &str) -> Result<Self> {
        parse(optic)
    }
}

impl ToString for Optic {
    fn to_string(&self) -> String {
        let mut res = String::new();

        if self.discard_non_matching {
            res.push_str("DiscardNonMatching;\n");
        }

        for rule in &self.rules {
            res.push_str(&rule.to_string());
        }

        for ranking in &self.rankings {
            res.push_str(&format!("{};\n", ranking.to_string()));
        }

        res.push_str(&self.host_rankings.to_string());

        res
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Rule {
    pub matches: Vec<Matching>,
    pub action: Action,
}

impl ToString for Rule {
    fn to_string(&self) -> String {
        let mut res = String::new();

        res.push_str("Rule {\n");
        if !self.matches.is_empty() {
            res.push_str("\tMatches {\n");
            for m in &self.matches {
                res.push_str(&format!("\t\t{},\n", m.to_string()));
            }
            res.push_str("\t}")
        }

        res.push_str(&format!("\t{}\n", self.action.to_string()));
        res.push_str("};\n");

        res
    }
}

#[derive(Debug, PartialEq, Default, Serialize, Deserialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct HostRankings {
    pub liked: Vec<String>,
    pub disliked: Vec<String>,
    pub blocked: Vec<String>,
}

impl ToString for HostRankings {
    fn to_string(&self) -> String {
        let mut res = String::new();

        for liked in &self.liked {
            res.push_str(&format!("Like(Domain(\"{}\"));\n", liked));
        }

        for disliked in &self.disliked {
            res.push_str(&format!("Dislike(Domain(\"{}\"));\n", disliked));
        }

        for blocked in &self.blocked {
            let rule = Rule {
                matches: vec![Matching {
                    pattern: vec![
                        PatternPart::Anchor,
                        PatternPart::Raw(blocked.clone()),
                        PatternPart::Anchor,
                    ],
                    location: MatchLocation::Domain,
                }],
                action: Action::Discard,
            };

            res.push_str(&rule.to_string());
        }

        res
    }
}

impl HostRankings {
    pub fn rules(&self) -> Vec<Rule> {
        self.blocked
            .iter()
            .map(|host| Rule {
                matches: vec![Matching {
                    pattern: vec![
                        PatternPart::Anchor,
                        PatternPart::Raw(host.clone()),
                        PatternPart::Anchor,
                    ],
                    location: MatchLocation::Domain,
                }],
                action: Action::Discard,
            })
            .collect()
    }

    pub fn into_optic(self) -> Optic {
        Optic {
            host_rankings: self,
            ..Default::default()
        }
    }

    pub fn merge_into(&mut self, host_rankings: HostRankings) {
        self.liked.extend(host_rankings.liked);
        self.disliked.extend(host_rankings.disliked);
        self.blocked.extend(host_rankings.blocked);
    }
}

#[cfg(test)]
mod tests {
    use crate::ast::{RankingCoeff, RankingTarget};

    use super::*;
    #[test]
    fn pattern_part() {
        let lex = PatternToken::lex("|test|");

        assert_eq!(
            lex,
            vec![
                PatternToken::Anchor,
                PatternToken::Raw("test".to_string()),
                PatternToken::Anchor
            ]
        );
        let lex = PatternToken::lex("test * string");

        assert_eq!(
            lex,
            vec![
                PatternToken::Raw("test".to_string()),
                PatternToken::Wildcard,
                PatternToken::Raw("string".to_string()),
            ]
        );
    }

    #[test]
    fn export() {
        let optic = Optic {
            rankings: vec![RankingCoeff {
                target: RankingTarget::Signal("bm25".to_string()),
                value: 1.0,
            }],
            host_rankings: HostRankings {
                liked: vec!["liked.com".to_string()],
                disliked: vec!["disliked.com".to_string()],
                blocked: vec![],
            },
            rules: vec![Rule {
                matches: vec![Matching {
                    pattern: vec![
                        PatternPart::Anchor,
                        PatternPart::Raw("test".to_string()),
                        PatternPart::Anchor,
                    ],
                    location: MatchLocation::Site,
                }],
                action: Action::Boost(0),
            }],
            discard_non_matching: true,
        };

        let exported = optic.to_string();

        println!("{:}", exported);

        let parsed = Optic::parse(&exported).unwrap();

        assert_eq!(optic, parsed);
    }
}
