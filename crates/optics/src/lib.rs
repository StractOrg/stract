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
use itertools::Itertools;
use std::fmt::Display;
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
        let mut blocked = Vec::new();

        for rule in raw.rules {
            let rule = Rule::try_from(rule)?;

            let blocked_sites = rule.as_blocked_sites();

            if blocked_sites.is_empty() {
                rules.push(rule);
            } else {
                blocked.extend(blocked_sites);
            }
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
                blocked,
            },
        })
    }
}

impl TryFrom<RawRule> for Rule {
    type Error = Error;

    fn try_from(raw: RawRule) -> Result<Self> {
        let RawRule { matches, action } = raw;

        let matches = matches
            .into_iter()
            .map(|m| {
                m.0.into_iter()
                    .map(Matching::try_from)
                    .collect::<Result<_>>()
            })
            .collect::<Result<_>>()?;

        Ok(Rule {
            matches,
            action: action.map_or(Action::Boost(0), Action::from),
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

#[derive(
    Debug,
    PartialEq,
    Eq,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct Matching {
    pub pattern: Vec<PatternPart>,
    pub location: MatchLocation,
}

impl Display for Matching {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self.location {
            MatchLocation::Site => "Site",
            MatchLocation::Url => "Url",
            MatchLocation::Domain => "Domain",
            MatchLocation::Title => "Title",
            MatchLocation::Description => "Description",
            MatchLocation::Content => "Content",
            MatchLocation::MicroformatTag => "MicroformatTag",
            MatchLocation::Schema => "Schema",
        };
        write!(f, "{s}(\"")?;

        for part in &self.pattern {
            write!(f, "{part}")?;
        }

        write!(f, "\")")
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

        for tok in PatternToken::lex(&s) {
            match tok {
                PatternToken::Raw(s) => pattern.push(PatternPart::Raw(s)),
                PatternToken::Wildcard => pattern.push(PatternPart::Wildcard),
                PatternToken::Anchor => pattern.push(PatternPart::Anchor),
            }
        }

        Ok(Self {
            location: loc,
            pattern,
        })
    }
}

#[derive(
    Debug,
    PartialEq,
    Eq,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
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

#[derive(
    Debug,
    PartialEq,
    Eq,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub enum PatternPart {
    Raw(String),
    Wildcard,
    Anchor,
}

impl Display for PatternPart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PatternPart::Raw(s) => write!(f, "{s}"),
            PatternPart::Wildcard => write!(f, "*"),
            PatternPart::Anchor => write!(f, "|"),
        }
    }
}

#[derive(
    Debug,
    PartialEq,
    Eq,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
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

#[derive(
    Debug,
    PartialEq,
    Eq,
    Clone,
    Copy,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub enum Action {
    Boost(u64),
    Downrank(u64),
    Discard,
}

impl Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Action(")?;

        match self {
            Action::Boost(b) => write!(f, "Boost({b})")?,
            Action::Downrank(d) => write!(f, "Downrank({d})")?,
            Action::Discard => write!(f, "Discard")?,
        }

        write!(f, ")")
    }
}

#[derive(
    Debug,
    PartialEq,
    Default,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
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

impl Display for Optic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.discard_non_matching {
            writeln!(f, "DiscardNonMatching;")?;
        }

        for rule in &self.rules {
            write!(f, "{rule}")?;
        }

        for ranking in &self.rankings {
            writeln!(f, "{ranking};")?;
        }

        write!(f, "{}", self.host_rankings)
    }
}

#[derive(
    Debug,
    PartialEq,
    Eq,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
pub struct Rule {
    /// A list of matchings, structured as an OR of ANDs (i.e. the rule matches if all of the matchings inside one list match).
    pub matches: Vec<Vec<Matching>>,
    /// What action to take if the rule matches.
    pub action: Action,
}
impl Rule {
    /// If the rule is on the form `Rule { Matches { Site("|...|") }*, Action(Discard) }`, return the sites to block.
    /// If the rule is not on this exact form, return an empty vector instead.
    fn as_blocked_sites(&self) -> Vec<String> {
        let mut res = Vec::new();

        if self.action == Action::Discard {
            for matching in &self.matches {
                if matching.len() != 1 {
                    return Vec::new();
                }

                let matching = &matching[0];

                if matching.pattern.len() != 3 {
                    return Vec::new();
                }

                if matching.location == MatchLocation::Site
                    && matching.pattern[0] == PatternPart::Anchor
                    && matching.pattern[2] == PatternPart::Anchor
                {
                    if let PatternPart::Raw(site) = &matching.pattern[1] {
                        res.push(site.clone());
                    } else {
                        return Vec::new();
                    }
                } else {
                    return Vec::new();
                }
            }
        }

        res
    }
}

impl Display for Rule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Rule {{")?;
        if !self.matches.is_empty() {
            for matcher in &self.matches {
                writeln!(f, "\tMatches {{")?;
                for m in matcher {
                    writeln!(f, "\t\t{m},")?;
                }
                writeln!(f, "\t}},")?;
            }
        }

        writeln!(f, "\t{}\n}};", self.action)
    }
}

#[derive(
    Debug,
    PartialEq,
    Default,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    Clone,
    ToSchema,
)]
#[serde(rename_all = "camelCase")]
pub struct HostRankings {
    pub liked: Vec<String>,
    pub disliked: Vec<String>,
    pub blocked: Vec<String>,
}

impl Display for HostRankings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for liked in &self.liked {
            writeln!(f, "Like(Site(\"{liked}\"));")?;
        }

        for disliked in &self.disliked {
            writeln!(f, "Dislike(Site(\"{disliked}\"));")?;
        }

        let matches = self
            .blocked
            .iter()
            .map(|host| {
                host.strip_prefix("www.")
                    .map(std::string::ToString::to_string)
                    .unwrap_or(host.clone())
            })
            .map(|host| {
                vec![Matching {
                    pattern: vec![
                        PatternPart::Anchor,
                        PatternPart::Raw(host),
                        PatternPart::Anchor,
                    ],
                    location: MatchLocation::Site,
                }]
            })
            .collect_vec();

        if !matches.is_empty() {
            let rule = Rule {
                matches,
                action: Action::Discard,
            };

            write!(f, "{rule}")?;
        }

        Ok(())
    }
}

impl HostRankings {
    pub fn empty() -> Self {
        Self {
            liked: Vec::new(),
            disliked: Vec::new(),
            blocked: Vec::new(),
        }
    }

    #[must_use]
    pub fn rules(&self) -> Rule {
        let matches: Vec<_> = self
            .blocked
            .iter()
            .map(|host| {
                host.strip_prefix("www.")
                    .map(std::string::ToString::to_string)
                    .unwrap_or(host.clone())
            })
            .map(|host| {
                vec![Matching {
                    pattern: vec![
                        PatternPart::Anchor,
                        PatternPart::Raw(host.clone()),
                        PatternPart::Anchor,
                    ],
                    location: MatchLocation::Site,
                }]
            })
            .collect();

        Rule {
            matches,
            action: Action::Discard,
        }
    }

    #[must_use]
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
    use crate::ast::RankingTarget;

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
                matches: vec![vec![Matching {
                    pattern: vec![
                        PatternPart::Anchor,
                        PatternPart::Raw("test".to_string()),
                        PatternPart::Anchor,
                    ],
                    location: MatchLocation::Site,
                }]],
                action: Action::Boost(0),
            }],
            discard_non_matching: true,
        };

        let exported = optic.to_string();

        println!("{exported:}");

        let parsed = Optic::parse(&exported).unwrap();

        assert_eq!(optic, parsed);
    }
}
