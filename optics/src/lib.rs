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

use std::convert::TryFrom;

use ast::RankingCoeff;
use logos::Logos;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use self::ast::{RawAction, RawMatchPart, RawOptic, RawRule};
pub use lexer::lex;
pub use lexer::Token;

pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("Unexpected EOF")]
    UnexpectedEOF {
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

        let mut liked_sites = Vec::new();
        let mut disliked_sites = Vec::new();

        for pref in raw.site_preferences {
            match pref {
                ast::RawSitePreference::Like(site) => liked_sites.push(site),
                ast::RawSitePreference::Dislike(site) => disliked_sites.push(site),
            }
        }

        Ok(Self {
            rules,
            rankings: raw.rankings,
            discard_non_matching: raw.discard_non_matching,
            site_rankings: SiteRankings {
                liked: liked_sites,
                disliked: disliked_sites,
                blocked: Vec::new(), // blocked sites are handled by `$discard` syntax.
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
            action: raw.action.map(Action::from).unwrap_or(Action::Boost(1)),
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

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Matching {
    pub pattern: Vec<PatternPart>,
    pub location: MatchLocation,
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
            RawMatchPart::Schema(s) => (s, MatchLocation::Schema),
        };

        let mut pattern = Vec::new();

        if matches!(&loc, MatchLocation::Schema) {
            for tok in PatternToken::lexer(&s) {
                match tok {
                    PatternToken::Raw(s) => pattern.push(PatternPart::Raw(s)),
                    PatternToken::Wildcard => return Err(Error::Pattern),
                    PatternToken::Anchor => return Err(Error::Pattern),
                    PatternToken::Error => unreachable!("no patterns should fail lexing"),
                }
            }
        } else {
            for tok in PatternToken::lexer(&s) {
                match tok {
                    PatternToken::Raw(s) => pattern.push(PatternPart::Raw(s)),
                    PatternToken::Wildcard => pattern.push(PatternPart::Wildcard),
                    PatternToken::Anchor => pattern.push(PatternPart::Anchor),
                    PatternToken::Error => unreachable!("no patterns should fail lexing"),
                }
            }
        }

        Ok(Self {
            location: loc,
            pattern,
        })
    }
}

#[derive(Logos, Debug)]
enum PatternToken {
    #[regex(".*", |lex| lex.slice().to_string())]
    Raw(String),

    #[token("*")]
    Wildcard,

    #[token("|")]
    Anchor,

    #[error]
    Error,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum PatternPart {
    Raw(String),
    Wildcard,
    Anchor,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum MatchLocation {
    Site,
    Url,
    Domain,
    Title,
    Description,
    Content,
    Schema,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Action {
    Boost(u64),
    Downrank(u64),
    Discard,
}

#[derive(Debug, Default, Clone)]
pub struct Optic {
    pub rankings: Vec<RankingCoeff>,
    pub site_rankings: SiteRankings,
    pub rules: Vec<Rule>,
    pub discard_non_matching: bool,
}

impl Optic {
    pub fn parse(optic: &str) -> Result<Self> {
        parse(optic)
    }

    pub fn try_merge(mut self, mut other: Self) -> Result<Self> {
        self.rules.append(&mut other.rules);

        self.rankings.append(&mut other.rankings);

        self.discard_non_matching |= other.discard_non_matching;

        self.site_rankings
            .liked
            .append(&mut other.site_rankings.liked);

        self.site_rankings
            .disliked
            .append(&mut other.site_rankings.disliked);

        self.site_rankings
            .blocked
            .append(&mut other.site_rankings.blocked);

        Ok(self)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Rule {
    pub matches: Vec<Matching>,
    pub action: Action,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct SiteRankings {
    pub liked: Vec<String>,
    pub disliked: Vec<String>,
    pub blocked: Vec<String>,
}

impl SiteRankings {
    pub fn rules(&self) -> Vec<Rule> {
        self.blocked
            .iter()
            .map(|site| Rule {
                matches: vec![Matching {
                    pattern: vec![PatternPart::Raw(site.clone())],
                    location: MatchLocation::Site,
                }],
                action: Action::Discard,
            })
            .collect()
    }

    pub fn into_optic(self) -> Optic {
        Optic {
            site_rankings: self,
            ..Default::default()
        }
    }
}
