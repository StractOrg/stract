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
// along with this program.  If not, see <https://www.gnu.org/licenses/>

use crate::bangs::BANG_PREFIXES;

#[derive(Debug, Clone)]
pub struct TermCompound {
    pub terms: Vec<SimpleTerm>,
}

#[derive(Debug, Clone)]
pub struct CompoundAwareTerm {
    pub term: Term,
    pub adjacent_terms: Vec<TermCompound>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SimpleOrPhrase {
    Simple(SimpleTerm),
    Phrase(Vec<String>),
}

impl SimpleOrPhrase {
    pub fn as_string(&self) -> String {
        match self {
            SimpleOrPhrase::Simple(simple) => simple.as_str().to_string(),
            SimpleOrPhrase::Phrase(phrase) => phrase.join(" "),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SimpleTerm(String);
impl SimpleTerm {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl From<String> for SimpleTerm {
    fn from(value: String) -> Self {
        SimpleTerm(value)
    }
}

impl From<SimpleTerm> for String {
    fn from(value: SimpleTerm) -> Self {
        value.0
    }
}
impl std::fmt::Display for SimpleOrPhrase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SimpleOrPhrase::Simple(s) => write!(f, "{}", s.as_str()),
            SimpleOrPhrase::Phrase(p) => write!(f, "\"{}\"", p.join(" ")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Term {
    SimpleOrPhrase(SimpleOrPhrase),
    Site(String),
    Title(SimpleOrPhrase),
    Body(SimpleOrPhrase),
    Url(SimpleOrPhrase),
    PossibleBang(String),
    Not(Box<Term>),
}

impl std::fmt::Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Term::SimpleOrPhrase(term) => write!(f, "{}", term),
            Term::Not(term) => write!(f, "-{}", term),
            Term::Site(site) => write!(f, "site:{}", site),
            Term::Title(title) => write!(f, "intitle:{}", title),
            Term::Body(body) => write!(f, "inbody:{}", body),
            Term::Url(url) => write!(f, "inurl:{}", url),
            Term::PossibleBang(bang) => write!(f, "{}{}", BANG_PREFIXES[0], bang),
        }
    }
}

impl Term {
    pub fn as_simple_text(&self) -> Option<String> {
        match self {
            Term::SimpleOrPhrase(term) => Some(term.as_string()),
            _ => None,
        }
    }
}
