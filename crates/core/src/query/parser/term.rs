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
// along with this program.  If not, see <https://www.gnu.org/licenses/>

const MAX_PHRASE_LENGTH: usize = 32;
const MAX_TERM_LENGTH_CHARS: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TermCompound {
    pub terms: Vec<SimpleTerm>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CompoundAwareTerm {
    pub term: SimpleTerm,
    pub adjacent_terms: Vec<TermCompound>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SimpleOrPhrase {
    Simple(SimpleTerm),
    Phrase(Vec<String>),
}

impl SimpleOrPhrase {
    fn truncate(self) -> SimpleOrPhrase {
        match self {
            SimpleOrPhrase::Simple(simple) => SimpleOrPhrase::Simple(SimpleTerm(
                simple.0.chars().take(MAX_TERM_LENGTH_CHARS).collect(),
            )),
            SimpleOrPhrase::Phrase(phrase) => SimpleOrPhrase::Phrase(
                phrase
                    .into_iter()
                    .take(MAX_PHRASE_LENGTH)
                    .map(|s| s.chars().take(MAX_TERM_LENGTH_CHARS).collect())
                    .collect(),
            ),
        }
    }
}

impl SimpleOrPhrase {
    pub fn as_string(&self) -> String {
        match self {
            SimpleOrPhrase::Simple(simple) => simple.as_str().to_string(),
            SimpleOrPhrase::Phrase(phrase) => phrase.join(" "),
        }
    }
}

impl From<SimpleTerm> for SimpleOrPhrase {
    fn from(value: SimpleTerm) -> Self {
        SimpleOrPhrase::Simple(value)
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
    LinksTo(String),
    Title(SimpleOrPhrase),
    Body(SimpleOrPhrase),
    Url(SimpleOrPhrase),
    PossibleBang { prefix: char, bang: String },
    Not(Box<Term>),
}

impl std::fmt::Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Term::SimpleOrPhrase(term) => write!(f, "{}", term),
            Term::Not(term) => write!(f, "-{}", term),
            Term::Site(site) => write!(f, "site:{}", site),
            Term::LinksTo(site) => write!(f, "linksto:{}", site),
            Term::Title(title) => write!(f, "intitle:{}", title),
            Term::Body(body) => write!(f, "inbody:{}", body),
            Term::Url(url) => write!(f, "inurl:{}", url),
            Term::PossibleBang { prefix, bang } => write!(f, "{}{}", prefix, bang),
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

    pub fn truncate(self) -> Term {
        match self {
            Term::SimpleOrPhrase(s) => Term::SimpleOrPhrase(s.truncate()),
            Term::Site(s) => Term::Site(s),
            Term::LinksTo(s) => Term::LinksTo(s),
            Term::Title(s) => Term::Title(s.truncate()),
            Term::Body(s) => Term::Body(s.truncate()),
            Term::Url(s) => Term::Url(s.truncate()),
            Term::Not(n) => Term::Not(Box::new(n.truncate())),
            Term::PossibleBang { prefix, bang } => Term::PossibleBang {
                prefix,
                bang: bang.chars().take(MAX_TERM_LENGTH_CHARS).collect(),
            },
        }
    }
}
