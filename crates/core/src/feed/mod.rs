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

use std::str::FromStr;

use url::Url;

mod parser;

pub use parser::parse;

use crate::dated_url::DatedUrl;

#[derive(
    Debug,
    Clone,
    Copy,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    PartialEq,
    Eq,
    Hash,
)]
pub enum FeedKind {
    Atom,
    Rss,
}

impl FromStr for FeedKind {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "application/atom" => Ok(Self::Atom),
            "application/atom+xml" => Ok(Self::Atom),
            "application/rss" => Ok(Self::Rss),
            "application/rss+xml" => Ok(Self::Rss),
            s => anyhow::bail!("Unknown feed kind: {s}"),
        }
    }
}

#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    PartialEq,
    Eq,
    Hash,
)]
pub struct Feed {
    #[bincode(with_serde)]
    pub url: Url,
    pub kind: FeedKind,
}

pub struct ParsedFeed {
    pub links: Vec<DatedUrl>,
}
