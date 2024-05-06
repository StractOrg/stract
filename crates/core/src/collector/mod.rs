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

use crate::{prehashed::Prehashed, ranking::initial::InitialScoreTweaker, simhash};

pub mod approx_count;
mod top_docs;

pub use top_docs::{BucketCollector, TopDocs};
pub type MainCollector = top_docs::TweakedScoreTopCollector<InitialScoreTweaker>;

#[derive(Clone, Debug)]
pub struct MaxDocsConsidered {
    pub total_docs: usize,
    pub segments: usize,
}

#[derive(
    Clone,
    Copy,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    PartialEq,
)]
pub struct Hashes {
    pub site: Prehashed,
    pub title: Prehashed,
    pub url: Prehashed,
    pub url_without_tld: Prehashed,
    pub simhash: simhash::HashType,
}

pub trait Doc: Clone {
    fn score(&self) -> f64;
    fn hashes(&self) -> Hashes;
}
