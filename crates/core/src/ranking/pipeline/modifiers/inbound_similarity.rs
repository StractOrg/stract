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

use crate::{
    ranking::{self, pipeline::RankableWebpage},
    searcher::api,
};

use super::Modifier;

const INBOUND_SIMILARITY_SMOOTHING: f64 = 8.0;

pub struct InboundSimilarity;

impl Modifier for InboundSimilarity {
    type Webpage = api::ScoredWebpagePointer;

    fn boost(&self, webpage: &Self::Webpage) -> f64 {
        webpage
            .as_ranking()
            .signals()
            .get(ranking::InboundSimilarity.into())
            .map(|calc| calc.value)
            .unwrap_or(0.0)
            + INBOUND_SIMILARITY_SMOOTHING
    }
}
