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

//! Modifiers are used to modify the ranking of pages.
//!
//! Each page is ranked by a linear combination of the signals like
//! `score = boost * (signal_1 * weight_1 + signal_2 * weight_2 + ...)`
//!
//! Modifiers can either modify the multiplicative boost factor for
//! each page or override the ranking entirely (if we want to rank
//! for something other than the score).

mod inbound_similarity;

use super::{RankableWebpage, Top};
pub use inbound_similarity::InboundSimilarity;

/// A modifier that gives full control over the ranking.
pub trait FullModifier: Send + Sync {
    type Webpage: RankableWebpage;
    /// Modify the boost factor for each page.
    fn update_boosts(&self, webpages: &mut [Self::Webpage]);

    /// Override ranking of the pages.
    fn rank(&self, webpages: &mut [Self::Webpage]) {
        webpages.sort_by(|a, b| b.score().partial_cmp(&a.score()).unwrap());
    }

    /// The number of pages to return from this part of the pipeline.
    fn top(&self) -> Top {
        Top::Unlimited
    }
}

/// A modifier that modifies the multiplicative boost factor for each page.
///
/// This is the most common type of modifier.
pub trait Modifier: Send + Sync {
    type Webpage: RankableWebpage;
    /// Modify the boost factor for a page.
    ///
    /// The new boost factor will be multiplied with the page's current boost factor.
    fn boost(&self, webpage: &Self::Webpage) -> f64;

    /// The number of pages to return from this part of the pipeline.
    fn top(&self) -> Top {
        Top::Unlimited
    }
}

impl<T> FullModifier for T
where
    T: Modifier,
{
    type Webpage = <T as Modifier>::Webpage;

    fn update_boosts(&self, webpages: &mut [Self::Webpage]) {
        for webpage in webpages {
            let cur_boost = webpage.boost();
            webpage.set_boost(cur_boost * self.boost(webpage));
        }
    }

    fn top(&self) -> Top {
        Modifier::top(self)
    }
}
