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

mod inbound_similarity;

use super::{RankableWebpage, Top};
pub use inbound_similarity::InboundSimilarity;

pub trait FullModifier: Send + Sync {
    type Webpage: RankableWebpage;
    fn update_boosts(&self, webpages: &mut [Self::Webpage]);

    fn rank(&self, webpages: &mut [Self::Webpage]) {
        webpages.sort_by(|a, b| b.score().partial_cmp(&a.score()).unwrap());
    }

    fn top_n(&self) -> Top {
        Top::Unlimited
    }
}

pub trait Modifier: Send + Sync {
    type Webpage: RankableWebpage;
    fn boost(&self, webpage: &Self::Webpage) -> f64;

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

    fn top_n(&self) -> Top {
        Modifier::top(self)
    }
}
