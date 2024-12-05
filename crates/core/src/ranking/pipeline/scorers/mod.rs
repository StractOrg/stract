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

//! Scorers are used to compute the ranking signals in the ranking pipeline.
//!
//! Each scorer computes a single signal which is then used to rank the pages.

pub mod embedding;
pub mod inbound_similarity;
pub mod lambdamart;
pub mod reranker;
pub mod term_distance;

pub use reranker::ReRanker;

use crate::ranking::{SignalCalculation, SignalCoefficients, SignalEnum};

use super::{RankableWebpage, Top};

/// A ranking stage that computes some signals for each page.
///
/// This trait is implemented for all scorers.
/// Most of the time you will want to implement the [`RankingStage`] trait instead,
/// but this trait gives you more control over the ranking pipeline.
pub trait FullRankingStage: Send + Sync {
    type Webpage: RankableWebpage;

    /// Compute the signal for each page.
    fn compute(&self, webpages: &mut [Self::Webpage]);

    /// The number of pages to return from this part of the pipeline.
    fn top(&self) -> Top {
        Top::Unlimited
    }

    /// Update the score for each page.
    fn update_scores(&self, webpages: &mut [Self::Webpage], coefficients: &SignalCoefficients) {
        for webpage in webpages.iter_mut() {
            webpage.set_raw_score(webpage.signals().iter().fold(0.0, |acc, (signal, calc)| {
                acc + calc.score * coefficients.get(&signal)
            }));
        }
    }

    /// Rank the pages by their score.
    fn rank(&self, webpages: &mut [Self::Webpage]) {
        webpages.sort_by(|a, b| b.score().partial_cmp(&a.score()).unwrap());
    }
}

/// A ranking stage that computes a single signal for each page.
pub trait RankingStage: Send + Sync {
    type Webpage: RankableWebpage;

    /// Compute the signal for a single page.
    fn compute(&self, webpage: &Self::Webpage) -> (SignalEnum, SignalCalculation);

    /// The number of pages to return from this part of the pipeline.
    fn top(&self) -> Top {
        Top::Unlimited
    }
}

impl<T> FullRankingStage for T
where
    T: RankingStage,
{
    type Webpage = <T as RankingStage>::Webpage;

    fn compute(&self, webpages: &mut [Self::Webpage]) {
        for webpage in webpages.iter_mut() {
            let (signal, signal_calculation) = self.compute(webpage);
            webpage.signals_mut().insert(signal, signal_calculation);
        }
    }

    fn top(&self) -> Top {
        self.top()
    }
}
