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
    collector::{self},
    enum_map::EnumMap,
    searcher::SearchQuery,
};

use super::{
    models::lambdamart::{self},
    SignalCalculation, SignalEnum, SignalScore,
};

mod modifiers;
mod scorers;
mod stages;

use modifiers::FullModifier;
pub use scorers::{FullRankingStage, ReRanker};
pub use stages::{LocalRecallRankingWebpage, PrecisionRankingWebpage, RecallRankingWebpage};

pub trait RankableWebpage: collector::Doc + Send + Sync {
    fn set_raw_score(&mut self, score: f64);
    fn unboosted_score(&self) -> f64;
    fn boost(&self) -> f64;
    fn set_boost(&mut self, boost: f64);
    fn signals(&self) -> &EnumMap<SignalEnum, SignalCalculation>;
    fn signals_mut(&mut self) -> &mut EnumMap<SignalEnum, SignalCalculation>;

    fn as_local_recall(&self) -> &LocalRecallRankingWebpage;

    fn score(&self) -> f64 {
        self.boost() * self.unboosted_score()
    }
}

impl lambdamart::AsValue for SignalScore {
    fn as_value(&self) -> f64 {
        self.value
    }
}

enum StageOrModifier<T> {
    Stage(Box<dyn FullRankingStage<Webpage = T>>),
    Modifier(Box<dyn FullModifier<Webpage = T>>),
}

pub enum Top {
    Unlimited,
    Limit(usize),
}

impl<T> StageOrModifier<T>
where
    T: RankableWebpage + Send + Sync,
{
    fn top_n(&self) -> Top {
        match self {
            StageOrModifier::Stage(stage) => stage.top_n(),
            StageOrModifier::Modifier(modifier) => modifier.top_n(),
        }
    }

    fn compute(&self, webpages: &mut [T]) {
        match self {
            StageOrModifier::Stage(stage) => stage.compute(webpages),
            StageOrModifier::Modifier(modifier) => modifier.update_boosts(webpages),
        }
    }

    fn update_scores(&self, webpages: &mut [T], coefficients: &super::SignalCoefficients) {
        match self {
            StageOrModifier::Stage(stage) => stage.update_scores(webpages, coefficients),
            StageOrModifier::Modifier(_) => {}
        }
    }

    fn rank(&self, webpages: &mut [T]) {
        match self {
            StageOrModifier::Stage(stage) => stage.rank(webpages),
            StageOrModifier::Modifier(modifier) => modifier.rank(webpages),
        }
    }
}

pub struct RankingPipeline<T> {
    stages_or_modifiers: Vec<StageOrModifier<T>>,
}

impl<T> RankingPipeline<T>
where
    T: RankableWebpage,
{
    fn new() -> Self {
        Self {
            stages_or_modifiers: Vec::new(),
        }
    }

    pub fn add_stage<R>(mut self, stage: R) -> Self
    where
        R: FullRankingStage<Webpage = T> + 'static,
    {
        self.stages_or_modifiers.push(StageOrModifier::Stage(
            Box::new(stage) as Box<dyn FullRankingStage<Webpage = T>>
        ));

        self
    }

    pub fn add_modifier<R>(mut self, modifier: R) -> Self
    where
        R: FullModifier<Webpage = T> + 'static,
    {
        self.stages_or_modifiers.push(StageOrModifier::Modifier(
            Box::new(modifier) as Box<dyn FullModifier<Webpage = T>>
        ));

        self
    }

    pub fn apply(&self, webpages: Vec<T>, query: &SearchQuery) -> Vec<T> {
        let mut webpages = webpages;
        let num_pages = webpages.len();
        let coefficients = query.signal_coefficients();

        for stage_or_modifier in self.stages_or_modifiers.iter() {
            let webpages = if let Top::Limit(top_n) = stage_or_modifier.top_n() {
                if query.offset() > top_n {
                    continue;
                }

                &mut webpages[..top_n.min(num_pages)]
            } else {
                &mut webpages
            };

            stage_or_modifier.compute(webpages);
            stage_or_modifier.update_scores(webpages, &coefficients);
            stage_or_modifier.rank(webpages);
        }

        webpages
            .into_iter()
            .skip(query.offset())
            .take(query.num_results())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use scorers::term_distance;

    use crate::{
        ampc::dht::ShardId,
        collector::Hashes,
        inverted_index::{DocAddress, WebpagePointer},
        prehashed::Prehashed,
        ranking::{self, bitvec_similarity::BitVec, initial::Score},
        searcher::api,
    };

    use super::*;

    fn pipeline() -> RankingPipeline<api::ScoredWebpagePointer> {
        RankingPipeline::new()
            .add_stage(term_distance::TitleDistanceScorer)
            .add_stage(term_distance::BodyDistanceScorer)
    }

    fn sample_websites(n: usize) -> Vec<api::ScoredWebpagePointer> {
        (0..n)
            .map(|i| -> LocalRecallRankingWebpage {
                let pointer = WebpagePointer {
                    score: Score { total: 0.0 },
                    hashes: Hashes {
                        site: Prehashed(0),
                        title: Prehashed(0),
                        url: Prehashed(0),
                        url_without_tld: Prehashed(0),
                        simhash: 0,
                    },
                    address: DocAddress::new(0, i as u32, ShardId::new(0)),
                };

                let mut signals = EnumMap::new();
                let score = 1.0 / i as f64;
                let calc = ranking::SignalCalculation {
                    value: i as f64,
                    score,
                };
                signals.insert(ranking::signals::HostCentrality.into(), calc);
                LocalRecallRankingWebpage::new_testing(pointer, signals, calc.score)
            })
            .map(|local| {
                api::ScoredWebpagePointer::Normal(
                    crate::searcher::distributed::ScoredWebpagePointer {
                        website: RecallRankingWebpage::new(local, BitVec::new(vec![])),
                        shard: ShardId::new(0),
                    },
                )
            })
            .collect()
    }

    #[test]
    fn simple() {
        let pipeline = pipeline();

        let sample = sample_websites(20);
        let res: Vec<_> = pipeline
            .apply(
                sample,
                &SearchQuery {
                    page: 0,
                    num_results: 20,
                    ..Default::default()
                },
            )
            .into_iter()
            .map(|w| w.as_ranking().pointer().address)
            .collect();

        let expected: Vec<_> = sample_websites(100)
            .into_iter()
            .take(20)
            .map(|w| w.as_ranking().pointer().address)
            .collect();

        assert_eq!(res, expected);
    }
}
