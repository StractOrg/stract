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

use std::sync::Arc;

use crate::{
    collector::{self, BucketCollector},
    config::CollectorConfig,
    models::dual_encoder::DualEncoder,
    searcher::SearchQuery,
};

use super::{
    models::lambdamart::{self, LambdaMART},
    SignalScore,
};

mod scorers;
mod stages;

pub use scorers::{ReRanker, Recall, Scorer};
pub use stages::{PrecisionRankingWebpage, RecallRankingWebpage};

pub trait RankableWebpage: collector::Doc + Send + Sync {
    fn set_score(&mut self, score: f64);
    fn boost(&self) -> Option<f64>;

    fn boost_score(&mut self) {
        if let Some(boost) = self.boost() {
            if boost != 0.0 {
                let score = self.score() * boost;
                self.set_score(score);
            }
        }
    }
}

impl lambdamart::AsValue for SignalScore {
    fn as_value(&self) -> f64 {
        self.value
    }
}

struct RankingStage<T> {
    scorer: Box<dyn Scorer<T>>,
    stage_top_n: usize,
    derank_similar: bool,
}

impl<T: RankableWebpage> RankingStage<T> {
    fn apply(
        &self,
        websites: Vec<T>,
        top_n: usize,
        offset: usize,
        collector_config: CollectorConfig,
    ) -> Vec<T> {
        let mut websites = websites
            .into_iter()
            .skip(offset)
            .take(self.stage_top_n.max(top_n))
            .collect::<Vec<_>>();

        self.scorer.score(&mut websites);

        let mut collector =
            BucketCollector::new(self.stage_top_n.max(top_n) + offset, collector_config);

        for mut website in websites {
            website.boost_score();
            collector.insert(website);
        }

        collector
            .into_sorted_vec(self.derank_similar)
            .into_iter()
            .take(top_n)
            .collect()
    }

    fn set_query_info(&mut self, query: &SearchQuery) {
        self.scorer.set_query_info(query);
    }
}

pub struct RankingPipeline<T> {
    stage: RankingStage<T>,
    page: usize,
    pub top_n: usize,
    collector_config: CollectorConfig,
}

impl RankingPipeline<crate::searcher::api::ScoredWebpagePointer> {
    fn create_recall_stage(
        lambdamart: Option<Arc<LambdaMART>>,
        dual_encoder: Option<Arc<DualEncoder>>,
        collector_config: CollectorConfig,
        stage_top_n: usize,
    ) -> Self {
        let last_stage = RankingStage {
            scorer: Box::new(Recall::<crate::searcher::api::ScoredWebpagePointer>::new(
                lambdamart,
                dual_encoder,
            )),
            stage_top_n,
            derank_similar: true,
        };

        Self {
            stage: last_stage,
            page: 0,
            top_n: 0,
            collector_config,
        }
    }

    pub fn recall_stage(
        query: &mut SearchQuery,
        lambdamart: Option<Arc<LambdaMART>>,
        dual_encoder: Option<Arc<DualEncoder>>,
        collector_config: CollectorConfig,
        top_n_considered: usize,
    ) -> Self {
        let mut pipeline =
            Self::create_recall_stage(lambdamart, dual_encoder, collector_config, top_n_considered);
        pipeline.set_query_info(query);

        pipeline
    }
}

impl<T: RankableWebpage> RankingPipeline<T> {
    fn set_query_info(&mut self, query: &mut SearchQuery) {
        self.stage.set_query_info(query);
        self.page = query.page;
        self.top_n = query.num_results;

        query.num_results = self.collector_top_n();
        query.page = 0;
    }

    pub fn offset(&self) -> usize {
        self.top_n * self.page
    }

    pub fn apply(self, websites: Vec<T>) -> Vec<T> {
        if websites.len() <= 1 {
            return websites;
        }

        self.stage.apply(
            websites,
            self.top_n,
            self.offset(),
            self.collector_config.clone(),
        )
    }

    pub fn collector_top_n(&self) -> usize {
        (self.initial_top_n().max(self.top_n) + self.top_n * self.page) + 1
    }

    pub fn initial_top_n(&self) -> usize {
        self.stage.stage_top_n.max(self.top_n)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::{
        collector::Hashes,
        enum_map::EnumMap,
        inverted_index::{DocAddress, WebpagePointer},
        prehashed::Prehashed,
        ranking::{self, initial::Score},
    };

    use super::*;

    fn sample_websites(n: usize) -> Vec<RecallRankingWebpage> {
        (0..n)
            .map(|i| -> RecallRankingWebpage {
                let mut signals = EnumMap::new();
                signals.insert(
                    ranking::signal::HostCentrality.into(),
                    SignalScore {
                        coefficient: 1.0,
                        value: 1.0 / i as f64,
                    },
                );
                RecallRankingWebpage {
                    pointer: WebpagePointer {
                        score: Score { total: 0.0 },
                        hashes: Hashes {
                            site: Prehashed(0),
                            title: Prehashed(0),
                            url: Prehashed(0),
                            url_without_tld: Prehashed(0),
                            simhash: 0,
                        },
                        address: DocAddress {
                            segment: 0,
                            doc_id: i as u32,
                        },
                    },
                    signals,
                    optic_boost: None,
                    title_embedding: None,
                    keyword_embedding: None,
                    score: 1.0 / i as f64,
                }
            })
            .collect()
    }

    #[test]
    fn simple() {
        let pipeline = RankingPipeline::<RecallRankingWebpage>::recall_stage(
            &mut SearchQuery {
                ..Default::default()
            },
            None,
            None,
            CollectorConfig::default(),
            20,
        );
        assert_eq!(pipeline.collector_top_n(), 20 + 1);

        let sample = sample_websites(pipeline.collector_top_n());
        let res: Vec<_> = pipeline
            .apply(sample)
            .into_iter()
            .map(|w| w.pointer.address)
            .collect();

        let expected: Vec<_> = sample_websites(100)
            .into_iter()
            .take(20)
            .map(|w| w.pointer.address)
            .collect();

        assert_eq!(res, expected);
    }

    #[test]
    fn top_n() {
        let num_results = 100;
        let pipeline = RankingPipeline::<RecallRankingWebpage>::recall_stage(
            &mut SearchQuery {
                num_results,
                ..Default::default()
            },
            None,
            None,
            CollectorConfig::default(),
            num_results,
        );

        let sample: Vec<_> = sample_websites(pipeline.collector_top_n());

        let expected: Vec<_> = sample
            .clone()
            .into_iter()
            .take(num_results)
            .map(|w| w.pointer.address)
            .collect();

        let res = pipeline
            .apply(sample)
            .into_iter()
            .map(|w| w.pointer.address)
            .collect_vec();

        assert_eq!(res.len(), num_results);
        assert_eq!(res, expected);
    }

    #[test]
    fn offsets() {
        let num_results = 20;
        let pipeline = RankingPipeline::<RecallRankingWebpage>::recall_stage(
            &mut SearchQuery {
                page: 0,
                num_results,
                ..Default::default()
            },
            None,
            None,
            CollectorConfig::default(),
            num_results,
        );

        let sample: Vec<_> = sample_websites(pipeline.collector_top_n());
        let mut prev: Vec<_> = pipeline.apply(sample);
        for p in 1..1_000 {
            let pipeline = RankingPipeline::<RecallRankingWebpage>::recall_stage(
                &mut SearchQuery {
                    page: p,
                    ..Default::default()
                },
                None,
                None,
                CollectorConfig::default(),
                num_results,
            );

            let sample: Vec<_> = sample_websites(pipeline.collector_top_n());
            let res: Vec<_> = pipeline.apply(sample).into_iter().collect();

            assert_eq!(
                res.len(),
                num_results,
                "Every page should have {num_results} results"
            );

            assert!(!prev
                .iter()
                .zip_eq(res.iter())
                .any(|(p, r)| p.pointer.address.doc_id == r.pointer.address.doc_id));

            prev = res;
        }
    }
}
