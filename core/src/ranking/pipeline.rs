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
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{
    collector::{self, BucketCollector},
    config::CollectorConfig,
    enum_map::EnumMap,
    inverted_index::WebsitePointer,
    searcher::SearchQuery,
    Result,
};

use super::{
    models::{cross_encoder::CrossEncoder, lambdamart::LambdaMART},
    Signal, SignalAggregator, SignalCoefficient,
};

pub trait AsRankingWebsite: Clone {
    fn as_ranking(&self) -> &RankingWebsite;
    fn as_mut_ranking(&mut self) -> &mut RankingWebsite;
}

impl<T> collector::Doc for T
where
    T: AsRankingWebsite,
{
    fn score(&self) -> f64 {
        self.as_ranking().score
    }

    fn id(&self) -> &tantivy::DocId {
        &self.as_ranking().pointer.address.doc_id
    }

    fn hashes(&self) -> collector::Hashes {
        self.as_ranking().pointer.hashes
    }
}

impl AsRankingWebsite for RankingWebsite {
    fn as_ranking(&self) -> &RankingWebsite {
        self
    }

    fn as_mut_ranking(&mut self) -> &mut RankingWebsite {
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RankingWebsite {
    pub pointer: WebsitePointer,
    pub signals: EnumMap<Signal, f64>,
    pub title: Option<String>,
    pub clean_body: Option<String>,
    pub optic_boost: Option<f64>,
    pub score: f64,
}

impl RankingWebsite {
    pub fn new(pointer: WebsitePointer, aggregator: &mut SignalAggregator) -> Self {
        let mut res = RankingWebsite {
            signals: EnumMap::new(),
            title: None,
            clean_body: None,
            score: pointer.score.total,
            optic_boost: None,
            pointer: pointer.clone(),
        };

        let mut total = 0.0;
        for computed_signal in aggregator.compute_signals(pointer.address.doc_id).flatten() {
            res.signals
                .insert(computed_signal.signal, computed_signal.value);
            total += computed_signal.value * computed_signal.coefficient;
        }

        res.signals.insert(Signal::LinearRegression, total);

        if let Some(boost) = aggregator.boosts(pointer.address.doc_id) {
            res.optic_boost = Some(boost);
        }

        res
    }
}

trait Scorer<T: AsRankingWebsite>: Send + Sync {
    fn score(&self, websites: &mut [T]);
    fn set_query_info(&mut self, _query: &SearchQuery) {}
}

struct ReRanker<M: CrossEncoder> {
    crossencoder: Arc<M>,
    lambda_mart: Option<Arc<LambdaMART>>,
    query: Option<SearchQuery>,
    signal_coefficients: Option<SignalCoefficient>,
}

impl<M: CrossEncoder> ReRanker<M> {
    fn new(crossencoder: Arc<M>, lambda: Option<Arc<LambdaMART>>) -> Self {
        Self {
            crossencoder,
            lambda_mart: lambda,
            query: None,
            signal_coefficients: None,
        }
    }

    fn crossencoder_score_websites<T: AsRankingWebsite>(&self, websites: &mut [T]) {
        let mut bodies = Vec::with_capacity(websites.len());

        for website in websites.iter_mut() {
            let website = website.as_mut_ranking();
            let title = website.title.clone().unwrap_or_default();
            let body = website.clean_body.clone().unwrap_or_default();
            let text = title + ". " + body.as_str();
            bodies.push(text);
        }

        let query = &self.query.as_ref().unwrap().query;
        let scores = self.crossencoder.run(query, &bodies);

        for (website, score) in websites.iter_mut().zip(scores) {
            let website = website.as_mut_ranking();
            website.signals.insert(Signal::CrossEncoder, score);
        }
    }
}

impl<T: AsRankingWebsite, M: CrossEncoder> Scorer<T> for ReRanker<M> {
    fn score(&self, websites: &mut [T]) {
        self.crossencoder_score_websites(websites);

        for website in websites.iter_mut() {
            let website = website.as_mut_ranking();
            let score = calculate_score(
                &self.lambda_mart,
                &self.signal_coefficients,
                &website.signals,
            );

            if let Some(coeffs) = &self.signal_coefficients {
                if let Some(coeff) = coeffs.get(&Signal::CrossEncoder) {
                    website.score =
                        score + coeff * website.signals.get(Signal::CrossEncoder).unwrap();
                } else {
                    website.score = score;
                }
            } else {
                website.score = score;
            }
        }
    }

    fn set_query_info(&mut self, query: &SearchQuery) {
        self.query = Some(query.clone());

        self.signal_coefficients = query.optic.as_ref().map(SignalCoefficient::from_optic);
    }
}

fn calculate_score(
    model: &Option<Arc<LambdaMART>>,
    signal_coefficients: &Option<SignalCoefficient>,
    signals: &EnumMap<Signal, f64>,
) -> f64 {
    let lambda_score = match model {
        Some(model) => match signal_coefficients {
            Some(coefficients) => match coefficients.get(&Signal::LambdaMART) {
                Some(coeff) => {
                    if coeff == 0.0 {
                        signals
                            .get(Signal::LinearRegression)
                            .copied()
                            .unwrap_or(0.0)
                    } else {
                        coeff * model.predict(signals)
                    }
                }
                None => Signal::LambdaMART.default_coefficient() * model.predict(signals),
            },
            None => Signal::LambdaMART.default_coefficient() * model.predict(signals),
        },
        None => signals
            .get(Signal::LinearRegression)
            .copied()
            .unwrap_or(0.0),
    };

    lambda_score
}

#[derive(Default)]
struct Initial {
    model: Option<Arc<LambdaMART>>,
    signal_coefficients: Option<SignalCoefficient>,
}

impl<T: AsRankingWebsite> Scorer<T> for Initial {
    fn score(&self, websites: &mut [T]) {
        for website in websites {
            let website = website.as_mut_ranking();
            website.score =
                calculate_score(&self.model, &self.signal_coefficients, &website.signals);
        }
    }

    fn set_query_info(&mut self, query: &SearchQuery) {
        self.signal_coefficients = query.optic.as_ref().map(SignalCoefficient::from_optic);
    }
}

struct RankingStage<T: AsRankingWebsite> {
    scorer: Box<dyn Scorer<T>>,
    stage_top_n: usize,
    derank_similar: bool,
}

impl<T: AsRankingWebsite> RankingStage<T> {
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
        for website in websites.iter_mut() {
            let boost = website.as_ranking().optic_boost;
            if let Some(boost) = boost {
                if boost != 0.0 {
                    website.as_mut_ranking().score *= boost;
                }
            }
        }

        let mut collector =
            BucketCollector::new(self.stage_top_n.max(top_n) + offset, collector_config);

        for website in websites {
            collector.insert(website);
        }

        collector.into_sorted_vec(self.derank_similar)
    }

    fn set_query_info(&mut self, query: &SearchQuery) {
        self.scorer.set_query_info(query);
    }
}

pub struct RankingPipeline<T: AsRankingWebsite> {
    stage: RankingStage<T>,
    page: usize,
    pub top_n: usize,
    collector_config: CollectorConfig,
}

impl<T: AsRankingWebsite> RankingPipeline<T> {
    fn create_reranking<M: CrossEncoder + 'static>(
        crossencoder: Arc<M>,
        lambda: Option<Arc<LambdaMART>>,
        collector_config: CollectorConfig,
    ) -> Result<Self> {
        let stage = RankingStage {
            scorer: Box::new(ReRanker::new(crossencoder, lambda)),
            stage_top_n: 20,
            derank_similar: true,
        };

        Ok(Self {
            stage,
            page: 0,
            top_n: 0,
            collector_config,
        })
    }

    pub fn reranking_for_query<M: CrossEncoder + 'static>(
        query: &mut SearchQuery,
        crossencoder: Arc<M>,
        lambda: Option<Arc<LambdaMART>>,
        collector_config: CollectorConfig,
    ) -> Result<Self> {
        let mut pipeline = Self::create_reranking(crossencoder, lambda, collector_config)?;
        pipeline.set_query_info(query);

        Ok(pipeline)
    }

    fn create_ltr(model: Option<Arc<LambdaMART>>, collector_config: CollectorConfig) -> Self {
        let last_stage = RankingStage {
            scorer: Box::new(Initial {
                model,
                signal_coefficients: None,
            }),
            stage_top_n: 10_000,
            derank_similar: true,
        };

        Self {
            stage: last_stage,
            page: 0,
            top_n: 0,
            collector_config,
        }
    }

    pub fn ltr_for_query(
        query: &mut SearchQuery,
        model: Option<Arc<LambdaMART>>,
        collector_config: CollectorConfig,
    ) -> Self {
        let mut pipeline = Self::create_ltr(model, collector_config);
        pipeline.set_query_info(query);

        pipeline
    }

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
        self.stage.apply(
            websites,
            self.top_n,
            self.offset(),
            self.collector_config.clone(),
        )
    }

    pub fn collector_top_n(&self) -> usize {
        self.initial_top_n().max(self.top_n) + self.top_n * self.page
    }

    pub fn initial_top_n(&self) -> usize {
        self.stage.stage_top_n.max(self.top_n)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::ranking::models::cross_encoder::DummyCrossEncoder;
    use crate::{
        collector::Hashes, inverted_index::DocAddress, prehashed::Prehashed,
        ranking::initial::Score,
    };

    use super::*;

    fn sample_websites(n: usize) -> Vec<RankingWebsite> {
        (0..n)
            .map(|i| -> RankingWebsite {
                let mut signals = EnumMap::new();
                signals.insert(Signal::LinearRegression, 1.0 / i as f64);
                RankingWebsite {
                    pointer: WebsitePointer {
                        score: Score { total: 0.0 },
                        hashes: Hashes {
                            site: Prehashed(0),
                            title: Prehashed(0),
                            url: Prehashed(0),
                            simhash: 0,
                        },
                        address: DocAddress {
                            segment: 0,
                            doc_id: i as u32,
                        },
                    },
                    signals,
                    optic_boost: None,
                    title: None,
                    clean_body: None,
                    score: 1.0 / i as f64,
                }
            })
            .collect()
    }

    #[test]
    fn simple() {
        let pipeline = RankingPipeline::reranking_for_query(
            &mut SearchQuery {
                ..Default::default()
            },
            Arc::new(DummyCrossEncoder {}),
            None,
            CollectorConfig::default(),
        )
        .unwrap();
        assert_eq!(pipeline.collector_top_n(), 20);

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
        let pipeline = RankingPipeline::reranking_for_query(
            &mut SearchQuery {
                num_results,
                ..Default::default()
            },
            Arc::new(DummyCrossEncoder {}),
            None,
            CollectorConfig::default(),
        )
        .unwrap();

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
        let pipeline = RankingPipeline::reranking_for_query(
            &mut SearchQuery {
                page: 0,
                num_results,
                ..Default::default()
            },
            Arc::new(DummyCrossEncoder {}),
            None,
            CollectorConfig::default(),
        )
        .unwrap();

        let sample: Vec<_> = sample_websites(pipeline.collector_top_n());
        let mut prev: Vec<_> = pipeline.apply(sample);
        for p in 1..1_000 {
            let pipeline = RankingPipeline::reranking_for_query(
                &mut SearchQuery {
                    page: p,
                    ..Default::default()
                },
                Arc::new(DummyCrossEncoder {}),
                None,
                CollectorConfig::default(),
            )
            .unwrap();

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
