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

use std::{cmp::Ordering, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{enum_map::EnumMap, inverted_index::WebsitePointer, searcher::SearchQuery, Result};

use super::{models::cross_encoder::CrossEncoder, Signal, SignalAggregator};

pub trait AsRankingWebsite: Clone {
    fn as_ranking(&self) -> &RankingWebsite;
    fn as_mut_ranking(&mut self) -> &mut RankingWebsite;
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
    pub score: f64,
}

impl RankingWebsite {
    pub fn new(pointer: WebsitePointer, aggregator: &mut SignalAggregator) -> Self {
        let mut res = RankingWebsite {
            signals: EnumMap::new(),
            title: None,
            clean_body: None,
            score: pointer.score.total,
            pointer: pointer.clone(),
        };

        for computed_signal in aggregator.compute_signals(pointer.address.doc_id).flatten() {
            res.signals
                .insert(computed_signal.signal, computed_signal.value);
        }

        res
    }
}

trait Scorer<T: AsRankingWebsite>: Send + Sync {
    fn score(&self, websites: &mut [T]);
    fn set_query_info(&mut self, _query: &SearchQuery) {}
}

struct ReRanker<M: CrossEncoder> {
    model: Arc<M>,
    query: String,
}

impl<M: CrossEncoder> ReRanker<M> {
    fn new(model: Arc<M>) -> Self {
        Self {
            model,
            query: String::new(),
        }
    }
}

impl<T: AsRankingWebsite, M: CrossEncoder> Scorer<T> for ReRanker<M> {
    fn score(&self, websites: &mut [T]) {
        let mut bodies = Vec::with_capacity(websites.len());

        for website in websites.iter_mut() {
            let website = website.as_mut_ranking();
            let title = website.title.clone().unwrap_or_default();
            let body = website.clean_body.clone().unwrap_or_default();
            let text = title + ". " + &body;
            bodies.push(text);
        }

        let scores = self.model.run(&self.query, &bodies);

        for (website, score) in websites.iter_mut().zip(scores.into_iter()) {
            let website = website.as_mut_ranking();
            website.signals.insert(Signal::CrossEncoder, score);
            website.score += score;
        }
    }

    fn set_query_info(&mut self, query: &SearchQuery) {
        self.query = query.query.clone();
    }
}

#[derive(Default)]
struct PrioritizeText {}

impl<T: AsRankingWebsite> Scorer<T> for PrioritizeText {
    fn score(&self, websites: &mut [T]) {
        for website in websites {
            let bm25 = website
                .as_ranking()
                .signals
                .get(Signal::Bm25)
                .copied()
                .unwrap_or(0.0);
            website.as_mut_ranking().score += bm25;
        }
    }
}

enum Prev<T: AsRankingWebsite> {
    Initial,
    #[allow(dead_code)]
    Node(Box<RankingStage<T>>),
}

struct RankingStage<T: AsRankingWebsite> {
    scorer: Box<dyn Scorer<T>>,
    prev: Prev<T>,
    top_n: usize,
    memory: Option<Vec<T>>,
}

impl<T: AsRankingWebsite> RankingStage<T> {
    fn initial_top_n(&self) -> usize {
        match &self.prev {
            Prev::Initial => self.top_n,
            Prev::Node(n) => n.initial_top_n(),
        }
    }

    pub fn populate(&mut self, websites: Vec<T>) {
        match &mut self.prev {
            Prev::Initial => {
                self.memory = Some(websites);
            }
            Prev::Node(n) => n.populate(websites),
        }
    }

    fn apply(&self, top_n: usize, page: usize) -> Vec<T> {
        let next_page = (page * top_n) / self.top_n;
        let mut websites = match &self.prev {
            Prev::Initial => self.memory.clone().unwrap(),
            Prev::Node(n) => n.apply(self.top_n, next_page),
        };

        let page = page - next_page;

        self.scorer.score(&mut websites);
        websites.sort_by(|a, b| {
            b.as_ranking()
                .score
                .partial_cmp(&a.as_ranking().score)
                .unwrap_or(Ordering::Equal)
        });

        websites
            .into_iter()
            .skip(page * top_n)
            .take(top_n)
            .collect()
    }

    fn set_query_info(&mut self, query: &SearchQuery) {
        self.scorer.set_query_info(query);
        match &mut self.prev {
            Prev::Initial => {}
            Prev::Node(prev) => prev.set_query_info(query),
        }
    }
}

pub struct RankingPipeline<T: AsRankingWebsite> {
    last_stage: RankingStage<T>,
    page: usize,
    pub top_n: usize,
}

impl<T: AsRankingWebsite> RankingPipeline<T> {
    fn create_reranking<M: CrossEncoder + 'static>(crossencoder: Arc<M>) -> Result<Self> {
        let last_stage = RankingStage {
            scorer: Box::new(ReRanker::new(crossencoder)),
            prev: Prev::Initial,
            memory: None,
            top_n: 20,
        };

        Ok(Self {
            last_stage,
            page: 0,
            top_n: 0,
        })
    }

    pub fn reranking_for_query<M: CrossEncoder + 'static>(
        query: &mut SearchQuery,
        crossencoder: Arc<M>,
    ) -> Result<Self> {
        let mut pipeline = Self::create_reranking(crossencoder)?;
        pipeline.set_query_info(query);

        Ok(pipeline)
    }

    fn create_ltr() -> Self {
        let last_stage = RankingStage {
            scorer: Box::<PrioritizeText>::default(),
            prev: Prev::Initial,
            memory: None,
            top_n: 10_000,
        };

        Self {
            last_stage,
            page: 0,
            top_n: 0,
        }
    }

    pub fn ltr_for_query(query: &mut SearchQuery) -> Self {
        let mut pipeline = Self::create_ltr();
        pipeline.set_query_info(query);

        pipeline
    }

    fn set_query_info(&mut self, query: &mut SearchQuery) {
        self.last_stage.set_query_info(query);
        self.page = query.page;
        self.top_n = query.num_results;

        query.num_results = self.collector_top_n();
        query.page = self.collector_page();
    }

    pub fn apply(mut self, websites: Vec<T>) -> Vec<T> {
        self.last_stage.populate(websites);

        self.last_stage.apply(self.top_n, self.pipeline_page())
    }

    pub fn collector_top_n(&self) -> usize {
        self.initial_top_n()
    }

    pub fn initial_top_n(&self) -> usize {
        self.last_stage.initial_top_n()
    }

    fn pipeline_page(&self) -> usize {
        self.page - (self.collector_page() * self.collector_top_n() / self.top_n)
    }

    pub fn collector_page(&self) -> usize {
        (self.page * self.top_n) / self.initial_top_n()
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
                    signals: EnumMap::new(),
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
    fn offsets() {
        let num_results = 20;
        let pipeline = RankingPipeline::reranking_for_query(
            &mut SearchQuery {
                page: 0,
                num_results,
                ..Default::default()
            },
            Arc::new(DummyCrossEncoder {}),
        )
        .unwrap();

        let sample: Vec<_> =
            sample_websites(pipeline.collector_top_n() + pipeline.collector_page() * num_results)
                .into_iter()
                .skip(pipeline.collector_page() * num_results)
                .collect();
        let mut prev: Vec<_> = pipeline.apply(sample);
        for p in 1..1_000 {
            let pipeline = RankingPipeline::reranking_for_query(
                &mut SearchQuery {
                    page: p,
                    ..Default::default()
                },
                Arc::new(DummyCrossEncoder {}),
            )
            .unwrap();

            let sample: Vec<_> = sample_websites(
                pipeline.collector_top_n() + pipeline.collector_page() * num_results,
            )
            .into_iter()
            .skip(pipeline.collector_page() * num_results)
            .collect();
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
