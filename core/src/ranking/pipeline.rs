// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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

use optics::ast::{RankingCoeff, RankingTarget};
use serde::{Deserialize, Serialize};

use crate::{
    inverted_index::WebsitePointer,
    schema::{FastField, Field, TextField, ALL_FIELDS, FLOAT_SCALING},
    searcher::SearchQuery,
    Result,
};

use super::{models::cross_encoder::CrossEncoder, SignalAggregator};

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
    pub host_centrality: f64,
    pub page_centrality: f64,
    pub topic_centrality: f64,
    pub personal_centrality: f64,
    pub query_centrality: f64,
    pub inbound_similarity: f64,
    pub title: String,
    pub clean_body: String,
    pub score: f64,
}

impl RankingWebsite {
    pub fn new(
        doc: tantivy::Document,
        pointer: WebsitePointer,
        aggregator: &SignalAggregator,
    ) -> Self {
        let mut res = RankingWebsite {
            host_centrality: 0.0,
            page_centrality: 0.0,
            topic_centrality: 0.0,
            personal_centrality: 0.0,
            query_centrality: 0.0,
            inbound_similarity: 0.0,
            title: String::new(),
            clean_body: String::new(),
            score: pointer.score.total,
            pointer,
        };

        for value in doc.field_values() {
            match ALL_FIELDS[value.field().field_id() as usize] {
                Field::Fast(FastField::HostCentrality) => {
                    res.host_centrality =
                        value.value.as_u64().unwrap() as f64 / FLOAT_SCALING as f64
                }
                Field::Fast(FastField::PageCentrality) => {
                    res.page_centrality =
                        value.value.as_u64().unwrap() as f64 / FLOAT_SCALING as f64
                }
                Field::Fast(FastField::HostNodeID) => {
                    let node = value.value.as_u64().unwrap().into();

                    res.personal_centrality = aggregator.personal_centrality(node);
                    res.inbound_similarity = aggregator.inbound_similarity(node);
                    res.topic_centrality = aggregator.topic_centrality(node).unwrap_or_default();
                    res.query_centrality = aggregator.query_centrality(node).unwrap_or_default();
                }
                Field::Text(TextField::Title) => {
                    res.title = value.value.as_text().unwrap().to_string()
                }
                Field::Text(TextField::StemmedCleanBody) => {
                    res.clean_body = value.value.as_text().unwrap().to_string()
                }
                _ => {}
            }
        }

        res
    }
}

trait Scorer<T: AsRankingWebsite>: Send + Sync {
    fn score(&self, websites: &mut [T]);
    fn set_coefficients(&mut self, coefficients: &[RankingCoeff]);
    fn set_query_info(&mut self, _query: &SearchQuery) {}
}

struct ReRanker<M: CrossEncoder> {
    coefficient: f64,
    model: Arc<M>,
    prev_score: f64,
    query: String,
}

impl<M: CrossEncoder> ReRanker<M> {
    fn new(model: Arc<M>) -> Self {
        Self {
            coefficient: 10000.0,
            model,
            prev_score: 1.0,
            query: String::new(),
        }
    }
}

impl<T: AsRankingWebsite, M: CrossEncoder> Scorer<T> for ReRanker<M> {
    fn score(&self, websites: &mut [T]) {
        for website in websites {
            let mut website = website.as_mut_ranking();
            let text = website.title.clone() + ". " + &website.clean_body;
            website.score = self.prev_score * website.score
                + self.coefficient * self.model.run(&self.query, &text);
        }
    }

    fn set_query_info(&mut self, query: &SearchQuery) {
        self.query = query.query.clone();
    }

    fn set_coefficients(&mut self, coefficients: &[RankingCoeff]) {
        for coeff in coefficients {
            if let RankingTarget::Signal(signal) = &coeff.target {
                match signal.as_str() {
                    "prev_score" => self.prev_score = coeff.score,
                    "crossencoder" => self.coefficient = coeff.score,
                    _ => {}
                }
            }
        }
    }
}

struct PrioritizeText {
    bm25: f64,
    prev_score: f64,
}

impl Default for PrioritizeText {
    fn default() -> Self {
        Self {
            bm25: 10.0,
            prev_score: 1.0,
        }
    }
}

impl<T: AsRankingWebsite> Scorer<T> for PrioritizeText {
    fn score(&self, websites: &mut [T]) {
        for website in websites {
            let bm25 = website.as_ranking().pointer.score.bm25 as f64;
            let prev_score = website.as_ranking().score;
            website.as_mut_ranking().score = self.bm25 * bm25 + self.prev_score * prev_score;
        }
    }

    fn set_coefficients(&mut self, coefficients: &[RankingCoeff]) {
        for coeff in coefficients {
            if let RankingTarget::Signal(signal) = &coeff.target {
                match signal.as_str() {
                    "prev_score" => self.prev_score = coeff.score,
                    "bm25" => self.bm25 = coeff.score,
                    _ => {}
                }
            }
        }
    }
}

enum Prev<T: AsRankingWebsite> {
    Initial,
    Node(Box<RankingStage<T>>),
}

struct RankingStage<T: AsRankingWebsite> {
    scorer: Box<dyn Scorer<T>>,
    prev: Prev<T>,
    top_n: usize,
    memory: Option<(Vec<T>, Vec<T>)>,
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
                let a: Vec<_> = websites.clone().into_iter().take(self.top_n).collect();
                let b: Vec<_> = websites
                    .into_iter()
                    .skip(self.top_n)
                    .take(self.top_n)
                    .collect();

                self.memory = Some((a, b));
            }
            Prev::Node(n) => n.populate(websites),
        }
    }

    fn apply(&self, top_n: usize, offset: usize) -> Vec<T> {
        let (mut a, mut b) = match &self.prev {
            Prev::Initial => self.memory.clone().unwrap(),
            Prev::Node(n) => {
                let k = offset / self.top_n;
                (
                    n.apply(self.top_n, k * self.top_n),
                    n.apply(self.top_n, (k + 1) * self.top_n),
                )
            }
        };

        self.scorer.score(&mut a);
        a.sort_by(|a, b| {
            b.as_ranking()
                .score
                .partial_cmp(&a.as_ranking().score)
                .unwrap_or(Ordering::Equal)
        });

        self.scorer.score(&mut b);
        b.sort_by(|a, b| {
            b.as_ranking()
                .score
                .partial_cmp(&a.as_ranking().score)
                .unwrap_or(Ordering::Equal)
        });

        a.into_iter()
            .chain(b.into_iter())
            .skip(offset % self.top_n)
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
    offset: usize,
    top_n: usize,
}

impl<T: AsRankingWebsite> RankingPipeline<T> {
    fn create<M: CrossEncoder + 'static>(crossencoder: Arc<M>) -> Result<Self> {
        let last_stage = RankingStage {
            scorer: Box::new(ReRanker::new(crossencoder)),
            prev: Prev::Node(Box::new(RankingStage {
                scorer: Box::<PrioritizeText>::default(),
                prev: Prev::Initial,
                memory: None,
                top_n: 40,
            })),
            memory: None,
            top_n: 20,
        };

        Ok(Self {
            last_stage,
            offset: 0,
            top_n: 0,
        })
    }

    pub fn for_query<M: CrossEncoder + 'static>(
        query: &mut SearchQuery,
        crossencoder: Arc<M>,
    ) -> Result<Self> {
        let mut pipeline = Self::create(crossencoder)?;
        if let Some(optic) = &query.optic_program {
            let optic = optics::parse(optic)?;
            if let Some(optic_pipeline) = optic.pipeline {
                let mut stage = Some(&mut pipeline.last_stage);

                // skip(1) since first stage is collector
                for optic_stage in optic_pipeline.stages.into_iter().skip(1).rev() {
                    match stage {
                        Some(inner_stage) => {
                            inner_stage
                                .scorer
                                .set_coefficients(&optic_stage.coefficients);

                            match &mut inner_stage.prev {
                                Prev::Initial => stage = None,
                                Prev::Node(next) => stage = Some(next),
                            }
                        }
                        None => break,
                    }
                }
            }
        }

        pipeline.last_stage.set_query_info(query);

        pipeline.offset = query.offset;
        pipeline.top_n = query.num_results;

        query.num_results = pipeline.collector_top_n();
        query.offset = pipeline.collector_offset();

        Ok(pipeline)
    }

    pub fn apply(mut self, websites: Vec<T>) -> Vec<T> {
        self.last_stage.populate(websites);

        self.last_stage.apply(self.top_n, self.pipeline_offset())
    }

    pub fn collector_top_n(&self) -> usize {
        2 * self.initial_top_n()
    }

    pub fn initial_top_n(&self) -> usize {
        self.last_stage.initial_top_n()
    }

    fn pipeline_offset(&self) -> usize {
        self.offset - self.collector_offset()
    }

    pub fn collector_offset(&self) -> usize {
        (self.offset / self.initial_top_n()) * self.initial_top_n()
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
                        score: Score {
                            bm25: 0.0,
                            total: 0.0,
                        },
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
                    host_centrality: 0.0,
                    page_centrality: 0.0,
                    topic_centrality: 0.0,
                    personal_centrality: 0.0,
                    query_centrality: 0.0,
                    inbound_similarity: 0.0,
                    title: String::new(),
                    clean_body: String::new(),
                    score: 1.0 / i as f64,
                }
            })
            .collect()
    }

    #[test]
    fn simple() {
        let pipeline = RankingPipeline::for_query(
            &mut SearchQuery {
                ..Default::default()
            },
            Arc::new(DummyCrossEncoder {}),
        )
        .unwrap();
        assert_eq!(pipeline.collector_top_n(), 80);

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
        let pipeline = RankingPipeline::for_query(
            &mut SearchQuery {
                offset: 0,
                ..Default::default()
            },
            Arc::new(DummyCrossEncoder {}),
        )
        .unwrap();

        let sample: Vec<_> =
            sample_websites(pipeline.collector_top_n() + pipeline.collector_offset())
                .into_iter()
                .skip(pipeline.collector_offset())
                .collect();
        let mut prev: Vec<_> = pipeline.apply(sample);

        for offset in 1..1_000 {
            let pipeline = RankingPipeline::for_query(
                &mut SearchQuery {
                    offset,
                    ..Default::default()
                },
                Arc::new(DummyCrossEncoder {}),
            )
            .unwrap();

            let sample: Vec<_> =
                sample_websites(pipeline.collector_top_n() + pipeline.collector_offset())
                    .into_iter()
                    .skip(pipeline.collector_offset())
                    .collect();
            let res: Vec<_> = pipeline.apply(sample);

            assert_eq!(res.len(), 20, "Every page should have 20 results");

            if let Some(first) = prev.first() {
                assert!(!res
                    .iter()
                    .any(|r| r.pointer.address.doc_id == first.pointer.address.doc_id));
            }

            prev = res;
        }

        let pipeline = RankingPipeline::for_query(
            &mut SearchQuery {
                offset: 0,
                ..Default::default()
            },
            Arc::new(DummyCrossEncoder {}),
        )
        .unwrap();

        let sample: Vec<_> =
            sample_websites(pipeline.collector_top_n() + pipeline.collector_offset())
                .into_iter()
                .skip(pipeline.collector_offset())
                .collect();
        let mut prev: Vec<_> = pipeline.apply(sample);
        for p in 1..100 {
            let pipeline = RankingPipeline::for_query(
                &mut SearchQuery {
                    offset: p * 20,
                    ..Default::default()
                },
                Arc::new(DummyCrossEncoder {}),
            )
            .unwrap();

            let sample: Vec<_> =
                sample_websites(pipeline.collector_top_n() + pipeline.collector_offset())
                    .into_iter()
                    .skip(pipeline.collector_offset())
                    .collect();
            let res: Vec<_> = pipeline.apply(sample).into_iter().collect();

            assert_eq!(res.len(), 20, "Every page should have 20 results");

            assert!(prev
                .iter()
                .zip_eq(res.iter())
                .all(|(p, r)| p.pointer.address.doc_id + 20 == r.pointer.address.doc_id));

            prev = res;
        }
    }

    #[test]
    fn multistage_coefficients() {
        let pipeline = RankingPipeline::for_query(
            &mut SearchQuery {
                optic_program: Some(
                    r#"
            RankingPipeline {
                Stage {},
                Stage {
                    Ranking{Signal("bm25"), 3},
                    Ranking{Signal("prev_score"), 2},
                },
                Stage {
                    Ranking{Signal("crossencoder"), 4},
                    Ranking{Signal("prev_score"), 3},
                },
            }
            "#
                    .to_string(),
                ),
                ..Default::default()
            },
            Arc::new(DummyCrossEncoder {}),
        )
        .unwrap();

        let w = RankingWebsite {
            pointer: WebsitePointer {
                score: Score {
                    bm25: 1.0,
                    total: 1.0,
                },
                hashes: Hashes {
                    site: Prehashed(0),
                    title: Prehashed(0),
                    url: Prehashed(0),
                    simhash: 0,
                },
                address: DocAddress {
                    segment: 0,
                    doc_id: 0,
                },
            },
            host_centrality: 0.0,
            page_centrality: 0.0,
            topic_centrality: 0.0,
            personal_centrality: 0.0,
            query_centrality: 0.0,
            inbound_similarity: 0.0,
            title: String::new(),
            clean_body: String::new(),
            score: 1.0,
        };

        let res = pipeline.apply(vec![w.clone()]);
        assert_eq!(res[0].score, 19.0);

        let pipeline = RankingPipeline::for_query(
            &mut SearchQuery {
                optic_program: Some(
                    r#"
            RankingPipeline {
                Stage {},
                Stage {
                    Ranking{Signal("bm25"), 3},
                    Ranking{Signal("prev_score"), 2},
                },
                Stage {
                    Ranking{Signal("crossencoder"), 4},
                    Ranking{Signal("prev_score"), 3},
                },
            }
            "#
                    .to_string(),
                ),
                ..Default::default()
            },
            Arc::new(DummyCrossEncoder {}),
        )
        .unwrap();

        let mut test = [w.clone()];

        pipeline.last_stage.scorer.score(&mut test);
        assert_eq!(test[0].score, 7.0);

        let mut test = [w];

        if let Prev::Node(prev) = pipeline.last_stage.prev {
            prev.scorer.score(&mut test);
            assert_eq!(test[0].score, 5.0);
        }
    }
}
