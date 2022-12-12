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

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::{
    inverted_index::WebsitePointer,
    schema::{FastField, Field, TextField, ALL_FIELDS, FLOAT_SCALING},
};

use super::SignalAggregator;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RankingWebsite {
    pub pointer: WebsitePointer,
    pub host_centrality: f64,
    pub page_centrality: f64,
    pub topic_centrality: f64,
    pub personal_centrality: f64,
    pub query_centrality: f64,
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
            pointer,
            host_centrality: 0.0,
            page_centrality: 0.0,
            topic_centrality: 0.0,
            personal_centrality: 0.0,
            query_centrality: 0.0,
            title: String::new(),
            clean_body: String::new(),
            score: 0.0,
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
                    let node = value.value.as_u64().unwrap();

                    res.personal_centrality = dbg!(aggregator.personal_centrality(node));
                    res.topic_centrality = aggregator.topic_centrality(node).unwrap_or_default();
                    res.query_centrality = aggregator.query_centrality(node).unwrap_or_default();
                }
                Field::Text(TextField::Title) => {
                    res.title = value.value.as_text().unwrap().to_string()
                }
                Field::Text(TextField::CleanBody) => {
                    res.clean_body = value.value.as_text().unwrap().to_string()
                }
                _ => {}
            }
        }

        res
    }
}

trait Scorer {
    fn score(&self, websites: &mut [RankingWebsite]);
}

struct ReRanker {}

impl Scorer for ReRanker {
    fn score(&self, websites: &mut [RankingWebsite]) {
        // TODO: Implement actual scoring
        // todo!();
    }
}

struct SignalFocusText {}

impl Scorer for SignalFocusText {
    fn score(&self, websites: &mut [RankingWebsite]) {
        // TODO: Implement actual scoring
        // todo!();
    }
}

enum Prev {
    Initial,
    Node(Box<RankingStage>),
}

struct RankingStage {
    scorer: Box<dyn Scorer>,
    prev: Prev,
    top_n: usize,
    memory: Option<Vec<RankingWebsite>>,
}

impl RankingStage {
    fn initial_top_n(&self) -> usize {
        match &self.prev {
            Prev::Initial => self.top_n,
            Prev::Node(n) => n.initial_top_n(),
        }
    }

    pub fn populate(&mut self, websites: Vec<RankingWebsite>) {
        match &mut self.prev {
            Prev::Initial => self.memory = Some(websites),
            Prev::Node(n) => n.populate(websites),
        }
    }

    fn apply(self, top_n: usize) -> Vec<RankingWebsite> {
        let mut chunk = match self.prev {
            Prev::Initial => self.memory.unwrap(),
            Prev::Node(n) => n.apply(self.top_n),
        };

        self.scorer.score(&mut chunk);
        chunk.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal));

        chunk.into_iter().take(top_n).collect()
    }
}

struct Pipeline {
    last_stage: RankingStage,
}

impl Default for Pipeline {
    fn default() -> Self {
        let last_stage = RankingStage {
            scorer: Box::new(ReRanker {}),
            prev: Prev::Node(Box::new(RankingStage {
                scorer: Box::new(SignalFocusText {}),
                prev: Prev::Initial,
                memory: None,
                top_n: 100,
            })),
            memory: None,
            top_n: 50,
        };
        Self { last_stage }
    }
}

impl Pipeline {
    pub fn with_offset(&mut self, offset: usize) {
        todo!();
    }

    pub fn apply(mut self, websites: Vec<RankingWebsite>) -> Vec<RankingWebsite> {
        self.last_stage.populate(websites);

        self.last_stage.apply(20)
    }

    pub fn initial_top_n(&self) -> usize {
        self.last_stage.initial_top_n()
    }
}

#[cfg(test)]
mod tests {
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
                    title: String::new(),
                    clean_body: String::new(),
                    score: 0.0,
                }
            })
            .collect()
    }

    #[test]
    fn simple() {
        let pipeline = Pipeline::default();
        assert_eq!(pipeline.initial_top_n(), 100);

        let sample = sample_websites(pipeline.initial_top_n());
        let res: Vec<_> = pipeline
            .apply(sample)
            .into_iter()
            .map(|w| w.pointer.address)
            .collect();

        let expected: Vec<_> = sample_websites(100)
            .into_iter()
            .rev()
            .take(20)
            .map(|w| w.pointer.address)
            .collect();

        assert_eq!(res, expected);
    }
}
