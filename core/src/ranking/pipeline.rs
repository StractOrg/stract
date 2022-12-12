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

#[derive(Clone, Debug)]
struct RankingWebsite {
    address: tantivy::DocAddress,
    bm25: f64,
    host_centrality: f64,
    page_centrality: f64,
    topic_centrality: f64,
    personal_centrality: f64,
    query_centrality: f64,
    title: String,
    clean_body: String,
    score: f64,
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
    use super::*;

    fn sample_websites(n: usize) -> Vec<RankingWebsite> {
        (0..n)
            .map(|i| -> RankingWebsite {
                RankingWebsite {
                    address: tantivy::DocAddress {
                        segment_ord: 0,
                        doc_id: i as u32,
                    },
                    bm25: 0.0,
                    host_centrality: 0.0,
                    page_centrality: 0.0,
                    topic_centrality: 0.0,
                    personal_centrality: 0.0,
                    query_centrality: 0.0,
                    title: String::new(),
                    clean_body: String::new(),
                    score: i as f64,
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
            .map(|w| w.address)
            .collect();

        let expected: Vec<_> = sample_websites(100)
            .into_iter()
            .rev()
            .take(20)
            .map(|w| w.address)
            .collect();

        assert_eq!(res, expected);
    }
}
