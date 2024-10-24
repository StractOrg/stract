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

use std::marker::PhantomData;

use tantivy::{
    collector::{SegmentCollector, TopNComputer},
    DocId, SegmentOrdinal,
};

use crate::distributed::member::ShardId;
use crate::webgraph::{
    doc_address::DocAddress,
    query::document_scorer::{DefaultDocumentScorer, DocumentScorer},
    EdgeLimit,
};

use super::Collector;

pub struct TopDocsCollector<S: DocumentScorer = DefaultDocumentScorer> {
    shard_id: Option<ShardId>,
    limit: Option<usize>,
    offset: Option<usize>,
    perform_offset: bool,
    _phantom: PhantomData<S>,
}

impl<S: DocumentScorer> From<EdgeLimit> for TopDocsCollector<S> {
    fn from(limit: EdgeLimit) -> Self {
        let mut collector = TopDocsCollector::new().disable_offset();

        match limit {
            EdgeLimit::Unlimited => {}
            EdgeLimit::Limit(limit) => collector = collector.with_limit(limit),
            EdgeLimit::LimitAndOffset { limit, offset } => {
                collector = collector.with_limit(limit);
                collector = collector.with_offset(offset);
            }
        }

        collector
    }
}

impl<S: DocumentScorer> Default for TopDocsCollector<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: DocumentScorer> TopDocsCollector<S> {
    pub fn new() -> Self {
        Self {
            shard_id: None,
            limit: None,
            offset: None,
            perform_offset: true,
            _phantom: PhantomData,
        }
    }

    pub fn with_shard_id(self, shard_id: ShardId) -> Self {
        Self {
            shard_id: Some(shard_id),
            ..self
        }
    }

    pub fn with_offset(self, offset: usize) -> Self {
        Self {
            offset: Some(offset),
            ..self
        }
    }

    pub fn with_limit(self, limit: usize) -> Self {
        Self {
            limit: Some(limit),
            ..self
        }
    }

    pub fn enable_offset(self) -> Self {
        Self {
            perform_offset: true,
            ..self
        }
    }

    pub fn disable_offset(self) -> Self {
        Self {
            perform_offset: false,
            ..self
        }
    }

    fn computer(&self) -> Computer {
        match (self.offset, self.limit) {
            (Some(offset), Some(limit)) => Computer::TopN(TopNComputer::new(limit + offset)),
            (Some(_), None) => Computer::All(AllComputer::new()),
            (None, Some(limit)) => Computer::TopN(TopNComputer::new(limit)),
            (None, None) => Computer::All(AllComputer::new()),
        }
    }
}

impl<S: DocumentScorer + 'static> Collector for TopDocsCollector<S> {
    type Fruit = Vec<(tantivy::Score, DocAddress)>;

    type Child = TopDocsSegmentCollector<S>;

    fn for_segment(
        &self,
        segment_ord: SegmentOrdinal,
        segment: &tantivy::SegmentReader,
    ) -> crate::Result<Self::Child> {
        let scorer = S::for_segment(segment)?;

        Ok(TopDocsSegmentCollector {
            shard_id: self.shard_id.unwrap(),
            computer: self.computer(),
            segment_ord,
            scorer,
        })
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as tantivy::collector::SegmentCollector>::Fruit>,
    ) -> crate::Result<Self::Fruit> {
        let mut computer = self.computer();

        for fruit in segment_fruits {
            for (score, doc) in fruit {
                computer.push(score, doc);
            }
        }

        let mut result = computer.harvest();

        if self.perform_offset {
            result = result.into_iter().skip(self.offset.unwrap_or(0)).collect();
        }

        Ok(result)
    }
}

enum Computer {
    TopN(TopNComputer<tantivy::Score, DocAddress>),
    All(AllComputer),
}

impl Computer {
    fn push(&mut self, score: tantivy::Score, doc: DocAddress) {
        match self {
            Computer::TopN(computer) => computer.push(score, doc),
            Computer::All(computer) => computer.push(score, doc),
        }
    }

    fn harvest(self) -> Vec<(tantivy::Score, DocAddress)> {
        match self {
            Computer::TopN(computer) => computer
                .into_sorted_vec()
                .into_iter()
                .map(|comparable_doc| (comparable_doc.feature, comparable_doc.doc))
                .collect(),
            Computer::All(computer) => computer.harvest(),
        }
    }
}

struct AllComputer {
    docs: Vec<(tantivy::Score, DocAddress)>,
}

impl AllComputer {
    fn new() -> Self {
        Self { docs: Vec::new() }
    }

    fn push(&mut self, score: tantivy::Score, doc: DocAddress) {
        self.docs.push((score, doc));
    }

    fn harvest(self) -> Vec<(tantivy::Score, DocAddress)> {
        let mut docs = self.docs;
        docs.sort_by(|(score1, _), (score2, _)| score2.total_cmp(score1));
        docs
    }
}

pub struct TopDocsSegmentCollector<S: DocumentScorer> {
    shard_id: ShardId,
    computer: Computer,
    segment_ord: SegmentOrdinal,
    scorer: S,
}

impl<S: DocumentScorer + 'static> SegmentCollector for TopDocsSegmentCollector<S> {
    type Fruit = Vec<(tantivy::Score, DocAddress)>;

    fn collect(&mut self, doc: DocId, _: tantivy::Score) {
        let score = self.scorer.score(doc);
        self.computer
            .push(score, DocAddress::new(self.shard_id, self.segment_ord, doc));
    }

    fn harvest(self) -> Self::Fruit {
        self.computer.harvest()
    }
}
