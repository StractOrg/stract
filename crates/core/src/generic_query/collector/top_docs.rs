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

use crate::{distributed::member::ShardId, inverted_index::DocAddress};
use tantivy::{
    collector::{SegmentCollector, TopNComputer},
    DocId, SegmentOrdinal,
};

use super::Collector;

pub struct TopDocsCollector {
    shard_id: Option<ShardId>,
    limit: Option<usize>,
    offset: Option<usize>,
    perform_offset: bool,
}

impl Default for TopDocsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl TopDocsCollector {
    pub fn new() -> Self {
        Self {
            shard_id: None,
            limit: None,
            offset: None,
            perform_offset: true,
        }
    }
}

impl TopDocsCollector {
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
}

impl TopDocsCollector {
    fn computer(&self) -> Computer {
        match (self.offset, self.limit) {
            (Some(offset), Some(limit)) => Computer::TopN(TopNComputer::new(limit + offset)),
            (Some(_), None) => Computer::All(AllComputer::new()),
            (None, Some(limit)) => Computer::TopN(TopNComputer::new(limit)),
            (None, None) => Computer::All(AllComputer::new()),
        }
    }
}

impl Collector for TopDocsCollector {
    type Fruit = Vec<(tantivy::Score, DocAddress)>;

    type Child = TopDocsSegmentCollector;

    fn for_segment(
        &self,
        segment_ord: SegmentOrdinal,
        _: &tantivy::SegmentReader,
    ) -> crate::Result<Self::Child> {
        Ok(TopDocsSegmentCollector {
            shard_id: self.shard_id.unwrap(),
            computer: self.computer(),
            segment_ord,
        })
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as tantivy::collector::SegmentCollector>::Fruit>,
    ) -> crate::Result<Self::Fruit> {
        let mut computer = self.computer();

        let fruits: Vec<_> = segment_fruits.into_iter().flatten().collect();

        for (score, doc) in fruits {
            computer.push(score, doc);
        }

        let result = computer.harvest();

        if self.perform_offset {
            Ok(result
                .into_iter()
                .skip(self.offset.unwrap_or(0))
                .take(self.limit.unwrap_or(usize::MAX))
                .collect())
        } else {
            Ok(result
                .into_iter()
                .take(self.limit.unwrap_or(usize::MAX))
                .collect())
        }
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

pub struct TopDocsSegmentCollector {
    shard_id: ShardId,
    computer: Computer,
    segment_ord: SegmentOrdinal,
}

impl SegmentCollector for TopDocsSegmentCollector {
    type Fruit = Vec<(tantivy::Score, DocAddress)>;

    fn collect(&mut self, doc: DocId, score: tantivy::Score) {
        if doc == tantivy::TERMINATED {
            return;
        }

        self.computer
            .push(score, DocAddress::new(self.segment_ord, doc, self.shard_id));
    }

    fn harvest(self) -> Self::Fruit {
        self.computer.harvest()
    }
}
