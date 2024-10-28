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

use anyhow::anyhow;
use itertools::Itertools;
use tantivy::{
    collector::{SegmentCollector, TopNComputer},
    columnar::Column,
    DocId, SegmentOrdinal,
};

use crate::webgraph::{
    doc_address::DocAddress,
    query::document_scorer::{DefaultDocumentScorer, DocumentScorer},
    schema::{Field, FieldEnum},
    EdgeLimit,
};
use crate::{distributed::member::ShardId, webgraph::warmed_column_fields::WarmedColumnFields};

use super::Collector;

pub trait DeduplicatorDoc
where
    Self: Send + Sync + serde::Serialize + serde::de::DeserializeOwned + Ord + Clone,
{
    fn new<S, D>(collector: &TopDocsSegmentCollector<S, D>, doc: DocId) -> Self
    where
        S: DocumentScorer + 'static,
        D: Deduplicator + 'static;
}

pub trait Deduplicator: Clone + Send + Sync {
    type Doc: DeduplicatorDoc;

    fn deduplicate(
        &self,
        docs: Vec<(tantivy::Score, Self::Doc)>,
    ) -> Vec<(tantivy::Score, Self::Doc)>;
}

impl DeduplicatorDoc for DocAddress {
    fn new<S, D>(collector: &TopDocsSegmentCollector<S, D>, doc: DocId) -> Self
    where
        S: DocumentScorer + 'static,
        D: Deduplicator + 'static,
    {
        DocAddress::new(collector.shard_id, collector.segment_ord, doc)
    }
}

#[derive(Clone)]
pub struct NoDeduplicator;

impl Deduplicator for NoDeduplicator {
    type Doc = DocAddress;

    fn deduplicate(
        &self,
        docs: Vec<(tantivy::Score, Self::Doc)>,
    ) -> Vec<(tantivy::Score, Self::Doc)> {
        docs
    }
}

#[derive(
    Clone,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    Ord,
    PartialOrd,
    Eq,
    PartialEq,
)]
pub struct DocAddressWithHost {
    pub address: DocAddress,
    pub host: u64,
}

impl DeduplicatorDoc for DocAddressWithHost {
    fn new<S, D>(collector: &TopDocsSegmentCollector<S, D>, doc: DocId) -> Self
    where
        S: DocumentScorer + 'static,
        D: Deduplicator + 'static,
    {
        let host = collector
            .host_column
            .as_ref()
            .and_then(|col| col.first(doc))
            .ok_or_else(|| anyhow!("ColumnFields must be set to use HostDeduplicator"))
            .unwrap();
        Self {
            address: DocAddress::new(collector.shard_id, collector.segment_ord, doc),
            host,
        }
    }
}

#[derive(Clone)]
pub struct HostDeduplicator;

impl Deduplicator for HostDeduplicator {
    type Doc = DocAddressWithHost;

    fn deduplicate(
        &self,
        docs: Vec<(tantivy::Score, Self::Doc)>,
    ) -> Vec<(tantivy::Score, Self::Doc)> {
        docs.into_iter().unique_by(|(_, doc)| doc.host).collect()
    }
}

pub struct ColumnFields {
    warmed_column_fields: WarmedColumnFields,
    host_field: FieldEnum,
}

impl ColumnFields {
    pub fn new<F: Field>(warmed_column_fields: WarmedColumnFields, host_field: F) -> Self {
        Self {
            warmed_column_fields,
            host_field: host_field.into(),
        }
    }
}

pub struct TopDocsCollector<S = DefaultDocumentScorer, D = NoDeduplicator> {
    shard_id: Option<ShardId>,
    limit: Option<usize>,
    offset: Option<usize>,
    perform_offset: bool,
    deduplicator: D,
    column_fields: Option<ColumnFields>,
    _phantom: PhantomData<S>,
}

impl<S> From<EdgeLimit> for TopDocsCollector<S, NoDeduplicator> {
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

impl<S> Default for TopDocsCollector<S, NoDeduplicator> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> TopDocsCollector<S, NoDeduplicator> {
    pub fn new() -> Self {
        Self {
            shard_id: None,
            limit: None,
            offset: None,
            perform_offset: true,
            deduplicator: NoDeduplicator,
            column_fields: None,
            _phantom: PhantomData,
        }
    }
}

impl<S, D> TopDocsCollector<S, D> {
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

    pub fn with_deduplicator<D2: Deduplicator>(self, deduplicator: D2) -> TopDocsCollector<S, D2> {
        TopDocsCollector {
            deduplicator,
            shard_id: self.shard_id,
            limit: self.limit,
            offset: self.offset,
            perform_offset: self.perform_offset,
            column_fields: self.column_fields,
            _phantom: PhantomData,
        }
    }

    pub fn with_column_fields<F: Field>(
        self,
        warmed_column_fields: WarmedColumnFields,
        host_field: F,
    ) -> Self {
        Self {
            column_fields: Some(ColumnFields::new(warmed_column_fields, host_field)),
            ..self
        }
    }
}

impl<S, D> TopDocsCollector<S, D>
where
    D: Deduplicator,
{
    fn computer(&self) -> Computer<D> {
        match (self.offset, self.limit) {
            (Some(offset), Some(limit)) => Computer::TopN(TopNComputer::new(limit + offset)),
            (Some(_), None) => Computer::All(AllComputer::new()),
            (None, Some(limit)) => Computer::TopN(TopNComputer::new(limit)),
            (None, None) => Computer::All(AllComputer::new()),
        }
    }
}

impl<S: DocumentScorer + 'static, D: Deduplicator + 'static> Collector for TopDocsCollector<S, D> {
    type Fruit = Vec<(tantivy::Score, <D as Deduplicator>::Doc)>;

    type Child = TopDocsSegmentCollector<S, D>;

    fn for_segment(
        &self,
        segment_ord: SegmentOrdinal,
        segment: &tantivy::SegmentReader,
    ) -> crate::Result<Self::Child> {
        let scorer = S::for_segment(segment)?;

        let segment_id = segment.segment_id();

        Ok(TopDocsSegmentCollector {
            shard_id: self.shard_id.unwrap(),
            computer: self.computer(),
            segment_ord,
            scorer,
            host_column: self.column_fields.as_ref().map(|cf| {
                cf.warmed_column_fields
                    .segment(&segment_id)
                    .u64_by_enum(cf.host_field)
                    .unwrap()
            }),
            _deduplicator: PhantomData,
        })
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as tantivy::collector::SegmentCollector>::Fruit>,
    ) -> crate::Result<Self::Fruit> {
        let mut computer = self.computer();

        for (score, doc) in self
            .deduplicator
            .deduplicate(segment_fruits.into_iter().flatten().collect())
        {
            computer.push(score, doc);
        }

        let mut result = computer.harvest();

        if self.perform_offset {
            result = result.into_iter().skip(self.offset.unwrap_or(0)).collect();
        }

        Ok(result)
    }
}

enum Computer<D: Deduplicator> {
    TopN(TopNComputer<tantivy::Score, <D as Deduplicator>::Doc>),
    All(AllComputer<D>),
}

impl<D: Deduplicator> Computer<D> {
    fn push(&mut self, score: tantivy::Score, doc: <D as Deduplicator>::Doc) {
        match self {
            Computer::TopN(computer) => computer.push(score, doc),
            Computer::All(computer) => computer.push(score, doc),
        }
    }

    fn harvest(self) -> Vec<(tantivy::Score, <D as Deduplicator>::Doc)> {
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

struct AllComputer<D: Deduplicator> {
    docs: Vec<(tantivy::Score, <D as Deduplicator>::Doc)>,
}

impl<D: Deduplicator> AllComputer<D> {
    fn new() -> Self {
        Self { docs: Vec::new() }
    }

    fn push(&mut self, score: tantivy::Score, doc: <D as Deduplicator>::Doc) {
        self.docs.push((score, doc));
    }

    fn harvest(self) -> Vec<(tantivy::Score, <D as Deduplicator>::Doc)> {
        let mut docs = self.docs;
        docs.sort_by(|(score1, _), (score2, _)| score2.total_cmp(score1));
        docs
    }
}

pub struct TopDocsSegmentCollector<S: DocumentScorer, D: Deduplicator> {
    shard_id: ShardId,
    computer: Computer<D>,
    segment_ord: SegmentOrdinal,
    scorer: S,
    host_column: Option<Column<u64>>,
    _deduplicator: PhantomData<D>,
}

impl<S: DocumentScorer + 'static, D: Deduplicator + 'static> SegmentCollector
    for TopDocsSegmentCollector<S, D>
{
    type Fruit = Vec<(tantivy::Score, <D as Deduplicator>::Doc)>;

    fn collect(&mut self, doc: DocId, _: tantivy::Score) {
        if doc == tantivy::TERMINATED {
            return;
        }

        let score = self.scorer.score(doc);
        self.computer
            .push(score, <D::Doc as DeduplicatorDoc>::new(self, doc));
    }

    fn harvest(self) -> Self::Fruit {
        self.computer.harvest()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        webgraph::{
            query::{BacklinksQuery, HostBacklinksQuery},
            Edge, Node, Webgraph,
        },
        webpage::RelFlags,
    };

    #[test]
    fn test_simple() {
        let temp_dir = crate::gen_temp_dir().unwrap();
        let mut graph = Webgraph::builder(&temp_dir, 0u64.into()).open().unwrap();

        graph
            .insert(Edge {
                from: Node::from("https://A.com/1"),
                to: Node::from("https://B.com/1"),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();

        graph.commit().unwrap();

        let res = graph
            .search(&BacklinksQuery::new(Node::from("https://B.com/1").id()))
            .unwrap();

        assert_eq!(res.len(), 1);
        assert!(res[0].from == Node::from("https://A.com/1").id());
    }

    #[test]
    fn test_deduplication() {
        let temp_dir = crate::gen_temp_dir().unwrap();
        let mut graph = Webgraph::builder(&temp_dir, 0u64.into()).open().unwrap();

        graph
            .insert(Edge {
                from: Node::from("https://A.com/1"),
                to: Node::from("https://B.com/1"),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();
        graph
            .insert(Edge {
                from: Node::from("https://A.com/2"),
                to: Node::from("https://B.com/1"),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();

        graph.commit().unwrap();

        let res = graph
            .search(&BacklinksQuery::new(Node::from("https://B.com/1").id()))
            .unwrap();

        assert_eq!(res.len(), 2);

        let res = graph
            .search(&HostBacklinksQuery::new(
                Node::from("https://B.com/").into_host().id(),
            ))
            .unwrap();

        assert_eq!(res.len(), 1);
    }

    #[test]
    fn test_deduplication_across_segments() {
        let temp_dir = crate::gen_temp_dir().unwrap();
        let mut graph = Webgraph::builder(&temp_dir, 0u64.into()).open().unwrap();

        graph
            .insert(Edge {
                from: Node::from("https://A.com/1"),
                to: Node::from("https://B.com/1"),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();
        graph.commit().unwrap();

        graph
            .insert(Edge {
                from: Node::from("https://A.com/2"),
                to: Node::from("https://B.com/1"),
                rel_flags: RelFlags::default(),
                label: String::new(),
                sort_score: 0.0,
            })
            .unwrap();

        graph.commit().unwrap();

        let res = graph
            .search(&HostBacklinksQuery::new(
                Node::from("https://B.com/").into_host().id(),
            ))
            .unwrap();

        assert_eq!(res.len(), 1);
    }
}
