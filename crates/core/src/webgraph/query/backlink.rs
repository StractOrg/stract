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

use itertools::Itertools;
use tantivy::query::ShortCircuitQuery;

use super::{
    collector::TopDocsCollector,
    raw::{HostLinksQuery, LinksQuery},
    Query,
};
use crate::{
    ampc::dht::ShardId,
    webgraph::{
        doc_address::DocAddress,
        document::Edge,
        schema::{Field, FromHostId, FromId, RelFlags, ToHostId, ToId},
        searcher::Searcher,
        EdgeLimit, Node, NodeID, SmallEdge, SmallEdgeWithLabel,
    },
    Result,
};

pub fn fetch_small_edges<F: Field>(
    searcher: &Searcher,
    mut doc_ids: Vec<DocAddress>,
    node_id_field: F,
) -> Result<Vec<(NodeID, crate::webpage::RelFlags)>> {
    doc_ids.sort_unstable_by_key(|doc| doc.segment_ord);
    let mut prev_segment_id = None;
    let mut field_column = None;
    let mut rel_flags_column = None;

    let mut edges = Vec::with_capacity(doc_ids.len());

    for doc in doc_ids {
        if Some(doc.segment_ord) != prev_segment_id {
            prev_segment_id = Some(doc.segment_ord);
            let segment_column_fields = searcher.warmed_column_fields().segment(
                &searcher
                    .tantivy_searcher()
                    .segment_reader(doc.segment_ord)
                    .segment_id(),
            );
            field_column = Some(segment_column_fields.u64(node_id_field).unwrap());
            rel_flags_column = Some(segment_column_fields.u64(RelFlags).unwrap());
        }

        let Some(id) = field_column.as_ref().unwrap().first(doc.doc_id) else {
            continue;
        };
        let Some(rel_flags) = rel_flags_column.as_ref().unwrap().first(doc.doc_id) else {
            continue;
        };

        edges.push((NodeID::from(id), crate::webpage::RelFlags::from(rel_flags)));
    }

    Ok(edges)
}

pub fn fetch_edges(searcher: &Searcher, mut doc_ids: Vec<DocAddress>) -> Result<Vec<Edge>> {
    doc_ids.sort_unstable_by_key(|doc| doc.segment_ord);

    let mut edges = Vec::with_capacity(doc_ids.len());

    for doc in doc_ids {
        edges.push(searcher.tantivy_searcher().doc(doc.into())?);
    }

    Ok(edges)
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct BacklinksQuery {
    node: NodeID,
    limit: EdgeLimit,
}

impl BacklinksQuery {
    pub fn new(node: NodeID) -> Self {
        Self {
            node,
            limit: EdgeLimit::Unlimited,
        }
    }

    pub fn with_limit(self, limit: EdgeLimit) -> Self {
        Self {
            node: self.node,
            limit,
        }
    }
}

impl Query for BacklinksQuery {
    type Collector = TopDocsCollector;
    type TantivyQuery = Box<dyn tantivy::query::Query>;
    type IntermediateOutput = Vec<(f32, SmallEdge)>;
    type Output = Vec<SmallEdge>;

    fn tantivy_query(&self, _: &Searcher) -> Self::TantivyQuery {
        match self.limit {
            EdgeLimit::Unlimited => Box::new(LinksQuery::new(self.node, ToId)),
            EdgeLimit::Limit(limit) | EdgeLimit::LimitAndOffset { limit, .. } => Box::new(
                ShortCircuitQuery::new(Box::new(LinksQuery::new(self.node, ToId)), limit as u64),
            ),
        }
    }

    fn collector(&self, shard_id: ShardId) -> Self::Collector {
        TopDocsCollector::from(self.limit)
            .with_shard_id(shard_id)
            .disable_offset()
    }

    fn remote_collector(&self) -> Self::Collector {
        TopDocsCollector::from(self.limit).enable_offset()
    }

    fn retrieve(
        &self,
        searcher: &Searcher,
        fruit: <Self::Collector as super::collector::Collector>::Fruit,
    ) -> Result<Self::IntermediateOutput> {
        let (scores, docs): (Vec<_>, Vec<_>) = fruit.into_iter().unzip();
        let nodes = fetch_small_edges(searcher, docs, FromId)?;
        Ok(scores
            .into_iter()
            .zip_eq(nodes.into_iter().map(|(node, rel_flags)| SmallEdge {
                from: node,
                to: self.node,
                rel_flags,
            }))
            .collect())
    }

    fn filter_fruit_shards(
        &self,
        shard_id: ShardId,
        fruit: <Self::Collector as super::Collector>::Fruit,
    ) -> <Self::Collector as super::Collector>::Fruit {
        fruit
            .into_iter()
            .filter(|(_, doc)| doc.shard_id == shard_id)
            .collect()
    }

    fn merge_results(results: Vec<Self::IntermediateOutput>) -> Self::Output {
        let mut edges: Vec<_> = results.into_iter().flatten().collect();
        edges.sort_by(|(a, _), (b, _)| b.total_cmp(a));
        edges.into_iter().map(|(_, e)| e).collect()
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct HostBacklinksQuery {
    node: NodeID,
    limit: EdgeLimit,
}

impl HostBacklinksQuery {
    pub fn new(node: NodeID) -> Self {
        Self {
            node,
            limit: EdgeLimit::Unlimited,
        }
    }

    pub fn with_limit(mut self, limit: EdgeLimit) -> Self {
        self.limit = limit;
        self
    }
}

impl Query for HostBacklinksQuery {
    type Collector = TopDocsCollector;
    type TantivyQuery = Box<dyn tantivy::query::Query>;
    type IntermediateOutput = Vec<(f32, SmallEdge)>;
    type Output = Vec<SmallEdge>;

    fn tantivy_query(&self, searcher: &Searcher) -> Self::TantivyQuery {
        match self.limit {
            EdgeLimit::Unlimited => Box::new(HostLinksQuery::new(
                self.node,
                ToHostId,
                FromHostId,
                searcher.warmed_column_fields().clone(),
            )),
            EdgeLimit::Limit(limit) | EdgeLimit::LimitAndOffset { limit, .. } => {
                Box::new(ShortCircuitQuery::new(
                    Box::new(HostLinksQuery::new(
                        self.node,
                        ToHostId,
                        FromHostId,
                        searcher.warmed_column_fields().clone(),
                    )),
                    limit as u64,
                ))
            }
        }
    }

    fn collector(&self, shard_id: ShardId) -> Self::Collector {
        TopDocsCollector::from(self.limit)
            .with_shard_id(shard_id)
            .disable_offset()
    }

    fn remote_collector(&self) -> Self::Collector {
        TopDocsCollector::from(self.limit).enable_offset()
    }

    fn retrieve(
        &self,
        searcher: &Searcher,
        fruit: <Self::Collector as super::collector::Collector>::Fruit,
    ) -> Result<Self::IntermediateOutput> {
        let (scores, docs): (Vec<_>, Vec<_>) = fruit.into_iter().unzip();
        let nodes = fetch_small_edges(searcher, docs, FromHostId)?;
        Ok(scores
            .into_iter()
            .zip_eq(nodes.into_iter().map(|(node, rel_flags)| SmallEdge {
                from: node,
                to: self.node,
                rel_flags,
            }))
            .collect())
    }

    fn filter_fruit_shards(
        &self,
        shard_id: ShardId,
        fruit: <Self::Collector as super::Collector>::Fruit,
    ) -> <Self::Collector as super::Collector>::Fruit {
        fruit
            .into_iter()
            .filter(|(_, doc)| doc.shard_id == shard_id)
            .collect()
    }

    fn merge_results(results: Vec<Self::IntermediateOutput>) -> Self::Output {
        let mut edges: Vec<_> = results.into_iter().flatten().collect();
        edges.sort_by(|(a, _), (b, _)| b.total_cmp(a));
        edges.into_iter().map(|(_, e)| e).collect()
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct FullBacklinksQuery {
    node: Node,
    limit: EdgeLimit,
}

impl FullBacklinksQuery {
    pub fn new(node: Node) -> Self {
        Self {
            node,
            limit: EdgeLimit::Unlimited,
        }
    }

    pub fn with_limit(self, limit: EdgeLimit) -> Self {
        Self {
            node: self.node,
            limit,
        }
    }
}

impl Query for FullBacklinksQuery {
    type Collector = TopDocsCollector;
    type TantivyQuery = Box<dyn tantivy::query::Query>;
    type IntermediateOutput = Vec<(f32, Edge)>;
    type Output = Vec<Edge>;

    fn tantivy_query(&self, _: &Searcher) -> Self::TantivyQuery {
        match self.limit {
            EdgeLimit::Unlimited => Box::new(LinksQuery::new(self.node.id(), ToId)),
            EdgeLimit::Limit(limit) | EdgeLimit::LimitAndOffset { limit, .. } => {
                Box::new(ShortCircuitQuery::new(
                    Box::new(LinksQuery::new(self.node.id(), ToId)),
                    limit as u64,
                ))
            }
        }
    }

    fn collector(&self, shard_id: ShardId) -> Self::Collector {
        TopDocsCollector::from(self.limit)
            .with_shard_id(shard_id)
            .disable_offset()
    }

    fn remote_collector(&self) -> Self::Collector {
        TopDocsCollector::from(self.limit).enable_offset()
    }

    fn retrieve(
        &self,
        searcher: &Searcher,
        fruit: <Self::Collector as super::collector::Collector>::Fruit,
    ) -> Result<Self::IntermediateOutput> {
        let (scores, docs): (Vec<_>, Vec<_>) = fruit.into_iter().unzip();
        let edges = fetch_edges(searcher, docs)?;
        Ok(scores.into_iter().zip_eq(edges).collect())
    }

    fn filter_fruit_shards(
        &self,
        shard_id: ShardId,
        fruit: <Self::Collector as super::Collector>::Fruit,
    ) -> <Self::Collector as super::Collector>::Fruit {
        fruit
            .into_iter()
            .filter(|(_, doc)| doc.shard_id == shard_id)
            .collect()
    }

    fn merge_results(results: Vec<Self::IntermediateOutput>) -> Self::Output {
        let mut edges: Vec<_> = results.into_iter().flatten().collect();
        edges.sort_by(|(a, _), (b, _)| b.total_cmp(a));
        edges.into_iter().map(|(_, e)| e).collect()
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct FullHostBacklinksQuery {
    node: Node,
    limit: EdgeLimit,
}

impl FullHostBacklinksQuery {
    pub fn new(node: Node) -> Self {
        Self {
            node,
            limit: EdgeLimit::Unlimited,
        }
    }

    pub fn with_limit(self, limit: EdgeLimit) -> Self {
        Self {
            node: self.node,
            limit,
        }
    }
}

impl Query for FullHostBacklinksQuery {
    type Collector = TopDocsCollector;
    type TantivyQuery = Box<dyn tantivy::query::Query>;
    type IntermediateOutput = Vec<(f32, Edge)>;
    type Output = Vec<Edge>;

    fn tantivy_query(&self, searcher: &Searcher) -> Self::TantivyQuery {
        match self.limit {
            EdgeLimit::Unlimited => Box::new(HostLinksQuery::new(
                self.node.clone().into_host().id(),
                ToHostId,
                FromHostId,
                searcher.warmed_column_fields().clone(),
            )),
            EdgeLimit::Limit(limit) | EdgeLimit::LimitAndOffset { limit, .. } => {
                Box::new(ShortCircuitQuery::new(
                    Box::new(HostLinksQuery::new(
                        self.node.clone().into_host().id(),
                        ToHostId,
                        FromHostId,
                        searcher.warmed_column_fields().clone(),
                    )),
                    limit as u64,
                ))
            }
        }
    }

    fn collector(&self, shard_id: ShardId) -> Self::Collector {
        TopDocsCollector::from(self.limit)
            .with_shard_id(shard_id)
            .disable_offset()
    }

    fn remote_collector(&self) -> Self::Collector {
        TopDocsCollector::from(self.limit).enable_offset()
    }

    fn retrieve(
        &self,
        searcher: &Searcher,
        fruit: <Self::Collector as super::collector::Collector>::Fruit,
    ) -> Result<Self::IntermediateOutput> {
        let (scores, docs): (Vec<_>, Vec<_>) = fruit.into_iter().unzip();
        let edges = fetch_edges(searcher, docs)?;
        Ok(scores
            .into_iter()
            .zip_eq(edges.into_iter().map(|e| Edge {
                from: e.from.into_host(),
                to: e.to.into_host(),
                rel_flags: e.rel_flags,
                label: e.label,
                sort_score: e.sort_score,
            }))
            .collect())
    }

    fn filter_fruit_shards(
        &self,
        shard_id: ShardId,
        fruit: <Self::Collector as super::Collector>::Fruit,
    ) -> <Self::Collector as super::Collector>::Fruit {
        fruit
            .into_iter()
            .filter(|(_, doc)| doc.shard_id == shard_id)
            .collect()
    }

    fn merge_results(results: Vec<Self::IntermediateOutput>) -> Self::Output {
        let mut edges: Vec<_> = results.into_iter().flatten().collect();
        edges.sort_by(|(a, _), (b, _)| b.total_cmp(a));
        edges.into_iter().map(|(_, e)| e).collect()
    }
}
#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct BacklinksWithLabelsQuery {
    node: NodeID,
    limit: EdgeLimit,
}

impl BacklinksWithLabelsQuery {
    pub fn new(node: NodeID) -> Self {
        Self {
            node,
            limit: EdgeLimit::Unlimited,
        }
    }

    pub fn with_limit(self, limit: EdgeLimit) -> Self {
        Self {
            node: self.node,
            limit,
        }
    }
}

impl Query for BacklinksWithLabelsQuery {
    type Collector = TopDocsCollector;
    type TantivyQuery = Box<dyn tantivy::query::Query>;
    type IntermediateOutput = Vec<(f32, SmallEdgeWithLabel)>;
    type Output = Vec<SmallEdgeWithLabel>;

    fn tantivy_query(&self, _: &Searcher) -> Self::TantivyQuery {
        Box::new(LinksQuery::new(self.node, ToId))
    }

    fn collector(&self, shard_id: ShardId) -> Self::Collector {
        TopDocsCollector::from(self.limit)
            .with_shard_id(shard_id)
            .disable_offset()
    }

    fn remote_collector(&self) -> Self::Collector {
        TopDocsCollector::from(self.limit).enable_offset()
    }

    fn retrieve(
        &self,
        searcher: &Searcher,
        fruit: <Self::Collector as super::Collector>::Fruit,
    ) -> Result<Self::IntermediateOutput> {
        let (scores, docs): (Vec<_>, Vec<_>) = fruit.into_iter().unzip();
        let edges = fetch_edges(searcher, docs)?;
        Ok(scores
            .into_iter()
            .zip_eq(edges.into_iter().map(|e| SmallEdgeWithLabel {
                from: e.from.id(),
                to: e.to.id(),
                rel_flags: e.rel_flags,
                label: e.label,
            }))
            .collect())
    }

    fn filter_fruit_shards(
        &self,
        shard_id: ShardId,
        fruit: <Self::Collector as super::Collector>::Fruit,
    ) -> <Self::Collector as super::Collector>::Fruit {
        fruit
            .into_iter()
            .filter(|(_, doc)| doc.shard_id == shard_id)
            .collect()
    }

    fn merge_results(results: Vec<Self::IntermediateOutput>) -> Self::Output {
        let mut edges: Vec<_> = results.into_iter().flatten().collect();
        edges.sort_by(|(a, _), (b, _)| b.total_cmp(a));
        edges.into_iter().map(|(_, e)| e).collect()
    }
}
