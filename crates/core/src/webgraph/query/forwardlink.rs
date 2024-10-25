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

use super::{
    backlink::{fetch_edges, fetch_small_edges},
    collector::TopDocsCollector,
    raw::{host_links::HostLinksQuery, links::LinksQuery},
    Query,
};
use crate::{
    ampc::dht::ShardId,
    webgraph::{
        document::Edge,
        schema::{FromHostId, FromId, ToHostId, ToId},
        EdgeLimit, Node, NodeID, Searcher, SmallEdge,
    },
    Result,
};

use itertools::Itertools;
use tantivy::query::ShortCircuitQuery;

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct ForwardlinksQuery {
    node: NodeID,
    limit: EdgeLimit,
}

impl ForwardlinksQuery {
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

impl Query for ForwardlinksQuery {
    type Collector = TopDocsCollector;
    type TantivyQuery = Box<dyn tantivy::query::Query>;
    type IntermediateOutput = Vec<(f32, SmallEdge)>;
    type Output = Vec<SmallEdge>;

    fn tantivy_query(&self, _: &Searcher) -> Self::TantivyQuery {
        match self.limit {
            EdgeLimit::Unlimited => Box::new(LinksQuery::new(self.node, FromId)),
            EdgeLimit::Limit(limit) | EdgeLimit::LimitAndOffset { limit, .. } => Box::new(
                ShortCircuitQuery::new(Box::new(LinksQuery::new(self.node, FromId)), limit as u64),
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

    fn filter_fruit_shards(
        &self,
        shard: crate::ampc::dht::ShardId,
        fruit: <Self::Collector as super::Collector>::Fruit,
    ) -> <Self::Collector as super::Collector>::Fruit {
        fruit
            .into_iter()
            .filter(|(_, doc)| doc.shard_id == shard)
            .collect()
    }

    fn retrieve(
        &self,
        searcher: &Searcher,
        fruit: <Self::Collector as super::collector::Collector>::Fruit,
    ) -> Result<Self::IntermediateOutput> {
        let (scores, docs): (Vec<_>, Vec<_>) = fruit.into_iter().unzip();
        let nodes = fetch_small_edges(searcher, docs, ToId)?;
        Ok(scores
            .into_iter()
            .zip_eq(nodes.into_iter().map(|(node, rel_flags)| SmallEdge {
                from: self.node,
                to: node,
                rel_flags,
            }))
            .collect())
    }

    fn merge_results(results: Vec<Self::IntermediateOutput>) -> Self::Output {
        let mut edges: Vec<_> = results.into_iter().flatten().collect();
        edges.sort_by(|(a, _), (b, _)| b.total_cmp(a));
        edges.into_iter().map(|(_, e)| e).collect()
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct HostForwardlinksQuery {
    node: NodeID,
    limit: EdgeLimit,
}

impl HostForwardlinksQuery {
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

impl Query for HostForwardlinksQuery {
    type Collector = TopDocsCollector;
    type TantivyQuery = Box<dyn tantivy::query::Query>;
    type IntermediateOutput = Vec<(f32, SmallEdge)>;
    type Output = Vec<SmallEdge>;

    fn tantivy_query(&self, searcher: &Searcher) -> Self::TantivyQuery {
        match self.limit {
            EdgeLimit::Unlimited => Box::new(HostLinksQuery::new(
                self.node,
                FromHostId,
                ToHostId,
                searcher.warmed_column_fields().clone(),
            )),
            EdgeLimit::Limit(limit) | EdgeLimit::LimitAndOffset { limit, .. } => {
                Box::new(ShortCircuitQuery::new(
                    Box::new(HostLinksQuery::new(
                        self.node,
                        FromHostId,
                        ToHostId,
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

    fn filter_fruit_shards(
        &self,
        shard: crate::ampc::dht::ShardId,
        fruit: <Self::Collector as super::Collector>::Fruit,
    ) -> <Self::Collector as super::Collector>::Fruit {
        fruit
            .into_iter()
            .filter(|(_, doc)| doc.shard_id == shard)
            .collect()
    }

    fn retrieve(
        &self,
        searcher: &Searcher,
        fruit: <Self::Collector as super::collector::Collector>::Fruit,
    ) -> Result<Self::IntermediateOutput> {
        let (scores, docs): (Vec<_>, Vec<_>) = fruit.into_iter().unzip();
        let nodes = fetch_small_edges(searcher, docs, ToHostId)?;
        Ok(scores
            .into_iter()
            .zip_eq(nodes.into_iter().map(|(node, rel_flags)| SmallEdge {
                from: self.node,
                to: node,
                rel_flags,
            }))
            .collect())
    }

    fn merge_results(results: Vec<Self::IntermediateOutput>) -> Self::Output {
        let mut edges: Vec<_> = results.into_iter().flatten().collect();
        edges.sort_by(|(a, _), (b, _)| b.total_cmp(a));
        edges.into_iter().map(|(_, e)| e).collect()
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct FullForwardlinksQuery {
    node: Node,
    limit: EdgeLimit,
}

impl FullForwardlinksQuery {
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

impl Query for FullForwardlinksQuery {
    type Collector = TopDocsCollector;
    type TantivyQuery = Box<dyn tantivy::query::Query>;
    type IntermediateOutput = Vec<(f32, Edge)>;
    type Output = Vec<Edge>;

    fn tantivy_query(&self, _: &Searcher) -> Self::TantivyQuery {
        match self.limit {
            EdgeLimit::Unlimited => Box::new(LinksQuery::new(self.node.id(), FromId)),
            EdgeLimit::Limit(limit) | EdgeLimit::LimitAndOffset { limit, .. } => {
                Box::new(ShortCircuitQuery::new(
                    Box::new(LinksQuery::new(self.node.id(), FromId)),
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

    fn filter_fruit_shards(
        &self,
        shard: crate::ampc::dht::ShardId,
        fruit: <Self::Collector as super::Collector>::Fruit,
    ) -> <Self::Collector as super::Collector>::Fruit {
        fruit
            .into_iter()
            .filter(|(_, doc)| doc.shard_id == shard)
            .collect()
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

    fn merge_results(results: Vec<Self::IntermediateOutput>) -> Self::Output {
        let mut edges: Vec<_> = results.into_iter().flatten().collect();
        edges.sort_by(|(a, _), (b, _)| b.total_cmp(a));
        edges.into_iter().map(|(_, e)| e).collect()
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct FullHostForwardlinksQuery {
    node: Node,
    limit: EdgeLimit,
}

impl FullHostForwardlinksQuery {
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

impl Query for FullHostForwardlinksQuery {
    type Collector = TopDocsCollector;
    type TantivyQuery = Box<dyn tantivy::query::Query>;
    type IntermediateOutput = Vec<(f32, Edge)>;
    type Output = Vec<Edge>;

    fn tantivy_query(&self, searcher: &Searcher) -> Self::TantivyQuery {
        match self.limit {
            EdgeLimit::Unlimited => Box::new(HostLinksQuery::new(
                self.node.clone().into_host().id(),
                FromHostId,
                ToHostId,
                searcher.warmed_column_fields().clone(),
            )),
            EdgeLimit::Limit(limit) | EdgeLimit::LimitAndOffset { limit, .. } => {
                Box::new(ShortCircuitQuery::new(
                    Box::new(HostLinksQuery::new(
                        self.node.clone().into_host().id(),
                        FromHostId,
                        ToHostId,
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

    fn filter_fruit_shards(
        &self,
        shard: crate::ampc::dht::ShardId,
        fruit: <Self::Collector as super::Collector>::Fruit,
    ) -> <Self::Collector as super::Collector>::Fruit {
        fruit
            .into_iter()
            .filter(|(_, doc)| doc.shard_id == shard)
            .collect()
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

    fn merge_results(results: Vec<Self::IntermediateOutput>) -> Self::Output {
        let mut edges: Vec<_> = results.into_iter().flatten().collect();
        edges.sort_by(|(a, _), (b, _)| b.total_cmp(a));
        edges.into_iter().map(|(_, e)| e).collect()
    }
}
