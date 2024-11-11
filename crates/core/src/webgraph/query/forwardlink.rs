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
    collector::{top_docs, HostDeduplicator, TopDocsCollector},
    document_scorer::DefaultDocumentScorer,
    raw::{host_links::HostLinksQuery, links::LinksQuery},
    AndFilter, Filter, FilterEnum, Query,
};
use crate::{
    webgraph::{
        document::Edge,
        schema::{FromHostId, FromId, ToHostId, ToId},
        EdgeLimit, Node, NodeID, Searcher, SmallEdge,
    },
    Result,
};

use tantivy::query::{BooleanQuery, Occur, ShortCircuitQuery};

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct ForwardlinksQuery {
    node: NodeID,
    limit: EdgeLimit,
    filters: Vec<FilterEnum>,
}

impl ForwardlinksQuery {
    pub fn new(node: NodeID) -> Self {
        Self {
            node,
            limit: EdgeLimit::Unlimited,
            filters: Vec::new(),
        }
    }

    pub fn with_limit(self, limit: EdgeLimit) -> Self {
        Self {
            node: self.node,
            limit,
            filters: self.filters,
        }
    }

    pub fn filter<F: Filter>(mut self, filter: F) -> Self {
        self.filters.push(filter.into());
        self
    }

    fn filter_as_and(&self) -> Option<AndFilter> {
        if self.filters.is_empty() {
            None
        } else {
            let mut filter = AndFilter::new();

            for f in self.filters.clone() {
                filter = filter.and(f);
            }

            Some(filter)
        }
    }
}

impl Query for ForwardlinksQuery {
    type Collector = TopDocsCollector;
    type TantivyQuery = Box<dyn tantivy::query::Query>;
    type IntermediateOutput = Vec<(f64, SmallEdge)>;
    type Output = Vec<SmallEdge>;

    fn tantivy_query(&self, searcher: &Searcher) -> Self::TantivyQuery {
        let mut raw =
            Box::new(LinksQuery::new(self.node, FromId)) as Box<dyn tantivy::query::Query>;

        if let Some(filter) = self.filter_as_and().and_then(|f| f.inverted_index_filter()) {
            let filter = filter.query(searcher);
            let mut queries = vec![(Occur::Must, raw)];
            queries.extend(filter);
            raw = Box::new(BooleanQuery::new(queries));
        }

        match self.limit {
            EdgeLimit::Unlimited => raw,
            EdgeLimit::Limit(limit) => Box::new(ShortCircuitQuery::new(
                raw,
                (limit + top_docs::DEDUPLICATION_BUFFER) as u64,
            )),
            EdgeLimit::LimitAndOffset { limit, offset } => Box::new(ShortCircuitQuery::new(
                raw,
                (limit + offset + top_docs::DEDUPLICATION_BUFFER) as u64,
            )),
        }
    }

    fn collector(&self, searcher: &Searcher) -> Self::Collector {
        let mut collector = TopDocsCollector::from(self.limit)
            .with_shard_id(searcher.shard())
            .disable_offset()
            .with_column_fields(searcher.warmed_column_fields().clone());

        if let Some(filter) = self.filter_as_and().and_then(|f| f.column_field_filter()) {
            collector = collector.with_filter(filter);
        }

        collector
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
        let docs: Vec<_> = fruit.into_iter().map(|(_, doc)| doc).collect();
        let nodes = fetch_small_edges(searcher, docs, ToId)?;
        Ok(nodes
            .into_iter()
            .map(|(node, rel_flags, score)| {
                (
                    score,
                    SmallEdge {
                        from: self.node,
                        to: node,
                        rel_flags,
                    },
                )
            })
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
    filters: Vec<FilterEnum>,
}

impl HostForwardlinksQuery {
    pub fn new(node: NodeID) -> Self {
        Self {
            node,
            limit: EdgeLimit::Unlimited,
            filters: Vec::new(),
        }
    }

    pub fn with_limit(self, limit: EdgeLimit) -> Self {
        Self {
            node: self.node,
            limit,
            filters: self.filters,
        }
    }

    pub fn filter<F: Filter>(mut self, filter: F) -> Self {
        self.filters.push(filter.into());
        self
    }

    fn filter_as_and(&self) -> Option<AndFilter> {
        if self.filters.is_empty() {
            None
        } else {
            let mut filter = AndFilter::new();
            for f in self.filters.clone() {
                filter = filter.and(f);
            }
            Some(filter)
        }
    }
}

impl Query for HostForwardlinksQuery {
    type Collector = TopDocsCollector<DefaultDocumentScorer, HostDeduplicator>;
    type TantivyQuery = Box<dyn tantivy::query::Query>;
    type IntermediateOutput = Vec<(f64, SmallEdge)>;
    type Output = Vec<SmallEdge>;

    fn tantivy_query(&self, searcher: &Searcher) -> Self::TantivyQuery {
        let mut raw = Box::new(HostLinksQuery::new(
            self.node,
            FromHostId,
            ToHostId,
            searcher.warmed_column_fields().clone(),
        )) as Box<dyn tantivy::query::Query>;

        if let Some(filter) = self.filter_as_and().and_then(|f| f.inverted_index_filter()) {
            let filter = filter.query(searcher);
            let mut queries = vec![(Occur::Must, raw)];
            queries.extend(filter);
            raw = Box::new(BooleanQuery::new(queries));
        }

        match self.limit {
            EdgeLimit::Unlimited => raw,
            EdgeLimit::Limit(limit) => Box::new(ShortCircuitQuery::new(
                raw,
                (limit + top_docs::DEDUPLICATION_BUFFER) as u64,
            )),
            EdgeLimit::LimitAndOffset { limit, offset } => Box::new(ShortCircuitQuery::new(
                raw,
                (limit + offset + top_docs::DEDUPLICATION_BUFFER) as u64,
            )),
        }
    }

    fn collector(&self, searcher: &Searcher) -> Self::Collector {
        let mut collector = TopDocsCollector::from(self.limit)
            .with_shard_id(searcher.shard())
            .disable_offset()
            .with_deduplicator(HostDeduplicator)
            .with_column_fields(searcher.warmed_column_fields().clone())
            .with_host_field(ToHostId);

        if let Some(filter) = self.filter_as_and().and_then(|f| f.column_field_filter()) {
            collector = collector.with_filter(filter);
        }

        collector
    }

    fn remote_collector(&self) -> Self::Collector {
        TopDocsCollector::from(self.limit)
            .enable_offset()
            .with_deduplicator(HostDeduplicator)
    }

    fn filter_fruit_shards(
        &self,
        shard: crate::ampc::dht::ShardId,
        fruit: <Self::Collector as super::Collector>::Fruit,
    ) -> <Self::Collector as super::Collector>::Fruit {
        fruit
            .into_iter()
            .filter(|(_, doc)| doc.address.shard_id == shard)
            .collect()
    }

    fn retrieve(
        &self,
        searcher: &Searcher,
        fruit: <Self::Collector as super::collector::Collector>::Fruit,
    ) -> Result<Self::IntermediateOutput> {
        let docs: Vec<_> = fruit.into_iter().map(|(_, doc)| doc.address).collect();
        let nodes = fetch_small_edges(searcher, docs, ToHostId)?;
        Ok(nodes
            .into_iter()
            .map(|(node, rel_flags, score)| {
                (
                    score,
                    SmallEdge {
                        from: self.node,
                        to: node,
                        rel_flags,
                    },
                )
            })
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
    filters: Vec<FilterEnum>,
}

impl FullForwardlinksQuery {
    pub fn new(node: Node) -> Self {
        Self {
            node,
            limit: EdgeLimit::Unlimited,
            filters: Vec::new(),
        }
    }

    pub fn with_limit(self, limit: EdgeLimit) -> Self {
        Self {
            node: self.node,
            limit,
            filters: self.filters,
        }
    }

    pub fn filter<F: Filter>(mut self, filter: F) -> Self {
        self.filters.push(filter.into());
        self
    }

    fn filter_as_and(&self) -> Option<AndFilter> {
        if self.filters.is_empty() {
            None
        } else {
            let mut filter = AndFilter::new();
            for f in self.filters.clone() {
                filter = filter.and(f);
            }
            Some(filter)
        }
    }
}

impl Query for FullForwardlinksQuery {
    type Collector = TopDocsCollector;
    type TantivyQuery = Box<dyn tantivy::query::Query>;
    type IntermediateOutput = Vec<Edge>;
    type Output = Vec<Edge>;

    fn tantivy_query(&self, searcher: &Searcher) -> Self::TantivyQuery {
        let mut raw =
            Box::new(LinksQuery::new(self.node.id(), FromId)) as Box<dyn tantivy::query::Query>;

        if let Some(filter) = self.filter_as_and().and_then(|f| f.inverted_index_filter()) {
            let filter = filter.query(searcher);
            let mut queries = vec![(Occur::Must, raw)];
            queries.extend(filter);
            raw = Box::new(BooleanQuery::new(queries));
        }

        match self.limit {
            EdgeLimit::Unlimited => raw,
            EdgeLimit::Limit(limit) => Box::new(ShortCircuitQuery::new(
                raw,
                (limit + top_docs::DEDUPLICATION_BUFFER) as u64,
            )),
            EdgeLimit::LimitAndOffset { limit, offset } => Box::new(ShortCircuitQuery::new(
                raw,
                (limit + offset + top_docs::DEDUPLICATION_BUFFER) as u64,
            )),
        }
    }

    fn collector(&self, searcher: &Searcher) -> Self::Collector {
        let mut collector = TopDocsCollector::from(self.limit)
            .with_shard_id(searcher.shard())
            .disable_offset()
            .with_column_fields(searcher.warmed_column_fields().clone());

        if let Some(filter) = self.filter_as_and().and_then(|f| f.column_field_filter()) {
            collector = collector.with_filter(filter);
        }

        collector
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
        let docs: Vec<_> = fruit.into_iter().map(|(_, doc)| doc).collect();
        let edges = fetch_edges(searcher, docs)?;
        Ok(edges)
    }

    fn merge_results(results: Vec<Self::IntermediateOutput>) -> Self::Output {
        let mut edges: Vec<_> = results.into_iter().flatten().collect();
        edges.sort_by(|a, b| b.sort_score.total_cmp(&a.sort_score));
        edges
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct FullHostForwardlinksQuery {
    node: Node,
    limit: EdgeLimit,
    filters: Vec<FilterEnum>,
}

impl FullHostForwardlinksQuery {
    pub fn new(node: Node) -> Self {
        Self {
            node,
            limit: EdgeLimit::Unlimited,
            filters: Vec::new(),
        }
    }

    pub fn with_limit(self, limit: EdgeLimit) -> Self {
        Self {
            node: self.node,
            limit,
            filters: self.filters,
        }
    }

    pub fn filter<F: Filter>(mut self, filter: F) -> Self {
        self.filters.push(filter.into());
        self
    }

    fn filter_as_and(&self) -> Option<AndFilter> {
        if self.filters.is_empty() {
            None
        } else {
            let mut filter = AndFilter::new();
            for f in self.filters.clone() {
                filter = filter.and(f);
            }
            Some(filter)
        }
    }
}

impl Query for FullHostForwardlinksQuery {
    type Collector = TopDocsCollector<DefaultDocumentScorer, HostDeduplicator>;
    type TantivyQuery = Box<dyn tantivy::query::Query>;
    type IntermediateOutput = Vec<Edge>;
    type Output = Vec<Edge>;

    fn tantivy_query(&self, searcher: &Searcher) -> Self::TantivyQuery {
        let mut raw = Box::new(HostLinksQuery::new(
            self.node.clone().into_host().id(),
            FromHostId,
            ToHostId,
            searcher.warmed_column_fields().clone(),
        )) as Box<dyn tantivy::query::Query>;

        if let Some(filter) = self.filter_as_and().and_then(|f| f.inverted_index_filter()) {
            let filter = filter.query(searcher);
            let mut queries = vec![(Occur::Must, raw)];
            queries.extend(filter);
            raw = Box::new(BooleanQuery::new(queries));
        }

        match self.limit {
            EdgeLimit::Unlimited => raw,
            EdgeLimit::Limit(limit) => Box::new(ShortCircuitQuery::new(
                raw,
                (limit + top_docs::DEDUPLICATION_BUFFER) as u64,
            )),
            EdgeLimit::LimitAndOffset { limit, offset } => Box::new(ShortCircuitQuery::new(
                raw,
                (limit + offset + top_docs::DEDUPLICATION_BUFFER) as u64,
            )),
        }
    }

    fn collector(&self, searcher: &Searcher) -> Self::Collector {
        let mut collector = TopDocsCollector::from(self.limit)
            .with_shard_id(searcher.shard())
            .disable_offset()
            .with_deduplicator(HostDeduplicator)
            .with_column_fields(searcher.warmed_column_fields().clone())
            .with_host_field(ToHostId);

        if let Some(filter) = self.filter_as_and().and_then(|f| f.column_field_filter()) {
            collector = collector.with_filter(filter);
        }

        collector
    }

    fn remote_collector(&self) -> Self::Collector {
        TopDocsCollector::from(self.limit)
            .enable_offset()
            .with_deduplicator(HostDeduplicator)
    }

    fn filter_fruit_shards(
        &self,
        shard: crate::ampc::dht::ShardId,
        fruit: <Self::Collector as super::Collector>::Fruit,
    ) -> <Self::Collector as super::Collector>::Fruit {
        fruit
            .into_iter()
            .filter(|(_, doc)| doc.address.shard_id == shard)
            .collect()
    }

    fn retrieve(
        &self,
        searcher: &Searcher,
        fruit: <Self::Collector as super::collector::Collector>::Fruit,
    ) -> Result<Self::IntermediateOutput> {
        let docs: Vec<_> = fruit.into_iter().map(|(_, doc)| doc.address).collect();
        let edges = fetch_edges(searcher, docs)?;
        Ok(edges
            .into_iter()
            .map(|e| Edge {
                from: e.from.into_host(),
                to: e.to.into_host(),
                ..e
            })
            .collect())
    }

    fn merge_results(results: Vec<Self::IntermediateOutput>) -> Self::Output {
        let mut edges: Vec<_> = results.into_iter().flatten().collect();
        edges.sort_by(|a, b| b.sort_score.total_cmp(&a.sort_score));
        edges
    }
}
