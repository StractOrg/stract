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

use tantivy::query::{BooleanQuery, Occur, ShortCircuitQuery};

use super::{
    collector::{top_docs, HostDeduplicator, TopDocsCollector},
    document_scorer::DefaultDocumentScorer,
    raw::{HostLinksQuery, LinksQuery},
    AndFilter, Filter, FilterEnum, Query,
};
use crate::{
    ampc::dht::ShardId,
    webgraph::{
        doc_address::DocAddress,
        document::Edge,
        schema::{Field, FromHostId, FromId, RelFlags, SortScore, ToHostId, ToId},
        searcher::Searcher,
        EdgeLimit, Node, NodeID, SmallEdge, SmallEdgeWithLabel,
    },
    Result,
};

pub fn fetch_small_edges<F: Field>(
    searcher: &Searcher,
    mut doc_ids: Vec<DocAddress>,
    node_id_field: F,
) -> Result<Vec<(NodeID, crate::webpage::RelFlags, f64)>> {
    doc_ids.sort_unstable_by_key(|doc| doc.segment_ord);
    let mut prev_segment_id = None;
    let mut field_column = None;
    let mut score_column = None;
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
            score_column = Some(segment_column_fields.f64(SortScore).unwrap());
        }

        let Some(id) = field_column.as_ref().unwrap().first(doc.doc_id) else {
            continue;
        };
        let Some(rel_flags) = rel_flags_column.as_ref().unwrap().first(doc.doc_id) else {
            continue;
        };
        let Some(sort_score) = score_column.as_ref().unwrap().first(doc.doc_id) else {
            continue;
        };

        edges.push((
            NodeID::from(id),
            crate::webpage::RelFlags::from(rel_flags),
            sort_score,
        ));
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
    filters: Vec<FilterEnum>,
}

impl BacklinksQuery {
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

impl Query for BacklinksQuery {
    type Collector = TopDocsCollector;
    type TantivyQuery = Box<dyn tantivy::query::Query>;
    type IntermediateOutput = Vec<(f64, SmallEdge)>;
    type Output = Vec<SmallEdge>;

    fn tantivy_query(&self, searcher: &Searcher) -> Self::TantivyQuery {
        let mut raw = Box::new(LinksQuery::new(self.node, ToId)) as Box<dyn tantivy::query::Query>;

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

    fn retrieve(
        &self,
        searcher: &Searcher,
        fruit: <Self::Collector as super::collector::Collector>::Fruit,
    ) -> Result<Self::IntermediateOutput> {
        let docs: Vec<_> = fruit.into_iter().map(|(_, doc)| doc).collect();
        let nodes = fetch_small_edges(searcher, docs, FromId)?;
        Ok(nodes
            .into_iter()
            .map(|(node, rel_flags, sort_score)| {
                (
                    sort_score,
                    SmallEdge {
                        from: node,
                        to: self.node,
                        rel_flags,
                    },
                )
            })
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
    filters: Vec<FilterEnum>,
}

impl HostBacklinksQuery {
    pub fn new(node: NodeID) -> Self {
        Self {
            node,
            limit: EdgeLimit::Unlimited,
            filters: Vec::new(),
        }
    }

    pub fn with_limit(mut self, limit: EdgeLimit) -> Self {
        self.limit = limit;
        self
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

impl Query for HostBacklinksQuery {
    type Collector = TopDocsCollector<DefaultDocumentScorer, HostDeduplicator>;
    type TantivyQuery = Box<dyn tantivy::query::Query>;
    type IntermediateOutput = Vec<(f64, SmallEdge)>;
    type Output = Vec<SmallEdge>;

    fn tantivy_query(&self, searcher: &Searcher) -> Self::TantivyQuery {
        let mut raw = Box::new(HostLinksQuery::new(
            self.node,
            ToHostId,
            FromHostId,
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
            .with_host_field(FromHostId);

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

    fn retrieve(
        &self,
        searcher: &Searcher,
        fruit: <Self::Collector as super::collector::Collector>::Fruit,
    ) -> Result<Self::IntermediateOutput> {
        let docs: Vec<_> = fruit.into_iter().map(|(_, doc)| doc.address).collect();
        let nodes = fetch_small_edges(searcher, docs, FromHostId)?;
        Ok(nodes
            .into_iter()
            .map(|(node, rel_flags, score)| {
                (
                    score,
                    SmallEdge {
                        from: node,
                        to: self.node,
                        rel_flags,
                    },
                )
            })
            .collect())
    }

    fn filter_fruit_shards(
        &self,
        shard_id: ShardId,
        fruit: <Self::Collector as super::Collector>::Fruit,
    ) -> <Self::Collector as super::Collector>::Fruit {
        fruit
            .into_iter()
            .filter(|(_, doc)| doc.address.shard_id == shard_id)
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
    filters: Vec<FilterEnum>,
}

impl FullBacklinksQuery {
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

impl Query for FullBacklinksQuery {
    type Collector = TopDocsCollector;
    type TantivyQuery = Box<dyn tantivy::query::Query>;
    type IntermediateOutput = Vec<Edge>;
    type Output = Vec<Edge>;

    fn tantivy_query(&self, searcher: &Searcher) -> Self::TantivyQuery {
        let mut raw =
            Box::new(LinksQuery::new(self.node.id(), ToId)) as Box<dyn tantivy::query::Query>;

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

    fn retrieve(
        &self,
        searcher: &Searcher,
        fruit: <Self::Collector as super::collector::Collector>::Fruit,
    ) -> Result<Self::IntermediateOutput> {
        let docs: Vec<_> = fruit.into_iter().map(|(_, doc)| doc).collect();
        let edges = fetch_edges(searcher, docs)?;
        Ok(edges)
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
        edges.sort_by(|a, b| b.sort_score.total_cmp(&a.sort_score));
        edges
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct FullHostBacklinksQuery {
    node: Node,
    limit: EdgeLimit,
    filters: Vec<FilterEnum>,
    nodes_as_host: bool,
    skip_self_links: bool,
}

impl FullHostBacklinksQuery {
    pub fn new(node: Node) -> Self {
        Self {
            node,
            limit: EdgeLimit::Unlimited,
            filters: Vec::new(),
            nodes_as_host: true,
            skip_self_links: true,
        }
    }

    pub fn with_limit(self, limit: EdgeLimit) -> Self {
        Self { limit, ..self }
    }

    pub fn nodes_as_host(mut self, nodes_as_host: bool) -> Self {
        self.nodes_as_host = nodes_as_host;
        self
    }

    pub fn skip_self_links(mut self, skip_self_links: bool) -> Self {
        self.skip_self_links = skip_self_links;
        self
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

impl Query for FullHostBacklinksQuery {
    type Collector = TopDocsCollector<DefaultDocumentScorer, HostDeduplicator>;
    type TantivyQuery = Box<dyn tantivy::query::Query>;
    type IntermediateOutput = Vec<Edge>;
    type Output = Vec<Edge>;

    fn tantivy_query(&self, searcher: &Searcher) -> Self::TantivyQuery {
        let mut raw = Box::new(
            HostLinksQuery::new(
                self.node.clone().into_host().id(),
                ToHostId,
                FromHostId,
                searcher.warmed_column_fields().clone(),
            )
            .skip_self_links(self.skip_self_links),
        ) as Box<dyn tantivy::query::Query>;

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
            .with_host_field(FromHostId);

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

    fn retrieve(
        &self,
        searcher: &Searcher,
        fruit: <Self::Collector as super::collector::Collector>::Fruit,
    ) -> Result<Self::IntermediateOutput> {
        let docs: Vec<_> = fruit.into_iter().map(|(_, doc)| doc.address).collect();
        let edges = fetch_edges(searcher, docs)?;

        if !self.nodes_as_host {
            return Ok(edges);
        }

        Ok(edges
            .into_iter()
            .map(|e| Edge {
                from: e.from.into_host(),
                to: e.to.into_host(),
                ..e
            })
            .collect())
    }

    fn filter_fruit_shards(
        &self,
        shard_id: ShardId,
        fruit: <Self::Collector as super::Collector>::Fruit,
    ) -> <Self::Collector as super::Collector>::Fruit {
        fruit
            .into_iter()
            .filter(|(_, doc)| doc.address.shard_id == shard_id)
            .collect()
    }

    fn merge_results(results: Vec<Self::IntermediateOutput>) -> Self::Output {
        let mut edges: Vec<_> = results.into_iter().flatten().collect();
        edges.sort_by(|a, b| b.sort_score.total_cmp(&a.sort_score));
        edges
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct BacklinksWithLabelsQuery {
    node: NodeID,
    limit: EdgeLimit,
    filters: Vec<FilterEnum>,
}

impl BacklinksWithLabelsQuery {
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

impl Query for BacklinksWithLabelsQuery {
    type Collector = TopDocsCollector;
    type TantivyQuery = Box<dyn tantivy::query::Query>;
    type IntermediateOutput = Vec<(f64, SmallEdgeWithLabel)>;
    type Output = Vec<SmallEdgeWithLabel>;

    fn tantivy_query(&self, searcher: &Searcher) -> Self::TantivyQuery {
        let mut raw = Box::new(LinksQuery::new(self.node, ToId)) as Box<dyn tantivy::query::Query>;

        if let Some(filter) = self.filter_as_and().and_then(|f| f.inverted_index_filter()) {
            let filter = filter.query(searcher);
            let mut queries = vec![(Occur::Must, raw)];
            queries.extend(filter);
            raw = Box::new(BooleanQuery::new(queries));
        }

        raw
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

    fn retrieve(
        &self,
        searcher: &Searcher,
        fruit: <Self::Collector as super::Collector>::Fruit,
    ) -> Result<Self::IntermediateOutput> {
        let docs: Vec<_> = fruit.into_iter().map(|(_, doc)| doc).collect();
        let edges = fetch_edges(searcher, docs)?;
        Ok(edges
            .into_iter()
            .map(|e| {
                (
                    e.sort_score,
                    SmallEdgeWithLabel {
                        from: e.from.id(),
                        to: e.to.id(),
                        rel_flags: e.rel_flags,
                        label: e.label,
                    },
                )
            })
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
