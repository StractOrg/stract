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
    webgraph::{
        document::Edge,
        schema::{FromId, ToId},
        EdgeLimit, Node, NodeID, SmallEdge,
    },
    Result,
};

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
    type Output = Vec<SmallEdge>;

    fn tantivy_query(&self) -> Self::TantivyQuery {
        match self.limit {
            EdgeLimit::Unlimited => Box::new(LinksQuery::new(self.node, FromId)),
            EdgeLimit::Limit(limit) | EdgeLimit::LimitAndOffset { limit, .. } => Box::new(
                ShortCircuitQuery::new(Box::new(LinksQuery::new(self.node, FromId)), limit as u64),
            ),
        }
    }

    fn collector(&self) -> Self::Collector {
        self.limit.into()
    }

    fn remote_collector(&self) -> Self::Collector {
        self.collector().enable_offset()
    }

    fn retrieve(
        &self,
        searcher: &tantivy::Searcher,
        fruit: <Self::Collector as super::collector::Collector>::Fruit,
    ) -> Result<Self::Output> {
        let fruit = fruit.into_iter().map(|(_, doc)| doc).collect();
        let nodes = fetch_small_edges(searcher, fruit, ToId)?;
        Ok(nodes
            .into_iter()
            .map(|(node, rel_flags)| SmallEdge {
                from: self.node,
                to: node,
                rel_flags,
            })
            .collect())
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
    type Output = Vec<SmallEdge>;

    fn tantivy_query(&self) -> Self::TantivyQuery {
        match self.limit {
            EdgeLimit::Unlimited => Box::new(HostLinksQuery::new(self.node, FromId)),
            EdgeLimit::Limit(limit) | EdgeLimit::LimitAndOffset { limit, .. } => {
                Box::new(ShortCircuitQuery::new(
                    Box::new(HostLinksQuery::new(self.node, FromId)),
                    limit as u64,
                ))
            }
        }
    }

    fn collector(&self) -> Self::Collector {
        self.limit.into()
    }

    fn remote_collector(&self) -> Self::Collector {
        self.collector().enable_offset()
    }

    fn retrieve(
        &self,
        searcher: &tantivy::Searcher,
        fruit: <Self::Collector as super::collector::Collector>::Fruit,
    ) -> Result<Self::Output> {
        let fruit = fruit.into_iter().map(|(_, doc)| doc).collect();
        let nodes = fetch_small_edges(searcher, fruit, ToId)?;
        Ok(nodes
            .into_iter()
            .map(|(node, rel_flags)| SmallEdge {
                from: self.node,
                to: node,
                rel_flags,
            })
            .collect())
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
    type Output = Vec<Edge>;

    fn tantivy_query(&self) -> Self::TantivyQuery {
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

    fn collector(&self) -> Self::Collector {
        self.limit.into()
    }

    fn remote_collector(&self) -> Self::Collector {
        self.collector().enable_offset()
    }

    fn retrieve(
        &self,
        searcher: &tantivy::Searcher,
        fruit: <Self::Collector as super::collector::Collector>::Fruit,
    ) -> Result<Self::Output> {
        let fruit = fruit.into_iter().map(|(_, doc)| doc).collect();
        let edges = fetch_edges(searcher, fruit)?;
        Ok(edges)
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
    type Output = Vec<Edge>;

    fn tantivy_query(&self) -> Self::TantivyQuery {
        match self.limit {
            EdgeLimit::Unlimited => Box::new(HostLinksQuery::new(
                self.node.clone().into_host().id(),
                FromId,
            )),
            EdgeLimit::Limit(limit) | EdgeLimit::LimitAndOffset { limit, .. } => {
                Box::new(ShortCircuitQuery::new(
                    Box::new(HostLinksQuery::new(
                        self.node.clone().into_host().id(),
                        FromId,
                    )),
                    limit as u64,
                ))
            }
        }
    }

    fn collector(&self) -> Self::Collector {
        self.limit.into()
    }

    fn remote_collector(&self) -> Self::Collector {
        self.collector().enable_offset()
    }

    fn retrieve(
        &self,
        searcher: &tantivy::Searcher,
        fruit: <Self::Collector as super::collector::Collector>::Fruit,
    ) -> Result<Self::Output> {
        let fruit = fruit.into_iter().map(|(_, doc)| doc).collect();
        let edges = fetch_edges(searcher, fruit)?;
        Ok(edges)
    }
}
