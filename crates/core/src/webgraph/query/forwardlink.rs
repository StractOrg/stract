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
    backlink::fetch_nodes,
    collector::TopDocsCollector,
    raw::{host_links::HostLinksQuery, links::LinksQuery},
    Query,
};
use crate::{
    webgraph::{
        schema::{FromId, ToId},
        EdgeLimit, NodeID,
    },
    Result,
};

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct ForwardlinksQuery {
    node: NodeID,
    limit: EdgeLimit,
}

impl Query for ForwardlinksQuery {
    type Collector = TopDocsCollector;
    type TantivyQuery = LinksQuery;
    type Output = Vec<NodeID>;

    fn tantivy_query(&self) -> Self::TantivyQuery {
        LinksQuery::new(self.node, FromId)
    }

    fn collector(&self) -> Self::Collector {
        let mut collector = TopDocsCollector::new().disable_offset();

        match self.limit {
            EdgeLimit::Unlimited => {}
            EdgeLimit::Limit(limit) => collector = collector.with_limit(limit),
            EdgeLimit::LimitAndOffset { limit, offset } => {
                collector = collector.with_limit(limit);
                collector = collector.with_offset(offset);
            }
        }

        collector
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
        let nodes = fetch_nodes(searcher, fruit, ToId)?;
        Ok(nodes)
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct HostForwardlinksQuery {
    node: NodeID,
    limit: EdgeLimit,
}

impl Query for HostForwardlinksQuery {
    type Collector = TopDocsCollector;
    type TantivyQuery = HostLinksQuery;
    type Output = Vec<NodeID>;

    fn tantivy_query(&self) -> Self::TantivyQuery {
        HostLinksQuery::new(self.node, FromId)
    }

    fn collector(&self) -> Self::Collector {
        let mut collector = TopDocsCollector::new().disable_offset();

        match self.limit {
            EdgeLimit::Unlimited => {}
            EdgeLimit::Limit(limit) => collector = collector.with_limit(limit),
            EdgeLimit::LimitAndOffset { limit, offset } => {
                collector = collector.with_limit(limit);
                collector = collector.with_offset(offset);
            }
        }

        collector
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
        let nodes = fetch_nodes(searcher, fruit, ToId)?;
        Ok(nodes)
    }
}
