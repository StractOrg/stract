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

use tantivy::DocAddress;

use super::{
    collector::TopDocsCollector,
    raw::{host_links::HostLinksQuery, links::LinksQuery},
    Query,
};
use crate::{
    webgraph::{
        schema::{Field, FromId, ToId},
        EdgeLimit, NodeID,
    },
    Result,
};

pub fn fetch_nodes<F: Field>(
    searcher: &tantivy::Searcher,
    mut doc_ids: Vec<DocAddress>,
    field: F,
) -> Result<Vec<NodeID>> {
    doc_ids.sort_unstable_by_key(|doc| doc.segment_ord);

    Ok(doc_ids
        .iter()
        .filter_map(|doc| {
            let segment_reader = searcher.segment_reader(doc.segment_ord);
            let from_id = segment_reader.column_fields().u64(field.name()).unwrap();

            from_id.first(doc.doc_id)
        })
        .map(|from_id| NodeID::from(from_id))
        .collect())
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct BacklinksQuery {
    node: NodeID,
    limit: EdgeLimit,
}

impl Query for BacklinksQuery {
    type Collector = TopDocsCollector;
    type TantivyQuery = LinksQuery;
    type Output = Vec<NodeID>;

    fn tantivy_query(&self) -> Self::TantivyQuery {
        LinksQuery::new(self.node, ToId)
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
        let nodes = fetch_nodes(searcher, fruit, FromId)?;
        Ok(nodes)
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct HostBacklinksQuery {
    node: NodeID,
    limit: EdgeLimit,
}

impl Query for HostBacklinksQuery {
    type Collector = TopDocsCollector;
    type TantivyQuery = HostLinksQuery;
    type Output = Vec<NodeID>;

    fn tantivy_query(&self) -> Self::TantivyQuery {
        HostLinksQuery::new(self.node, ToId)
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
        let nodes = fetch_nodes(searcher, fruit, FromId)?;
        Ok(nodes)
    }
}
