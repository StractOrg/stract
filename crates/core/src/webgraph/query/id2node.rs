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

use super::collector::FirstDocCollector;
use super::Query;
use crate::webgraph::query::raw;
use crate::webgraph::schema::{ToHostId, ToId};
use crate::webgraph::{Edge, Node, NodeID};

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub enum Id2NodeQuery {
    Page(NodeID),
    Host(NodeID),
}

impl Query for Id2NodeQuery {
    type Collector = FirstDocCollector;
    type TantivyQuery = raw::Id2NodeQuery;
    type Output = Option<Node>;

    fn tantivy_query(&self) -> Self::TantivyQuery {
        match self {
            Self::Page(node) => raw::Id2NodeQuery::new(*node, ToId),
            Self::Host(node) => raw::Id2NodeQuery::new(*node, ToHostId),
        }
    }

    fn collector(&self) -> Self::Collector {
        FirstDocCollector
    }

    fn retrieve(
        &self,
        searcher: &tantivy::Searcher,
        fruit: <Self::Collector as super::collector::Collector>::Fruit,
    ) -> crate::Result<Self::Output> {
        Ok(fruit.and_then(|doc| {
            searcher.doc::<Edge>(doc).ok().map(|e| match self {
                Self::Page(_) => e.to,
                Self::Host(_) => e.to.into_host(),
            })
        }))
    }
}
