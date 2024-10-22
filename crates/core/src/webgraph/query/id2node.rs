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
use crate::ampc::dht::ShardId;
use crate::webgraph::query::raw;
use crate::webgraph::schema::{FromHostId, FromId, ToHostId, ToId};
use crate::webgraph::searcher::Searcher;
use crate::webgraph::{Edge, Node, NodeID};

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub enum Id2NodeQuery {
    Page(NodeID),
    Host(NodeID),
}

impl Query for Id2NodeQuery {
    type Collector = FirstDocCollector;
    type TantivyQuery = raw::Id2NodeQuery;
    type IntermediateOutput = Option<Node>;
    type Output = Option<Node>;

    fn tantivy_query(&self) -> Self::TantivyQuery {
        match self {
            Self::Page(node) => raw::Id2NodeQuery::new(*node, vec![ToId.into(), FromId.into()]),
            Self::Host(node) => {
                raw::Id2NodeQuery::new(*node, vec![ToHostId.into(), FromHostId.into()])
            }
        }
    }

    fn collector(&self, shard_id: ShardId) -> Self::Collector {
        FirstDocCollector::with_shard_id(shard_id)
    }

    fn retrieve(
        &self,
        searcher: &Searcher,
        fruit: <Self::Collector as super::collector::Collector>::Fruit,
    ) -> crate::Result<Self::Output> {
        Ok(fruit.and_then(|doc| {
            searcher
                .tantivy_searcher()
                .doc::<Edge>(doc.into())
                .ok()
                .and_then(|e| match self {
                    Self::Page(node) => {
                        if e.to.id() == *node {
                            Some(e.to)
                        } else if e.from.id() == *node {
                            Some(e.from)
                        } else {
                            None
                        }
                    }
                    Self::Host(node) => {
                        let to = e.to.into_host();
                        let from = e.from.into_host();

                        if to.id() == *node {
                            Some(to)
                        } else if from.id() == *node {
                            Some(from)
                        } else {
                            None
                        }
                    }
                })
        }))
    }

    fn remote_collector(&self) -> Self::Collector {
        FirstDocCollector::without_shard_id()
    }

    fn filter_fruit_shards(
        &self,
        shard_id: crate::ampc::dht::ShardId,
        fruit: <Self::Collector as super::Collector>::Fruit,
    ) -> <Self::Collector as super::Collector>::Fruit {
        match fruit {
            Some(doc) if doc.shard_id == shard_id => fruit,
            _ => None,
        }
    }

    fn merge_results(results: Vec<Self::IntermediateOutput>) -> Self::Output {
        results.into_iter().flatten().next()
    }
}
