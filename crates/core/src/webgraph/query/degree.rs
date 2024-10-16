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

use crate::webgraph::{
    schema::{Field, ToId},
    NodeID,
};

use super::{
    collector::{FastCountCollector, FastCountValue},
    raw, Query,
};

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct InDegreeQuery {
    node: NodeID,
}

impl Query for InDegreeQuery {
    type Collector = FastCountCollector;
    type TantivyQuery = raw::DummyQuery;
    type Output = u64;

    fn tantivy_query(&self) -> Self::TantivyQuery {
        raw::DummyQuery
    }

    fn collector(&self) -> Self::Collector {
        FastCountCollector::new(
            ToId.name().to_string(),
            FastCountValue::U64(self.node.as_u64()),
        )
    }

    fn retrieve(
        &self,
        _: &tantivy::Searcher,
        fruit: <Self::Collector as super::collector::Collector>::Fruit,
    ) -> crate::Result<Self::Output> {
        Ok(fruit)
    }
}
