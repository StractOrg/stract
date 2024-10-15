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

use crate::Result;
use collector::Collector;
use tantivy::Searcher;

pub mod backlinks;
pub mod collector;
mod degree;
mod raw;

pub trait Query: Send + Sync + bincode::Encode + bincode::Decode + Clone {
    type Collector: Collector;
    type TantivyQuery: tantivy::query::Query;
    type Output;

    fn tantivy_query(&self) -> Self::TantivyQuery;
    fn collector(&self) -> Self::Collector;

    fn retrieve(
        &self,
        searcher: &Searcher,
        fruit: &<Self::Collector as Collector>::Fruit,
    ) -> Result<Self::Output>;
}
