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

mod and;
pub use and::AndFilter;

mod or;
pub use or::OrFilter;

mod not;
pub use not::NotFilter;

mod text;
pub use text::TextFilter;

use tantivy::{query::Occur, DocId};

use crate::webgraph::{searcher::Searcher, warmed_column_fields::WarmedColumnFields};

pub trait Filter:
    Send
    + Sync
    + bincode::Encode
    + bincode::Decode
    + std::fmt::Debug
    + Clone
    + Into<FilterEnum>
    + 'static
{
    fn column_field_filter(&self) -> Option<Box<dyn ColumnFieldFilter>>;
    fn inverted_index_filter(&self) -> Option<Box<dyn InvertedIndexFilter>>;
}

#[derive(Clone, Debug, bincode::Encode, bincode::Decode)]
pub enum FilterEnum {
    AndFilter(AndFilter),
    OrFilter(OrFilter),
    NotFilter(Box<NotFilter>),
    TextFilter(TextFilter),
}

impl Filter for FilterEnum {
    fn column_field_filter(&self) -> Option<Box<dyn ColumnFieldFilter>> {
        match self {
            FilterEnum::AndFilter(filter) => filter.column_field_filter(),
            FilterEnum::OrFilter(filter) => filter.column_field_filter(),
            FilterEnum::NotFilter(filter) => filter.column_field_filter(),
            FilterEnum::TextFilter(filter) => filter.column_field_filter(),
        }
    }

    fn inverted_index_filter(&self) -> Option<Box<dyn InvertedIndexFilter>> {
        match self {
            FilterEnum::AndFilter(filter) => filter.inverted_index_filter(),
            FilterEnum::OrFilter(filter) => filter.inverted_index_filter(),
            FilterEnum::NotFilter(filter) => filter.inverted_index_filter(),
            FilterEnum::TextFilter(filter) => filter.inverted_index_filter(),
        }
    }
}

pub trait ColumnFieldFilter: Send + Sync + 'static {
    fn for_segment(&self, column_fields: &WarmedColumnFields) -> Box<dyn SegmentColumnFieldFilter>;
}

pub trait SegmentColumnFieldFilter {
    fn should_skip(&self, doc_id: DocId) -> bool;
}

pub trait InvertedIndexFilter {
    fn query(&self, searcher: &Searcher) -> Vec<(Occur, Box<dyn tantivy::query::Query>)>;
}
