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

use crate::webgraph::{searcher::Searcher, warmed_column_fields::SegmentColumnFields};

use super::{Filter, FilterEnum};
use tantivy::{query::Occur, DocId};

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct NotFilter {
    filter: FilterEnum,
}

impl From<NotFilter> for FilterEnum {
    fn from(filter: NotFilter) -> Self {
        FilterEnum::NotFilter(Box::new(filter))
    }
}

impl NotFilter {
    pub fn new<F: Filter>(filter: F) -> Self {
        Self {
            filter: filter.into(),
        }
    }
}

impl Filter for NotFilter {
    fn column_field_filter(&self) -> Option<Box<dyn super::ColumnFieldFilter>> {
        let filter = self.filter.column_field_filter()?;
        Some(Box::new(NotColumnFieldFilter { filter }))
    }

    fn inverted_index_filter(&self) -> Option<Box<dyn super::InvertedIndexFilter>> {
        let filter = self.filter.inverted_index_filter()?;
        Some(Box::new(NotInvertedIndexFilter { filter }))
    }
}

struct NotColumnFieldFilter {
    filter: Box<dyn super::ColumnFieldFilter>,
}

impl super::ColumnFieldFilter for NotColumnFieldFilter {
    fn for_segment(
        &self,
        column_fields: &SegmentColumnFields,
    ) -> Box<dyn super::SegmentColumnFieldFilter> {
        let filter = self.filter.for_segment(column_fields);
        Box::new(NotSegmentColumnFieldFilter { filter })
    }
}

struct NotSegmentColumnFieldFilter {
    filter: Box<dyn super::SegmentColumnFieldFilter>,
}

impl super::SegmentColumnFieldFilter for NotSegmentColumnFieldFilter {
    fn should_keep(&self, doc_id: DocId) -> bool {
        !self.filter.should_keep(doc_id)
    }
}

struct NotInvertedIndexFilter {
    filter: Box<dyn super::InvertedIndexFilter>,
}

impl super::InvertedIndexFilter for NotInvertedIndexFilter {
    fn query(&self, searcher: &Searcher) -> Vec<(Occur, Box<dyn tantivy::query::Query>)> {
        let mut queries = Vec::new();
        for (occur, query) in self.filter.query(searcher) {
            queries.push((Occur::compose(Occur::MustNot, occur), query));
        }

        queries
    }
}

#[cfg(test)]
mod tests {
    use file_store::temp::TempDir;

    use crate::webgraph::{
        query::{FullForwardlinksQuery, OrFilter, TextFilter},
        schema::ToUrl,
        Edge, Node, Webgraph,
    };

    use super::*;

    pub fn test_edges() -> Vec<(Node, Node)> {
        vec![
            (Node::from("a.com"), Node::from("b.com/123")),
            (Node::from("a.com"), Node::from("b.dk/123")),
            (Node::from("a.com"), Node::from("b.se/123")),
            (Node::from("a.com"), Node::from("b.com/321")),
            (Node::from("a.com"), Node::from("c.com")),
        ]
    }

    pub fn test_graph() -> (Webgraph, TempDir) {
        let temp_dir = crate::gen_temp_dir().unwrap();
        let mut graph = Webgraph::builder(&temp_dir, 0u64.into()).open().unwrap();

        for (from, to) in test_edges() {
            graph.insert(Edge::new_test(from, to)).unwrap();
        }

        graph.commit().unwrap();

        (graph, temp_dir)
    }

    #[test]
    fn test_not_filter() {
        let (graph, _temp_dir) = test_graph();
        let node = Node::from("a.com");

        let res = graph
            .search(
                &FullForwardlinksQuery::new(node)
                    .filter(NotFilter::new(TextFilter::new(".dk".to_string(), ToUrl))),
            )
            .unwrap();

        assert_eq!(res.len(), 4);
        assert!(res.iter().all(|r| !r.to.as_str().contains(".dk")));
    }

    #[test]
    fn test_not_inside_or() {
        let (graph, _temp_dir) = test_graph();
        let node = Node::from("a.com");

        let res = graph
            .search(
                &FullForwardlinksQuery::new(node).filter(
                    OrFilter::new()
                        .or(NotFilter::new(TextFilter::new(".dk".to_string(), ToUrl)))
                        .or(TextFilter::new(".com".to_string(), ToUrl)),
                ),
            )
            .unwrap();

        assert_eq!(res.len(), 3);
        assert!(res.iter().all(|r| r.to.as_str().contains(".com")));
    }
}
