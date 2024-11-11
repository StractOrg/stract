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

use tantivy::{
    query::{BooleanQuery, Occur},
    DocId,
};

use crate::webgraph::{searcher::Searcher, warmed_column_fields::WarmedColumnFields};

use super::{Filter, FilterEnum};

#[derive(Default, Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct OrFilter {
    filters: Vec<FilterEnum>,
}

impl OrFilter {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn or<F: Filter>(mut self, filter: F) -> Self {
        self.filters.push(filter.into());
        self
    }
}

impl From<OrFilter> for FilterEnum {
    fn from(filter: OrFilter) -> Self {
        Self::OrFilter(filter)
    }
}

impl Filter for OrFilter {
    fn column_field_filter(&self) -> Option<Box<dyn super::ColumnFieldFilter>> {
        let mut filters = Vec::with_capacity(self.filters.len());
        for filter in self.filters.iter() {
            if let Some(column_field_filter) = filter.column_field_filter() {
                filters.push(column_field_filter);
            }
        }

        if !filters.is_empty() {
            Some(Box::new(OrColumnFieldFilter { filters }))
        } else {
            None
        }
    }

    fn inverted_index_filter(&self) -> Option<Box<dyn super::InvertedIndexFilter>> {
        let mut filters = Vec::with_capacity(self.filters.len());
        for filter in self.filters.iter() {
            if let Some(inverted_index_filter) = filter.inverted_index_filter() {
                filters.push(inverted_index_filter);
            }
        }

        if !filters.is_empty() {
            Some(Box::new(OrInvertedIndexFilter { filters }))
        } else {
            None
        }
    }
}

pub struct OrColumnFieldFilter {
    filters: Vec<Box<dyn super::ColumnFieldFilter>>,
}

impl super::ColumnFieldFilter for OrColumnFieldFilter {
    fn for_segment(
        &self,
        column_fields: &WarmedColumnFields,
    ) -> Box<dyn super::SegmentColumnFieldFilter> {
        let mut filters = Vec::with_capacity(self.filters.len());
        for filter in self.filters.iter() {
            filters.push(filter.for_segment(column_fields));
        }

        Box::new(OrSegmentColumnFieldFilter { filters })
    }
}

pub struct OrSegmentColumnFieldFilter {
    filters: Vec<Box<dyn super::SegmentColumnFieldFilter>>,
}

impl super::SegmentColumnFieldFilter for OrSegmentColumnFieldFilter {
    fn should_skip(&self, doc_id: DocId) -> bool {
        for filter in self.filters.iter() {
            if !filter.should_skip(doc_id) {
                return false;
            }
        }

        true
    }
}

pub struct OrInvertedIndexFilter {
    filters: Vec<Box<dyn super::InvertedIndexFilter>>,
}

impl super::InvertedIndexFilter for OrInvertedIndexFilter {
    fn query(&self, searcher: &Searcher) -> Vec<(Occur, Box<dyn tantivy::query::Query>)> {
        let mut queries = Vec::with_capacity(self.filters.len());
        for filter in self.filters.iter() {
            for (occur, query) in filter.query(searcher) {
                queries.push((Occur::compose(Occur::Should, occur), query));
            }
        }

        vec![(Occur::Should, Box::new(BooleanQuery::new(queries)))]
    }
}

#[cfg(test)]
mod tests {
    use file_store::temp::TempDir;

    use crate::{
        webgraph::{
            query::{FullForwardlinksQuery, TextFilter},
            schema::ToUrl,
            Edge, Node, Webgraph,
        },
        webpage::RelFlags,
    };

    use super::*;

    pub fn test_edges() -> Vec<(Node, Node, String)> {
        vec![
            (Node::from("a.com"), Node::from("b.com/123"), String::new()),
            (Node::from("a.com"), Node::from("b.dk/123"), String::new()),
            (Node::from("a.com"), Node::from("b.se/123"), String::new()),
            (Node::from("a.com"), Node::from("b.com/321"), String::new()),
            (Node::from("a.com"), Node::from("c.com"), String::new()),
        ]
    }

    pub fn test_graph() -> (Webgraph, TempDir) {
        let temp_dir = crate::gen_temp_dir().unwrap();
        let mut graph = Webgraph::builder(&temp_dir, 0u64.into()).open().unwrap();

        for (from, to, label) in test_edges() {
            graph
                .insert(Edge {
                    from,
                    to,
                    rel_flags: RelFlags::default(),
                    label,
                    sort_score: 0.0,
                })
                .unwrap();
        }

        graph.commit().unwrap();

        (graph, temp_dir)
    }

    #[test]
    fn test_or_filter() {
        let (graph, _temp_dir) = test_graph();

        let node = Node::from("a.com");

        let res = graph
            .search(
                &FullForwardlinksQuery::new(node).filter(
                    OrFilter::new()
                        .or(TextFilter::new(".com".to_string(), ToUrl))
                        .or(TextFilter::new(".dk".to_string(), ToUrl)),
                ),
            )
            .unwrap();

        assert_eq!(res.len(), 4);
        assert!(res.iter().all(|r| !r.to.as_str().contains(".se")));
    }
}
