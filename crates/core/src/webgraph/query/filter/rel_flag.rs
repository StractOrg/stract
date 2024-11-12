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

use tantivy::columnar::Column;

use crate::{
    webgraph::{schema, warmed_column_fields::SegmentColumnFields},
    webpage::RelFlags,
};

use super::{ColumnFieldFilter, Filter, FilterEnum, InvertedIndexFilter, SegmentColumnFieldFilter};

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct RelFlagsFilter(RelFlags);

impl From<RelFlags> for RelFlagsFilter {
    fn from(rel_flags: RelFlags) -> Self {
        RelFlagsFilter(rel_flags)
    }
}

impl Filter for RelFlagsFilter {
    fn column_field_filter(&self) -> Option<Box<dyn ColumnFieldFilter>> {
        Some(Box::new(RelFlagsColumnFieldFilter(self.0)))
    }

    fn inverted_index_filter(&self) -> Option<Box<dyn InvertedIndexFilter>> {
        None
    }
}

impl From<RelFlagsFilter> for FilterEnum {
    fn from(filter: RelFlagsFilter) -> Self {
        FilterEnum::RelFlags(filter)
    }
}

struct RelFlagsColumnFieldFilter(RelFlags);

impl ColumnFieldFilter for RelFlagsColumnFieldFilter {
    fn for_segment(
        &self,
        column_fields: &SegmentColumnFields,
    ) -> Box<dyn SegmentColumnFieldFilter> {
        Box::new(RelFlagsSegmentColumnFieldFilter {
            rel_flags: self.0,
            column: column_fields.u64(schema::RelFlags).unwrap(),
        })
    }
}

struct RelFlagsSegmentColumnFieldFilter {
    rel_flags: RelFlags,
    column: Column<u64>,
}

impl SegmentColumnFieldFilter for RelFlagsSegmentColumnFieldFilter {
    fn should_keep(&self, doc_id: tantivy::DocId) -> bool {
        let rel_flag = RelFlags::from(self.column.first(doc_id).unwrap());
        rel_flag.contains(self.rel_flags)
    }
}

#[cfg(test)]
mod tests {
    use file_store::temp::TempDir;

    use crate::{
        webgraph::{
            query::{FullForwardlinksQuery, NotFilter},
            Edge, Node, Webgraph,
        },
        webpage::RelFlags,
    };

    use super::*;

    pub fn test_edges() -> Vec<(Node, Node, RelFlags)> {
        vec![
            (
                Node::from("a.com"),
                Node::from("b.com/123"),
                RelFlags::default(),
            ),
            (
                Node::from("a.com"),
                Node::from("b.dk/123"),
                RelFlags::NOFOLLOW,
            ),
        ]
    }

    pub fn test_graph() -> (Webgraph, TempDir) {
        let temp_dir = crate::gen_temp_dir().unwrap();
        let mut graph = Webgraph::builder(&temp_dir, 0u64.into()).open().unwrap();

        for (from, to, rel_flags) in test_edges() {
            graph
                .insert(Edge {
                    from,
                    to,
                    rel_flags,
                    label: String::new(),
                    sort_score: 0.0,
                })
                .unwrap();
        }

        graph.commit().unwrap();

        (graph, temp_dir)
    }

    #[test]
    fn test_rel_flags_filter() {
        let (graph, _temp_dir) = test_graph();

        let node = Node::from("a.com");

        let res = graph
            .search(&FullForwardlinksQuery::new(node).filter(RelFlagsFilter(RelFlags::NOFOLLOW)))
            .unwrap();

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].to, Node::from("b.dk/123"));

        let node = Node::from("a.com");

        let res = graph
            .search(
                &FullForwardlinksQuery::new(node)
                    .filter(NotFilter::new(RelFlagsFilter(RelFlags::NOFOLLOW))),
            )
            .unwrap();

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].to, Node::from("b.com/123"));
    }
}
