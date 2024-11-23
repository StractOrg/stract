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

use tantivy::query::Occur;

use crate::webgraph::{
    query::raw::PhraseOrTermQuery,
    schema::{Field, FieldEnum},
    searcher::Searcher,
};

use super::{Filter, FilterEnum};

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct TextFilter {
    text: String,
    field: FieldEnum,
}

impl TextFilter {
    pub fn new<F: Field>(text: String, field: F) -> Self {
        Self {
            text,
            field: field.into(),
        }
    }
}

impl From<TextFilter> for FilterEnum {
    fn from(filter: TextFilter) -> Self {
        FilterEnum::TextFilter(filter)
    }
}

impl Filter for TextFilter {
    fn column_field_filter(&self) -> Option<Box<dyn super::ColumnFieldFilter>> {
        None
    }

    fn inverted_index_filter(&self) -> Option<Box<dyn super::InvertedIndexFilter>> {
        Some(Box::new(TextInvertedIndexFilter {
            text: self.text.clone(),
            field: self.field,
        }))
    }
}

struct TextInvertedIndexFilter {
    text: String,
    field: FieldEnum,
}

impl super::InvertedIndexFilter for TextInvertedIndexFilter {
    fn query(&self, _: &Searcher) -> Vec<(Occur, Box<dyn tantivy::query::Query>)> {
        vec![(
            Occur::Should,
            Box::new(PhraseOrTermQuery::new(self.text.clone(), self.field)),
        )]
    }
}

#[cfg(test)]
mod tests {
    use file_store::temp::TempDir;

    use crate::webgraph::{
        query::{BacklinksQuery, ForwardlinksQuery, FullBacklinksQuery, FullForwardlinksQuery},
        schema::{FromUrl, ToUrl},
        Edge, Node, Webgraph,
    };

    use super::*;

    pub fn test_edges() -> Vec<(Node, Node)> {
        vec![
            (Node::from("a.com"), Node::from("b.com")),
            (Node::from("a.com"), Node::from("b.dk")),
            (Node::from("b.com"), Node::from("b.dk")),
            (Node::from("c.dk"), Node::from("b.dk")),
            (Node::from("c.com"), Node::from("a.com")),
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
    fn test_text_filter() {
        let (graph, _temp_dir) = test_graph();
        let node = Node::from("b.dk");

        let filter = TextFilter::new(".dk".to_string(), FromUrl);

        let res = graph
            .search(&BacklinksQuery::new(node.id()).filter(filter))
            .unwrap();

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].from, Node::from("c.dk").id());

        let filter = TextFilter::new(".dk".to_string(), FromUrl);

        let res = graph
            .search(&FullBacklinksQuery::new(node.clone()).filter(filter))
            .unwrap();

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].from, Node::from("c.dk"));

        let filter = TextFilter::new(".dk".to_string(), ToUrl);

        let node = Node::from("a.com");

        let res = graph
            .search(&ForwardlinksQuery::new(node.id()).filter(filter))
            .unwrap();

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].to, Node::from("b.dk").id());

        let filter = TextFilter::new(".dk".to_string(), ToUrl);

        let res = graph
            .search(&FullForwardlinksQuery::new(node.clone()).filter(filter))
            .unwrap();

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].to, Node::from("b.dk"));
    }
}
