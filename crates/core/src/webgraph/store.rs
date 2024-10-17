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

use std::path::Path;

use super::{
    document::SmallEdge,
    query::collector::{Collector, TantivyCollector},
    schema::{self, Field, FromId, ToId},
    Edge,
};
use crate::{webpage::html::links::RelFlags, Result};
use tantivy::{columnar::Column, DocId, SegmentReader};

use super::{query::Query, NodeID};

pub struct EdgeStore {
    index: tantivy::Index,
}

impl EdgeStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Self { index: todo!() })
    }

    pub fn optimize_read(&mut self) -> Result<()> {
        todo!()
    }

    pub fn insert(&mut self, edge: Edge) -> Result<()> {
        todo!()
    }

    pub fn commit(&mut self) -> Result<()> {
        todo!()
    }

    pub fn merge(&self, other: Self) -> Result<()> {
        todo!()
    }

    pub fn search<Q: Query>(&self, query: &Q) -> Result<<Q::Collector as Collector>::Fruit> {
        let searcher = self.index.reader().unwrap().searcher();
        let res = searcher.search(
            &query.tantivy_query(),
            &TantivyCollector::from(&query.collector()),
        )?;

        Ok(res)
    }

    pub fn retrieve<Q: Query>(
        &self,
        query: &Q,
        fruit: <Q::Collector as Collector>::Fruit,
    ) -> Result<Q::Output> {
        let searcher = self.index.reader().unwrap().searcher();
        query.retrieve(&searcher, fruit)
    }

    pub fn iter_small(&self) -> impl Iterator<Item = SmallEdge> + '_ {
        let reader = self.index.reader().unwrap();
        let searcher = reader.searcher();
        let segment_readers: Vec<_> = searcher.segment_readers().iter().cloned().collect();

        segment_readers
            .into_iter()
            .flat_map(|segment| SmallSegmentEdgesIter::new(&segment))
    }
}

pub struct SmallSegmentEdgesIter {
    from_id: Column,
    to_id: Column,
    rel_flags: Column,

    docs: Box<dyn Iterator<Item = DocId>>,
}

impl SmallSegmentEdgesIter {
    fn new(segment: &SegmentReader) -> Self {
        let columns = segment.column_fields();

        Self {
            from_id: columns.u64(FromId.name()).unwrap(),
            to_id: columns.u64(ToId.name()).unwrap(),
            rel_flags: columns.u64(schema::RelFlags.name()).unwrap(),
            docs: Box::new(0..segment.max_doc()),
        }
    }
}

impl Iterator for SmallSegmentEdgesIter {
    type Item = SmallEdge;

    fn next(&mut self) -> Option<Self::Item> {
        let doc = self.docs.next()?;

        let from = self.from_id.first(doc)?;
        let to = self.to_id.first(doc)?;
        let rel_flags = self.rel_flags.first(doc)?;

        Some(SmallEdge {
            from: NodeID::from(from),
            to: NodeID::from(to),
            rel_flags: RelFlags::from(rel_flags),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::webgraph::{query::backlink::HostBacklinksQuery, Edge, Node};

    use super::*;

    #[test]
    fn test_insert() {
        let temp_dir = crate::gen_temp_dir().unwrap();
        let mut store = EdgeStore::open(&temp_dir).unwrap();

        let e = Edge {
            from: Node::from("https://www.first.com").into_host(),
            to: Node::from("https://www.second.com").into_host(),
            label: "test".to_string(),
            rel_flags: RelFlags::default(),
            combined_centrality: 0.0,
        };
        let from_node_id = e.from.id();
        let to_node_id = e.to.id();

        store.insert(e.clone()).unwrap();
        store.commit().unwrap();

        let query = HostBacklinksQuery::new(from_node_id);
        let res = store.search(&query).unwrap();
        let edges = store.retrieve(&query, res).unwrap();

        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].to, to_node_id);

        let query = HostBacklinksQuery::new(to_node_id);
        let res = store.search(&query).unwrap();
        let edges = store.retrieve(&query, res).unwrap();

        assert_eq!(edges.len(), 0);

        let edges = store.iter_small().collect::<Vec<_>>();

        assert_eq!(edges.len(), 1);
    }

    #[test]
    fn test_edge_ordering() {
        let temp_dir = crate::gen_temp_dir().unwrap();

        let a = Node::from("https://www.first.com").into_host();
        let b = Node::from("https://www.second.com").into_host();
        let c = Node::from("https://www.third.com").into_host();
        let d = Node::from("https://www.fourth.com").into_host();

        let a_centrality = 1.0;
        let b_centrality = 2.0;
        let c_centrality = 3.0;
        let d_centrality = 4.0;
        let mut store = EdgeStore::open(&temp_dir.as_ref().join("test-segment")).unwrap();

        let e1 = Edge {
            from: b.clone(),
            to: a.clone(),
            label: "test".to_string(),
            rel_flags: RelFlags::default(),
            combined_centrality: a_centrality + b_centrality,
        };

        let e2 = Edge {
            from: c.clone(),
            to: a.clone(),
            label: "2".to_string(),
            rel_flags: RelFlags::default(),
            combined_centrality: a_centrality + c_centrality,
        };

        let e3 = Edge {
            from: d.clone(),
            to: a.clone(),
            label: "3".to_string(),
            rel_flags: RelFlags::default(),
            combined_centrality: a_centrality + d_centrality,
        };

        store.insert(e1.clone()).unwrap();
        store.insert(e2.clone()).unwrap();
        store.insert(e3.clone()).unwrap();

        let query = HostBacklinksQuery::new(a.id());
        let res = store.search(&query).unwrap();
        let edges = store.retrieve(&query, res).unwrap();

        assert_eq!(edges.len(), 3);

        assert_eq!(edges[0].from, e2.from.id());
        assert_eq!(edges[1].from, e3.from.id());
        assert_eq!(edges[2].from, e1.from.id());
    }
}
