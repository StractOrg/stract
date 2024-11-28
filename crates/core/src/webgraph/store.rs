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

use std::{
    collections::HashSet,
    fs,
    ops::Range,
    path::{Path, PathBuf},
};

use super::{
    edge::SmallEdge,
    query::collector::{Collector, TantivyCollector},
    schema::{self, create_schema, Field, FromHostId, FromId, ToHostId, ToId},
    searcher::Searcher,
    tokenizer::{Tokenizer, TokenizerEnum},
    warmed_column_fields::WarmedColumnFields,
    Edge,
};
use crate::{ampc::dht::ShardId, webpage::html::links::RelFlags, Result};
use itertools::Itertools;
use rustc_hash::FxHashSet;
use tantivy::{
    columnar::Column, directory::MmapDirectory, indexer::NoMergePolicy,
    tokenizer::TokenizerManager, DocId, SegmentReader,
};

use super::{query::Query, NodeID};

fn register_tokenizers(manager: &TokenizerManager) {
    for tokenizer in TokenizerEnum::iter() {
        manager.register(tokenizer.name(), tokenizer.into_tantivy());
    }
}

pub struct EdgeStore {
    index: tantivy::Index,
    writer: Option<tantivy::IndexWriter<Edge>>,
    writer_dedup: FxHashSet<(NodeID, NodeID)>,
    reader: tantivy::IndexReader,
    shard_id: ShardId,
    path: PathBuf,
    warmed_column_fields: WarmedColumnFields,
}

impl EdgeStore {
    pub fn open<P: AsRef<Path>>(path: P, shard_id: ShardId) -> Result<Self> {
        if !path.as_ref().exists() {
            fs::create_dir_all(&path)?;
        }

        let index: tantivy::Index = tantivy::IndexBuilder::new()
            .schema(create_schema())
            .settings(tantivy::IndexSettings {
                sort_by_field: Some(tantivy::IndexSortByField {
                    field: schema::SortScore.name().to_string(),
                    order: tantivy::Order::Asc,
                }),
                ..Default::default()
            })
            .open_or_create(MmapDirectory::open(&path)?)?;

        register_tokenizers(index.tokenizers());

        let warmed_column_fields = WarmedColumnFields::new(&index.reader()?.searcher())?;

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            writer: None,
            reader: index.reader()?,
            index,
            shard_id,
            warmed_column_fields,
            writer_dedup: FxHashSet::default(),
        })
    }

    fn prepare_writer(&mut self) -> Result<()> {
        if self.writer.is_some() {
            return Ok(());
        }

        let writer = self.index.writer_with_num_threads(1, 1_000_000_000)?;

        let merge_policy = NoMergePolicy;
        writer.set_merge_policy(Box::new(merge_policy));

        self.writer = Some(writer);

        Ok(())
    }

    pub fn optimize_read(&mut self) -> Result<()> {
        self.prepare_writer()?;
        let base_path = Path::new(&self.path);
        let mut segments: Vec<_> = self.index.load_metas()?.segments.into_iter().collect();
        segments.sort_by_key(|a| a.max_doc());

        let mut num_docs: tantivy::DocId = 0;
        let mut segments_to_merge = Vec::new();

        for segment in segments {
            if num_docs.saturating_add(segment.max_doc()) >= tantivy::TERMINATED {
                let segments_to_merge = std::mem::take(&mut segments_to_merge);
                num_docs = 0;

                if segments_to_merge.len() > 1 {
                    tantivy::merge_segments(
                        self.writer
                            .as_mut()
                            .expect("writer should have been prepared"),
                        segments_to_merge,
                        base_path,
                        1,
                    )?;
                }
            }

            num_docs = num_docs.saturating_add(segment.max_doc());
            segments_to_merge.push(segment);
        }

        if !segments_to_merge.is_empty() {
            tantivy::merge_segments(
                self.writer
                    .as_mut()
                    .expect("writer should have been prepared"),
                segments_to_merge,
                base_path,
                1,
            )?;
        }

        self.commit()?;

        Ok(())
    }

    pub fn insert(&mut self, edge: Edge) -> Result<()> {
        self.prepare_writer()?;

        if !self.writer_dedup.insert((edge.from.id(), edge.to.id())) {
            return Ok(());
        }

        self.writer
            .as_mut()
            .expect("writer should have been prepared")
            .add_document(edge)?;
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        self.prepare_writer()?;
        self.writer
            .as_mut()
            .expect("writer should have been prepared")
            .commit()?;
        self.writer_dedup.clear();
        self.reader.reload()?;
        self.warmed_column_fields = WarmedColumnFields::new(&self.reader.searcher())?;
        Ok(())
    }

    pub fn merge(&mut self, mut other: Self) -> Result<()> {
        self.prepare_writer()?;
        other.prepare_writer()?;

        other.commit()?;
        self.commit()?;

        let other_meta = other.index.load_metas()?;

        let mut meta = self.index.load_metas()?;

        let other_path = Path::new(&other.path);
        let other_writer = other.writer.take().unwrap();
        other_writer.wait_merging_threads().unwrap();

        let self_path = Path::new(&self.path);
        let self_writer = self.writer.take().unwrap();
        self_writer.wait_merging_threads().unwrap();

        let ids: HashSet<_> = meta.segments.iter().map(|segment| segment.id()).collect();

        for segment in other_meta.segments {
            if ids.contains(&segment.id()) {
                continue;
            }

            // TODO: handle case where current index has segment with same name
            for file in segment.list_files() {
                let p = other_path.join(&file);
                if p.exists() {
                    fs::rename(p, self_path.join(&file)).unwrap();
                }
            }
            meta.segments.push(segment);
        }

        meta.segments
            .sort_by_key(|a| std::cmp::Reverse(a.max_doc()));

        fs::remove_dir_all(other_path).ok();

        let self_path = Path::new(&self.path);

        std::fs::write(
            self_path.join("meta.json"),
            serde_json::to_string_pretty(&meta).unwrap(),
        )
        .unwrap();

        self.re_open()?;

        Ok(())
    }

    fn re_open(&mut self) -> Result<()> {
        *self = Self::open(self.path.clone(), self.shard_id)?;
        Ok(())
    }

    fn searcher(&self) -> Searcher {
        Searcher::new(
            self.reader.searcher(),
            self.warmed_column_fields.clone(),
            self.shard_id,
        )
    }

    pub fn search_initial<Q: Query>(
        &self,
        query: &Q,
    ) -> Result<<Q::Collector as Collector>::Fruit> {
        let searcher = self.searcher();

        let res = searcher.tantivy_searcher().search(
            &query.tantivy_query(&searcher),
            &TantivyCollector::from(&query.collector(&searcher)),
        )?;

        Ok(res)
    }

    pub fn retrieve<Q: Query>(
        &self,
        query: &Q,
        fruit: <Q::Collector as Collector>::Fruit,
    ) -> Result<Q::IntermediateOutput> {
        query.retrieve(&self.searcher(), fruit)
    }

    pub fn search<Q>(&self, query: &Q) -> Result<Q::Output>
    where
        Q: Query,
        <<Q::Collector as Collector>::Child as tantivy::collector::SegmentCollector>::Fruit:
            From<<Q::Collector as Collector>::Fruit>,
    {
        let fruit = self.search_initial(query)?;
        let fruit = query.remote_collector().merge_fruits(vec![fruit.into()])?;
        let res = self.retrieve(query, fruit)?;
        Ok(Q::merge_results(vec![res]))
    }

    pub fn iter_pages_small(&self) -> impl Iterator<Item = SmallEdge> + '_ {
        let searcher = self.reader.searcher();
        let segment_readers: Vec<_> = searcher.segment_readers().to_vec();
        let warm_column_fields = self.warmed_column_fields.clone();

        segment_readers.into_iter().flat_map(move |segment| {
            SmallSegmentEdgesIter::new(
                &segment,
                &warm_column_fields,
                FromId,
                ToId,
                0..segment.max_doc(),
            )
        })
    }

    pub fn iter_hosts_small(&self) -> impl Iterator<Item = SmallEdge> + '_ {
        let searcher = self.reader.searcher();
        let segment_readers: Vec<_> = searcher.segment_readers().to_vec();
        let warm_column_fields = self.warmed_column_fields.clone();

        segment_readers
            .into_iter()
            .flat_map(move |segment| {
                SmallSegmentEdgesIter::new(
                    &segment,
                    &warm_column_fields,
                    FromHostId,
                    ToHostId,
                    0..segment.max_doc(),
                )
            })
            .unique_by(|e| (e.from, e.to))
    }

    pub fn iter_page_node_ids(&self, offset: u32, limit: u32) -> impl Iterator<Item = NodeID> + '_ {
        let searcher = self.reader.searcher();
        let segment_readers: Vec<_> = searcher.segment_readers().to_vec();
        let warm_column_fields = self.warmed_column_fields.clone();

        let range = offset..limit;

        segment_readers
            .into_iter()
            .flat_map(move |segment| {
                SmallSegmentEdgesIter::new(
                    &segment,
                    &warm_column_fields,
                    FromId,
                    ToId,
                    range.clone(),
                )
            })
            .flat_map(|e| [e.from, e.to])
            .unique()
    }

    pub fn iter_host_node_ids(&self, offset: u32, limit: u32) -> impl Iterator<Item = NodeID> + '_ {
        let searcher = self.reader.searcher();
        let segment_readers: Vec<_> = searcher.segment_readers().to_vec();
        let warm_column_fields = self.warmed_column_fields.clone();
        let range = offset..limit;

        segment_readers
            .into_iter()
            .flat_map(move |segment| {
                SmallSegmentEdgesIter::new(
                    &segment,
                    &warm_column_fields,
                    FromHostId,
                    ToHostId,
                    range.clone(),
                )
            })
            .flat_map(|e| [e.from, e.to])
            .unique()
    }
}

pub struct SmallSegmentEdgesIter {
    from_id: Column<u128>,
    to_id: Column<u128>,
    rel_flags: Column<u64>,
    doc_range: Range<DocId>,
    current_doc: DocId,
}

impl SmallSegmentEdgesIter {
    fn new<F1, F2>(
        segment: &SegmentReader,
        warm_column_fields: &WarmedColumnFields,
        from_id: F1,
        to_id: F2,
        mut doc_range: Range<DocId>,
    ) -> Self
    where
        F1: Field,
        F2: Field,
    {
        let columns = warm_column_fields.segment(&segment.segment_id());

        if doc_range.end > segment.max_doc() {
            doc_range.end = segment.max_doc();
        }

        Self {
            from_id: columns.u128(from_id).unwrap(),
            to_id: columns.u128(to_id).unwrap(),
            rel_flags: columns.u64(schema::RelFlags).unwrap(),
            current_doc: doc_range.start,
            doc_range,
        }
    }
}

impl Iterator for SmallSegmentEdgesIter {
    type Item = SmallEdge;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_doc >= self.doc_range.end {
            return None;
        }

        let doc = self.current_doc;
        self.current_doc += 1;

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
        let mut store = EdgeStore::open(&temp_dir, ShardId::new(0)).unwrap();

        let e = Edge::new_test(
            Node::from("https://www.first.com").into_host(),
            Node::from("https://www.second.com").into_host(),
        );
        let from_node_id = e.from.id();
        let to_node_id = e.to.id();

        store.insert(e.clone()).unwrap();
        store.commit().unwrap();

        let query = HostBacklinksQuery::new(to_node_id);
        let edges = store.search(&query).unwrap();

        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].from, from_node_id);
        assert_eq!(edges[0].to, to_node_id);

        let query = HostBacklinksQuery::new(from_node_id);
        let edges = store.search(&query).unwrap();

        assert_eq!(edges.len(), 0);

        let edges = store.iter_pages_small().collect::<Vec<_>>();

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
        let c_centrality = 4.0;
        let d_centrality = 3.0;

        let a_rank = 1;
        let b_rank = 2;
        let c_rank = 3;
        let d_rank = 4;

        let mut store =
            EdgeStore::open(&temp_dir.as_ref().join("test-segment"), ShardId::new(0)).unwrap();

        let e1 = Edge {
            from: b.clone(),
            to: a.clone(),
            label: "test".to_string(),
            rel_flags: RelFlags::default(),
            sort_score: a_rank + b_rank,
            from_centrality: b_centrality,
            to_centrality: a_centrality,
            from_rank: b_rank,
            to_rank: a_rank,
            ..Edge::empty()
        };

        let e2 = Edge {
            from: c.clone(),
            to: a.clone(),
            label: "2".to_string(),
            rel_flags: RelFlags::default(),
            sort_score: a_rank + c_rank,
            from_centrality: c_centrality,
            to_centrality: a_centrality,
            from_rank: c_rank,
            to_rank: a_rank,
            ..Edge::empty()
        };

        let e3 = Edge {
            from: d.clone(),
            to: a.clone(),
            label: "3".to_string(),
            rel_flags: RelFlags::default(),
            sort_score: a_rank + d_rank,
            from_centrality: d_centrality,
            to_centrality: a_centrality,
            from_rank: d_rank,
            to_rank: a_rank,
            ..Edge::empty()
        };

        store.insert(e3.clone()).unwrap();
        store.insert(e2.clone()).unwrap();
        store.insert(e1.clone()).unwrap();

        store.commit().unwrap();

        let query = HostBacklinksQuery::new(a.id());
        let edges = store.search(&query).unwrap();

        assert_eq!(edges.len(), 3);

        assert_eq!(edges[0].from, e1.from.id());
        assert_eq!(edges[1].from, e2.from.id());
        assert_eq!(edges[2].from, e3.from.id());
    }

    #[test]
    fn test_optimize_read() {
        let temp_dir = crate::gen_temp_dir().unwrap();
        let mut store = EdgeStore::open(&temp_dir, ShardId::new(0)).unwrap();

        store
            .insert(Edge::new_test(
                Node::from("https://www.first.com").into_host(),
                Node::from("https://www.second.com").into_host(),
            ))
            .unwrap();
        store.commit().unwrap();

        store
            .insert(Edge::new_test(
                Node::from("https://www.second.com").into_host(),
                Node::from("https://www.first.com").into_host(),
            ))
            .unwrap();
        store.commit().unwrap();

        store
            .insert(Edge::new_test(
                Node::from("https://www.third.com").into_host(),
                Node::from("https://www.first.com").into_host(),
            ))
            .unwrap();
        store.commit().unwrap();

        let segments = store.index.load_metas().unwrap().segments;
        assert_eq!(segments.len(), 3);

        store.optimize_read().unwrap();

        let segments = store.index.load_metas().unwrap().segments;
        assert_eq!(segments.len(), 1);
    }
}
