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

use std::{fs::File, ops::Range, path::Path};

use super::{
    document::SmallEdge,
    query::collector::{Collector, TantivyCollector},
    schema::{self, Field, FromId, ToId},
};
use crate::{
    webgraph::merge::{EdgeMerger, MergeIter},
    webpage::html::links::RelFlags,
    Result,
};
use anyhow::bail;
use file_store::{
    iterable::{
        ConstIterableStoreReader, ConstIterableStoreWriter, IterableStoreReader,
        IterableStoreWriter,
    },
    ConstSerializable,
};
use itertools::Itertools;
use tantivy::{columnar::Column, DocId, SegmentReader};

use super::{
    merge::{MergeNode, MergeSegmentOrd},
    query::Query,
    Compression, NodeDatum, NodeID, SegmentEdge, StoredEdge,
};

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct EdgeRange {
    range: std::ops::Range<u64>,
    host_rank: u64,
}

impl EdgeRange {
    pub fn new(range: std::ops::Range<u64>, host_rank: u64) -> Self {
        Self { range, host_rank }
    }
}

#[inline]
fn usize_range(range: std::ops::Range<u64>) -> std::ops::Range<usize> {
    range.start as usize..range.end as usize
}

pub struct RangesDb {
    edges: speedy_kv::Db<NodeID, EdgeRange>,
    labels: speedy_kv::Db<NodeID, std::ops::Range<u64>>,
}

impl RangesDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        let edges = speedy_kv::Db::open_or_create(path.as_ref().join("edges")).unwrap();
        let labels = speedy_kv::Db::open_or_create(path.as_ref().join("labels")).unwrap();

        Self { edges, labels }
    }

    pub fn optimize_read(&mut self) {
        self.edges.merge_all_segments().unwrap();
        self.labels.merge_all_segments().unwrap();
    }

    fn uncommitted_node_inserts(&self) -> usize {
        self.edges.uncommitted_inserts()
    }

    pub fn commit(&mut self) {
        self.edges.commit().unwrap();
        self.labels.commit().unwrap();
    }

    pub fn nodes_get_raw<'a>(
        &'a self,
        key: &'a [u8],
    ) -> Option<speedy_kv::SerializedRef<'a, EdgeRange>> {
        self.edges.get_raw(key)
    }

    pub fn labels_get_raw<'a>(
        &'a self,
        key: &'a [u8],
    ) -> Option<speedy_kv::SerializedRef<'a, Range<u64>>> {
        self.labels.get_raw(key)
    }

    pub fn insert_raw_node(&mut self, node: Vec<u8>, range: Vec<u8>) {
        self.edges.insert_raw(node, range);
    }

    pub fn insert_raw_label(&mut self, node: Vec<u8>, range: Vec<u8>) {
        self.labels.insert_raw(node, range);
    }

    fn merge_nodes(&self) -> impl Iterator<Item = MergeNode> + '_ {
        self.edges
            .sorted_iter_raw()
            .zip_eq(self.labels.sorted_iter_raw())
            .map(move |((key_node, val), (key_label, labels))| {
                debug_assert_eq!(key_node, key_label);

                let node = NodeID::from_be_bytes(key_node.as_bytes().try_into().unwrap());
                let range = EdgeRange::deserialize(val.as_bytes());

                let labels = {
                    let range = Range::deserialize(labels.as_bytes());
                    range.start..range.end
                };

                MergeNode::new(node, range, labels)
            })
    }
}

impl ConstSerializable for NodeID {
    const BYTES: usize = std::mem::size_of::<NodeID>();

    fn serialize(&self, buf: &mut [u8]) {
        self.as_u64().serialize(buf);
    }

    fn deserialize(buf: &[u8]) -> Self {
        let id = u64::deserialize(buf);
        NodeID::from(id)
    }
}

impl ConstSerializable for RelFlags {
    const BYTES: usize = std::mem::size_of::<RelFlags>();

    fn serialize(&self, buf: &mut [u8]) {
        self.as_u64().serialize(buf);
    }

    fn deserialize(buf: &[u8]) -> Self {
        u64::deserialize(buf).into()
    }
}

impl ConstSerializable for NodeDatum {
    const BYTES: usize = std::mem::size_of::<NodeDatum>();

    fn serialize(&self, buf: &mut [u8]) {
        let node = self.node().to_be_bytes();
        buf[..8].copy_from_slice(&node);
        self.host_rank().serialize(&mut buf[8..]);
    }

    fn deserialize(buf: &[u8]) -> Self {
        let node = NodeID::from_be_bytes(buf[..8].try_into().unwrap());
        let host_rank = u64::deserialize(&buf[8..]);

        Self::new(node, host_rank)
    }
}

impl ConstSerializable for EdgeRange {
    const BYTES: usize = std::mem::size_of::<EdgeRange>();

    fn serialize(&self, buf: &mut [u8]) {
        const RANGE_BYTES: usize = std::mem::size_of::<u64>() * 2;
        self.range.serialize(&mut buf[..RANGE_BYTES]);
        self.host_rank.serialize(&mut buf[RANGE_BYTES..]);
    }

    fn deserialize(buf: &[u8]) -> Self {
        const RANGE_BYTES: usize = std::mem::size_of::<u64>() * 2;
        let range: Range<u64> = Range::deserialize(&buf[..RANGE_BYTES]);
        let host_rank = u64::deserialize(&buf[RANGE_BYTES..]);

        Self { range, host_rank }
    }
}

impl ConstSerializable for StoredEdge {
    const BYTES: usize = std::mem::size_of::<StoredEdge>();

    fn serialize(&self, buf: &mut [u8]) {
        self.other.serialize(&mut buf[..NodeDatum::BYTES]);
        self.rel.serialize(&mut buf[NodeDatum::BYTES..]);
    }

    fn deserialize(buf: &[u8]) -> Self {
        let other = NodeDatum::deserialize(&buf[..NodeDatum::BYTES]);
        let rel = RelFlags::deserialize(&buf[NodeDatum::BYTES..]);

        Self::new(other, rel)
    }
}

pub const NUM_LABELS_PER_BLOCK: usize = 128;

#[derive(bincode::Encode, bincode::Decode)]
pub struct LabelBlock {
    labels: Vec<String>,
}

impl LabelBlock {
    pub fn new(labels: Vec<String>) -> Self {
        Self { labels }
    }

    pub fn compress(&self, compression: Compression) -> CompressedLabelBlock {
        let bytes = bincode::encode_to_vec(self, common::bincode_config()).unwrap();
        let compressed = compression.compress(&bytes);

        CompressedLabelBlock {
            compressions: compression,
            data: compressed,
        }
    }
}

#[derive(bincode::Encode, bincode::Decode)]
pub struct CompressedLabelBlock {
    compressions: Compression,
    data: Vec<u8>,
}

impl CompressedLabelBlock {
    pub fn decompress(&self) -> LabelBlock {
        let bytes = self.compressions.decompress(&self.data);
        let (res, _) = bincode::decode_from_slice(&bytes, common::bincode_config()).unwrap();
        res
    }
}

pub struct EdgeStore {
    reversed: bool,
    ranges: RangesDb,

    edge_labels: IterableStoreReader<CompressedLabelBlock>,
    edges: ConstIterableStoreReader<StoredEdge>,
    index: tantivy::Index,
}

impl EdgeStore {
    pub fn open<P: AsRef<Path>>(path: P, reversed: bool) -> Self {
        let ranges = RangesDb::open(path.as_ref().join("ranges"));

        let edge_labels = IterableStoreReader::open(path.as_ref().join("labels")).unwrap();

        let edges = ConstIterableStoreReader::open(path.as_ref().join("edges")).unwrap();

        Self {
            ranges,
            edge_labels,
            edges,
            reversed,
            index: todo!(),
        }
    }

    pub fn optimize_read(&mut self) {
        self.ranges.optimize_read();
    }

    fn merge_postings_for_node<'a>(
        buf: &[MergeNode<MergeSegmentOrd>],
        stores: &'a [EdgeStore],
    ) -> EdgeMerger<'a> {
        let mut edges = Vec::new();

        for node in buf {
            let store = &stores[node.ord().as_usize()];
            let stored_edges = store.edges.slice(usize_range(node.range().range.clone()));
            let labels = store
                .edge_labels
                .slice(usize_range(node.labels()))
                .map(|r| r.decompress())
                .flat_map(|block| block.labels.into_iter());

            edges.push(
                stored_edges
                    .zip_eq(labels)
                    .map(|(edge, label)| edge.with_label(label)),
            );
        }

        EdgeMerger::new(edges)
    }

    fn merge_postings<P: AsRef<Path>>(
        stores: &[EdgeStore],
        label_compression: Compression,
        folder: P,
    ) -> Result<Self> {
        let reversed = stores[0].reversed;
        let mut ranges = RangesDb::open(folder.as_ref().join("ranges"));

        let labels_file = File::options()
            .read(true)
            .create(true)
            .truncate(false)
            .write(true)
            .open(folder.as_ref().join("labels"))
            .unwrap();
        let mut labels_store = IterableStoreWriter::new(labels_file);

        let edges_file = File::options()
            .read(true)
            .create(true)
            .truncate(false)
            .write(true)
            .open(folder.as_ref().join("edges"))
            .unwrap();
        let mut edges_store: ConstIterableStoreWriter<StoredEdge, File> =
            ConstIterableStoreWriter::new(edges_file);

        let mut merge_iter = MergeIter::new(
            stores
                .iter()
                .map(|store| store.ranges.merge_nodes())
                .collect(),
        );

        let mut buf = Vec::new();

        while merge_iter.advance(&mut buf) {
            if buf.is_empty() {
                continue;
            }

            let edges = Self::merge_postings_for_node(&buf, stores);

            // write postings
            let node_host_rank = buf[0].range().host_rank;
            let node_id = buf[0].id();
            debug_assert!(buf.iter().all(|node| node.id() == node_id));
            let mut first_label_offset = None;
            let mut last_label_offset = None;
            let mut first_node_offset = None;
            let mut last_node_offset = None;

            for chunk in edges.chunks(NUM_LABELS_PER_BLOCK).into_iter() {
                let (labels, edges): (Vec<_>, Vec<_>) = chunk
                    .map(|edge| (edge.label().clone(), edge.with_label(())))
                    .unzip();

                let label_block = LabelBlock::new(labels).compress(label_compression);

                let label_offset = labels_store.write(&label_block).unwrap();

                if first_label_offset.is_none() {
                    first_label_offset = Some(label_offset);
                }

                last_label_offset = Some(label_offset);

                for edge in edges {
                    let offset = edges_store.write(&edge).unwrap();

                    if first_node_offset.is_none() {
                        first_node_offset = Some(offset);
                    }

                    last_node_offset = Some(offset);
                }
            }

            let label_range = Range {
                start: first_label_offset.unwrap().start,
                end: last_label_offset.unwrap().start + last_label_offset.unwrap().num_bytes,
            };

            let node_range: EdgeRange = EdgeRange::new(
                Range {
                    start: first_node_offset.unwrap().start,
                    end: last_node_offset.unwrap().start + last_node_offset.unwrap().num_bytes,
                },
                node_host_rank,
            );
            let node_range_bytes = node_range.serialize_to_vec();

            let node_bytes = node_id.as_u64().to_be_bytes().to_vec();
            ranges.insert_raw_node(node_bytes.clone(), node_range_bytes);

            let label_range_bytes = label_range.serialize_to_vec();

            ranges.insert_raw_label(node_bytes, label_range_bytes);

            if ranges.uncommitted_node_inserts() > 100_000_000 {
                ranges.commit();
                edges_store.flush().unwrap();
                labels_store.flush().unwrap();
            }
        }

        if ranges.uncommitted_node_inserts() > 0 {
            ranges.commit();
            edges_store.flush().unwrap();
            labels_store.flush().unwrap();
        }

        Ok(Self::open(folder, reversed))
    }

    pub fn merge<P: AsRef<Path>>(
        stores: Vec<EdgeStore>,
        label_compression: Compression,
        path: P,
    ) -> Result<()> {
        if stores.is_empty() {
            return Ok(());
        }

        if !path.as_ref().exists() {
            std::fs::create_dir_all(&path)?;
        }

        let reversed = stores[0].reversed;
        if !stores.iter().all(|store| store.reversed == reversed) {
            bail!("Cannot merge stores with different reversed flags");
        }

        let mut res = Self::merge_postings(&stores, label_compression, path)?;
        res.optimize_read();

        Ok(())
    }

    pub fn degree(&self, node: &NodeID) -> u64 {
        let node_bytes = node.as_u64().to_be_bytes();

        match self.ranges.edges.get_raw(&node_bytes) {
            Some(node_range_bytes) => {
                let range = EdgeRange::deserialize(node_range_bytes.as_bytes());
                self.edges.slice(usize_range(range.range)).len() as u64
            }
            None => 0,
        }
    }

    pub fn get_with_label<'a>(
        &'a self,
        node: &'a NodeID,
    ) -> Box<dyn Iterator<Item = SegmentEdge<String>> + Send + Sync + 'a> {
        let node_bytes = node.as_u64().to_be_bytes();

        match (
            self.ranges.edges.get_raw(&node_bytes),
            self.ranges.labels.get_raw(&node_bytes),
        ) {
            (Some(node_range_bytes), Some(edge_range_bytes)) => {
                let node_range = EdgeRange::deserialize(node_range_bytes.as_bytes());
                let label_range: Range<u64> = Range::deserialize(edge_range_bytes.as_bytes());

                let labels = self
                    .edge_labels
                    .slice(usize_range(label_range))
                    .map(|r| r.decompress())
                    .flat_map(|block| block.labels.into_iter());

                let edges = self.edges.slice(usize_range(node_range.range));

                Box::new(labels.zip_eq(edges).map(move |(label, edge)| {
                    if self.reversed {
                        SegmentEdge {
                            from: edge.other,
                            to: NodeDatum::new(*node, node_range.host_rank),
                            rel: edge.rel,
                            label,
                        }
                    } else {
                        SegmentEdge {
                            from: NodeDatum::new(*node, node_range.host_rank),
                            to: edge.other,
                            rel: edge.rel,
                            label,
                        }
                    }
                }))
            }
            _ => Box::new(std::iter::empty()),
        }
    }

    pub fn get_without_label<'a>(
        &'a self,
        node: &'a NodeID,
    ) -> Box<dyn Iterator<Item = SegmentEdge<()>> + Send + Sync + 'a> {
        let node_bytes = node.as_u64().to_be_bytes();

        match self.ranges.edges.get_raw(&node_bytes) {
            Some(node_range_bytes) => {
                let edge_range = EdgeRange::deserialize(node_range_bytes.as_bytes());

                let edges = self.edges.slice(usize_range(edge_range.range));

                Box::new(edges.into_iter().map(move |edge| {
                    if self.reversed {
                        SegmentEdge {
                            from: edge.other,
                            to: NodeDatum::new(*node, edge_range.host_rank),
                            rel: edge.rel,
                            label: (),
                        }
                    } else {
                        SegmentEdge {
                            from: NodeDatum::new(*node, edge_range.host_rank),
                            to: edge.other,
                            rel: edge.rel,
                            label: (),
                        }
                    }
                }))
            }
            _ => Box::new(std::iter::empty()),
        }
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
            .flat_map(|segment| SmallSegmentEdges::new(&segment))
    }
}

pub struct SmallSegmentEdges {
    from_id: Column,
    to_id: Column,
    rel_flags: Column,

    docs: Box<dyn Iterator<Item = DocId>>,
}

impl SmallSegmentEdges {
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

impl Iterator for SmallSegmentEdges {
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
    use std::sync::Arc;

    use crate::webgraph::{store_writer::EdgeStoreWriter, Edge, FullNodeID, InsertableEdge};

    use super::*;

    #[test]
    fn test_insert() {
        let temp_dir = crate::gen_temp_dir().unwrap();
        let mut kv: EdgeStoreWriter =
            EdgeStoreWriter::new(&temp_dir, Compression::default(), false, None);

        let e = InsertableEdge {
            from: FullNodeID {
                id: NodeID::from(0_u64),
                host: NodeID::from(0_u64),
            },
            to: FullNodeID {
                id: NodeID::from(1_u64),
                host: NodeID::from(0_u64),
            },
            label: "test".to_string(),
            rel: RelFlags::default(),
        };

        kv.put(e.clone());

        let store = kv.finalize();

        let edges: Vec<_> = store.get_with_label(&NodeID::from(0_u64)).collect();

        assert_eq!(edges.len(), 1);
        assert_eq!(&edges[0].to.node(), &SegmentEdge::from(e.clone()).to.node());

        let edges: Vec<_> = store.get_with_label(&NodeID::from(1_u64)).collect();

        assert_eq!(edges.len(), 0);

        let edges = store.iter_small().collect::<Vec<_>>();

        assert_eq!(edges.len(), 1);
    }

    #[test]
    fn test_reversed() {
        let temp_dir = crate::gen_temp_dir().unwrap();
        let mut kv: EdgeStoreWriter =
            EdgeStoreWriter::new(&temp_dir, Compression::default(), true, None);

        let e = InsertableEdge {
            from: FullNodeID {
                id: NodeID::from(0_u64),
                host: NodeID::from(0_u64),
            },
            to: FullNodeID {
                id: NodeID::from(1_u64),
                host: NodeID::from(0_u64),
            },
            label: "test".to_string(),
            rel: RelFlags::default(),
        };

        kv.put(e.clone());

        let store = kv.finalize();

        let edges: Vec<_> = store.get_with_label(&NodeID::from(0_u64)).collect();
        assert_eq!(edges.len(), 0);

        let edges: Vec<_> = store.get_with_label(&NodeID::from(1_u64)).collect();
        assert_eq!(edges.len(), 1);
        assert_eq!(&edges[0], &SegmentEdge::from(e.clone()));
    }

    #[test]
    fn test_edge_ordering() {
        let temp_dir = crate::gen_temp_dir().unwrap();
        let mut rank_store =
            speedy_kv::Db::open_or_create(temp_dir.as_ref().join("test-rank-store")).unwrap();

        rank_store.insert(NodeID::from(2_u64), 1).unwrap();
        rank_store.insert(NodeID::from(3_u64), 2).unwrap();
        rank_store.insert(NodeID::from(1_u64), 3).unwrap();

        rank_store.commit().unwrap();

        let mut kv: EdgeStoreWriter = EdgeStoreWriter::new(
            &temp_dir.as_ref().join("test-segment"),
            Compression::default(),
            true,
            Some(Arc::new(rank_store)),
        );

        let e1 = InsertableEdge {
            from: FullNodeID {
                id: NodeID::from(1_u64),
                host: NodeID::from(1_u64),
            },
            to: FullNodeID {
                id: NodeID::from(0_u64),
                host: NodeID::from(0_u64),
            },
            label: "1".to_string(),
            rel: RelFlags::default(),
        };

        let e2 = InsertableEdge {
            from: FullNodeID {
                id: NodeID::from(2_u64),
                host: NodeID::from(2_u64),
            },
            to: FullNodeID {
                id: NodeID::from(0_u64),
                host: NodeID::from(0_u64),
            },
            label: "2".to_string(),
            rel: RelFlags::default(),
        };

        let e3 = InsertableEdge {
            from: FullNodeID {
                id: NodeID::from(3_u64),
                host: NodeID::from(3_u64),
            },
            to: FullNodeID {
                id: NodeID::from(0_u64),
                host: NodeID::from(0_u64),
            },
            label: "3".to_string(),
            rel: RelFlags::default(),
        };

        kv.put(e1.clone());
        kv.put(e2.clone());
        kv.put(e3.clone());

        let store = kv.finalize();

        let edges: Vec<_> = store.get_with_label(&NodeID::from(0_u64)).collect();

        assert_eq!(edges.len(), 3);

        assert_eq!(
            Edge::from(edges[0].clone()).from.node(),
            Edge::from(e2.clone()).from.node()
        );
        assert_eq!(
            Edge::from(edges[1].clone()).from.node(),
            Edge::from(e3.clone()).from.node()
        );
        assert_eq!(
            Edge::from(edges[2].clone()).from.node(),
            Edge::from(e1.clone()).from.node()
        );
    }

    #[test]
    fn test_degree() {
        let temp_dir = crate::gen_temp_dir().unwrap();
        let mut kv: EdgeStoreWriter =
            EdgeStoreWriter::new(&temp_dir, Compression::default(), false, None);

        let e1 = InsertableEdge {
            from: FullNodeID {
                id: NodeID::from(1_u64),
                host: NodeID::from(1_u64),
            },
            to: FullNodeID {
                id: NodeID::from(2_u64),
                host: NodeID::from(2_u64),
            },
            label: "1".to_string(),
            rel: RelFlags::default(),
        };

        kv.put(e1.clone());

        let store = kv.finalize();

        assert_eq!(store.degree(&NodeID::from(1_u64)), 1);
        assert_eq!(store.degree(&NodeID::from(2_u64)), 0);
    }
}
