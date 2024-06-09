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

pub const MAX_BATCH_SIZE: usize = 3_000_000;

use std::{
    collections::BTreeSet,
    fs::File,
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};

use itertools::Itertools;

use crate::{
    webgraph::store::{NodeRange, NUM_LABELS_PER_BLOCK},
    Result,
};
use file_store::{
    iterable::{
        ConstIterableStoreWriter, IterableStoreReader, IterableStoreWriter,
        SortedIterableStoreReader,
    },
    ConstSerializable,
};

use super::{
    merge::NodeDatum,
    store::{CompressedLabelBlock, EdgeStore, HostDb, LabelBlock, RangesDb},
    Compression, EdgeLabel, InsertableEdge, NodeID,
};

#[derive(bincode::Encode, bincode::Decode)]
struct SortableEdge<L: EdgeLabel> {
    sort_node: NodeID,
    secondary_node: NodeID,
    edge: InsertableEdge<L>,
}

impl<L: EdgeLabel> PartialOrd for SortableEdge<L> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<L: EdgeLabel> Ord for SortableEdge<L> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sort_node
            .cmp(&other.sort_node)
            .then(self.secondary_node.cmp(&other.secondary_node))
    }
}

impl<L: EdgeLabel> PartialEq for SortableEdge<L> {
    fn eq(&self, other: &Self) -> bool {
        self.sort_node == other.sort_node && self.secondary_node == other.secondary_node
    }
}

impl<L: EdgeLabel> Eq for SortableEdge<L> {}

struct SortedEdgeIterator<M, D>
where
    M: Iterator<Item = SortableEdge<String>>,
    D: Iterator<Item = SortableEdge<String>>,
{
    mem: file_store::Peekable<M>,
    file_reader: file_store::Peekable<D>,
}

impl<M, D> Iterator for SortedEdgeIterator<M, D>
where
    M: Iterator<Item = SortableEdge<String>>,
    D: Iterator<Item = SortableEdge<String>>,
{
    type Item = SortableEdge<String>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(edge) = self.mem.peek() {
            if let Some(file_edge) = self.file_reader.peek() {
                if edge.sort_node < file_edge.sort_node {
                    self.mem.next()
                } else {
                    self.file_reader.next()
                }
            } else {
                self.mem.next()
            }
        } else {
            self.file_reader.next()
        }
    }
}

pub struct EdgeStoreWriter {
    reversed: bool,
    path: PathBuf,
    edges: BTreeSet<SortableEdge<String>>,
    stored_writers: Vec<PathBuf>,
    compression: Compression,
    host_centrality_rank_store: Option<Arc<speedy_kv::Db<NodeID, u64>>>,
}

impl EdgeStoreWriter {
    pub fn new<P: AsRef<Path>>(
        path: P,
        compression: Compression,
        reversed: bool,
        host_centrality_rank_store: Option<Arc<speedy_kv::Db<NodeID, u64>>>,
    ) -> Self {
        let writer_path = path.as_ref().join("writer");

        if !writer_path.exists() {
            std::fs::create_dir_all(&writer_path).unwrap();
        }

        Self {
            edges: BTreeSet::new(),
            reversed,
            path: path.as_ref().to_path_buf(),
            compression,
            stored_writers: Vec::new(),
            host_centrality_rank_store,
        }
    }

    fn flush_to_file(&mut self) -> Result<()> {
        let file_path = self
            .path
            .join("writer")
            .join(format!("{}.store", self.stored_writers.len()));
        let file = File::create(&file_path)?;

        let mut writer = file_store::iterable::IterableStoreWriter::new(file);

        for edge in &self.edges {
            writer.write(edge)?;
        }
        writer.finalize()?;

        self.edges.clear();

        self.stored_writers.push(file_path);

        Ok(())
    }

    pub fn put(&mut self, edge: InsertableEdge<String>) {
        let (sort_node, secondary_node) = if self.reversed {
            (edge.to.id, edge.from.id)
        } else {
            (edge.from.id, edge.to.id)
        };

        self.edges.insert(SortableEdge {
            sort_node,
            secondary_node,
            edge,
        });

        if self.edges.len() >= MAX_BATCH_SIZE {
            self.flush_to_file().unwrap();
        }
    }

    fn sorted_edges(mut self) -> impl Iterator<Item = SortableEdge<String>> {
        let readers = self
            .stored_writers
            .iter()
            .map(|p| IterableStoreReader::open(p).unwrap())
            .collect();
        let file_reader = SortedIterableStoreReader::new(readers).map(|r| r.unwrap());

        let edges = std::mem::take(&mut self.edges);

        SortedEdgeIterator {
            mem: file_store::Peekable::new(edges.into_iter()),
            file_reader: file_store::Peekable::new(file_reader),
        }
    }

    pub fn finalize(mut self) -> EdgeStore {
        self.flush_to_file().unwrap();

        let mut final_writer = FinalEdgeStoreWriter::open(
            self.compression,
            self.reversed,
            self.host_centrality_rank_store.clone(),
            &self.path,
        );

        let mut store = final_writer.build_store(self.sorted_edges().dedup().map(|e| e.edge));
        store.optimize_read();

        store
    }
}

impl Drop for EdgeStoreWriter {
    fn drop(&mut self) {
        std::fs::remove_dir_all(self.path.join("writer")).unwrap();
    }
}

struct FinalEdgeStoreWriter {
    ranges: RangesDb,
    hosts: HostDb,

    edge_labels: IterableStoreWriter<CompressedLabelBlock, File>,
    edge_nodes: ConstIterableStoreWriter<NodeDatum, File>,

    host_centrality_rank_store: Option<Arc<speedy_kv::Db<NodeID, u64>>>,

    compression: Compression,
    reversed: bool,

    path: PathBuf,
}

impl FinalEdgeStoreWriter {
    fn open<P: AsRef<Path>>(
        compression: Compression,
        reversed: bool,
        host_centrality_rank_store: Option<Arc<speedy_kv::Db<NodeID, u64>>>,
        path: P,
    ) -> Self {
        let ranges = RangesDb::open(path.as_ref().join("ranges"));

        let edge_labels_file = File::options()
            .read(true)
            .create(true)
            .truncate(false)
            .write(true)
            .open(path.as_ref().join("labels"))
            .unwrap();
        let edge_labels = IterableStoreWriter::new(edge_labels_file);

        let edge_nodes_file = File::options()
            .read(true)
            .create(true)
            .truncate(false)
            .write(true)
            .open(path.as_ref().join("nodes"))
            .unwrap();
        let edge_nodes = ConstIterableStoreWriter::new(edge_nodes_file);

        Self {
            ranges,
            hosts: HostDb::open(path.as_ref().join("hosts")),
            edge_labels,
            edge_nodes,
            reversed,
            compression,
            path: path.as_ref().to_path_buf(),
            host_centrality_rank_store,
        }
    }
    /// Insert a batch of edges into the store.
    /// The edges *must* have been de-duplicated by their from/to node.
    /// I.e. if the store is not reversed, there should only ever be a single
    /// put for each from node, and vice versa.
    fn put_store(&mut self, edges: &mut [InsertableEdge<String>]) {
        if edges.is_empty() {
            return;
        }

        let node = if self.reversed {
            edges[0].to.clone()
        } else {
            edges[0].from.clone()
        };

        // order edges by centrality rank of the other node host
        if let Some(rank_store) = &self.host_centrality_rank_store {
            edges.sort_by_cached_key(|e| {
                if self.reversed {
                    rank_store.get(&e.from.host).unwrap().unwrap_or(u64::MAX)
                } else {
                    rank_store.get(&e.to.host).unwrap().unwrap_or(u64::MAX)
                }
            });
        }

        self.hosts.insert(&node);
        let node_bytes = node.id.serialize_to_vec();

        debug_assert!(self.ranges.nodes_get_raw(&node_bytes).is_none());
        debug_assert!(self.ranges.labels_get_raw(&node_bytes).is_none());

        let mut edge_labels = Vec::new();
        let mut edge_nodes: Vec<NodeDatum> = Vec::new();

        for edge in edges {
            edge_labels.push(edge.label.clone());

            let (node, host) = if self.reversed {
                (edge.from.id, edge.from.host)
            } else {
                (edge.to.id, edge.to.host)
            };

            let sort_key = self
                .host_centrality_rank_store
                .as_ref()
                .map(|store| store.get(&host).unwrap().unwrap_or(u64::MAX))
                .unwrap_or(0);

            let datum = NodeDatum::new(node, sort_key);
            edge_nodes.push(datum);
        }

        let edge_labels: Vec<_> = edge_labels
            .into_iter()
            .chunks(NUM_LABELS_PER_BLOCK)
            .into_iter()
            .map(|chunk| LabelBlock::new(chunk.collect()).compress(self.compression))
            .collect();

        let mut first_label_offset = None;
        let mut last_label_offset = None;
        let mut first_node_offset = None;
        let mut last_node_offset = None;

        for block in &edge_labels {
            let offset = self.edge_labels.write(block).unwrap();

            if first_label_offset.is_none() {
                first_label_offset = Some(offset);
            }

            last_label_offset = Some(offset);
        }

        for node in &edge_nodes {
            let offset = self.edge_nodes.write(node).unwrap();

            if first_node_offset.is_none() {
                first_node_offset = Some(offset);
            }

            last_node_offset = Some(offset);
        }

        let label_range = Range {
            start: first_label_offset.unwrap().start,
            end: last_label_offset.unwrap().start + last_label_offset.unwrap().num_bytes,
        };

        let sort_key = self
            .host_centrality_rank_store
            .as_ref()
            .map(|store| store.get(&node.host).unwrap().unwrap_or(u64::MAX))
            .unwrap_or(0);

        let node_range: NodeRange = NodeRange::new(
            Range {
                start: first_node_offset.unwrap().start,
                end: last_node_offset.unwrap().start + last_node_offset.unwrap().num_bytes,
            },
            sort_key,
        );
        let node_range_bytes = node_range.serialize_to_vec();

        self.ranges
            .insert_raw_node(node_bytes.clone(), node_range_bytes);

        let label_range_bytes = label_range.serialize_to_vec();

        self.ranges.insert_raw_label(node_bytes, label_range_bytes);
    }

    /// Build a new edge store from a set of edges.
    ///
    /// **IMPORTANT** The edges must be sorted by
    /// either the from or to node, depending on the value of `reversed`.
    pub fn build_store(
        &mut self,
        edges: impl Iterator<Item = InsertableEdge<String>>,
    ) -> EdgeStore {
        let mut inserts_since_last_flush = 0;

        // create batches of consecutive edges with the same from/to node
        let mut batch = Vec::new();
        let mut last_node = None;
        for edge in edges {
            if let Some(last_node) = last_node {
                if (self.reversed && edge.to.id != last_node)
                    || (!self.reversed && edge.from.id != last_node)
                {
                    batch.sort_unstable_by_key(
                        |e: &InsertableEdge<_>| if self.reversed { e.from.id } else { e.to.id },
                    );
                    batch.dedup_by_key(|e| if self.reversed { e.from.id } else { e.to.id });
                    let batch_len = batch.len();
                    self.put_store(&mut batch);
                    batch.clear();
                    inserts_since_last_flush += batch_len;

                    if inserts_since_last_flush >= 100_000_000 {
                        self.flush();
                        inserts_since_last_flush = 0;
                    }
                }
            }

            last_node = Some(if self.reversed {
                edge.to.id
            } else {
                edge.from.id
            });
            batch.push(edge);
        }

        if !batch.is_empty() {
            batch.sort_unstable_by_key(
                |e: &InsertableEdge<_>| if self.reversed { e.from.id } else { e.to.id },
            );
            batch.dedup_by_key(|e| if self.reversed { e.from.id } else { e.to.id });
            self.put_store(&mut batch);
        }

        self.flush();

        EdgeStore::open(&self.path, self.reversed)
    }

    fn flush(&mut self) {
        self.hosts.flush();

        self.ranges.commit();

        self.edge_nodes.flush().unwrap();
        self.edge_labels.flush().unwrap();
    }
}
