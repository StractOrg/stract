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

use std::{fs::File, io::Write, ops::Range, path::Path};

use fst::Automaton;
use itertools::Itertools;
use memmap2::Mmap;

use crate::speedy_kv;

pub use super::store_writer::EdgeStoreWriter;
use super::{Compression, Edge, FullNodeID, InnerEdge, NodeID};

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
)]
struct SerializedEdge {
    from_prefix: NodeID,
    to_prefix: NodeID,
    label: Vec<u8>,
}

struct PrefixDb {
    db: speedy_kv::Db<Vec<u8>, Vec<u8>>,
}

impl PrefixDb {
    fn open<P: AsRef<Path>>(path: P) -> Self {
        let db = speedy_kv::Db::open_or_create(path).unwrap();

        Self { db }
    }

    fn insert(&mut self, node: &FullNodeID) {
        let key = [
            node.prefix.as_u64().to_le_bytes(),
            node.id.as_u64().to_le_bytes(),
        ]
        .concat();
        let value = vec![];

        self.db.insert_raw(key, value);
    }

    fn get(&self, prefix: &NodeID) -> Vec<NodeID> {
        let prefix = prefix.as_u64().to_le_bytes().to_vec();

        let query = speedy_kv::automaton::ExactMatch(&prefix).starts_with();

        self.db
            .search_raw(query)
            .map(|(key, _)| {
                let id = u64::from_le_bytes(
                    key.as_bytes()[u64::BITS as usize / 8..].try_into().unwrap(),
                );
                NodeID::from(id)
            })
            .collect()
    }

    fn flush(&mut self) {
        self.db.commit().unwrap();
        self.db.merge_all_segments().unwrap();
    }
}

struct RangesDb {
    nodes: speedy_kv::Db<NodeID, std::ops::Range<u64>>,
    labels: speedy_kv::Db<NodeID, std::ops::Range<u64>>,
}

impl RangesDb {
    fn open<P: AsRef<Path>>(path: P) -> Self {
        let nodes = speedy_kv::Db::open_or_create(path.as_ref().join("nodes")).unwrap();
        let labels = speedy_kv::Db::open_or_create(path.as_ref().join("labels")).unwrap();

        Self { nodes, labels }
    }
}

pub struct EdgeStore {
    reversed: bool,
    ranges: RangesDb,
    prefixes: PrefixDb,

    edge_labels_file: File,
    edge_labels_len: usize,
    edge_labels: Mmap,

    edge_nodes_file: File,
    edge_nodes_len: usize,
    edge_nodes: Mmap,

    compression: Compression,
}

impl EdgeStore {
    pub fn open<P: AsRef<Path>>(path: P, reversed: bool, compression: Compression) -> Self {
        let ranges = RangesDb::open(path.as_ref().join("ranges"));

        let edge_labels_file = File::options()
            .read(true)
            .create(true)
            .truncate(false)
            .write(true)
            .open(path.as_ref().join("labels"))
            .unwrap();
        let edge_labels = unsafe { Mmap::map(&edge_labels_file).unwrap() };
        let edge_labels_len = edge_labels.len();

        let edge_nodes_file = File::options()
            .read(true)
            .create(true)
            .truncate(false)
            .write(true)
            .open(path.as_ref().join("nodes"))
            .unwrap();
        let edge_nodes = unsafe { Mmap::map(&edge_nodes_file).unwrap() };
        let edge_nodes_len = edge_nodes.len();

        Self {
            reversed,
            ranges,
            prefixes: PrefixDb::open(path.as_ref().join("prefixes")),
            edge_labels,
            edge_labels_len,
            edge_labels_file,
            edge_nodes,
            edge_nodes_file,
            edge_nodes_len,
            compression,
        }
    }

    /// Insert a batch of edges into the store.
    /// The edges *must* have been de-duplicated by their from/to node.
    /// I.e. if the store is not reversed, there should only ever be a single
    /// put for each from node, and vice versa.
    fn put(&mut self, edges: &[InnerEdge<String>]) {
        if edges.is_empty() {
            return;
        }

        let node = if self.reversed {
            edges[0].to.clone()
        } else {
            edges[0].from.clone()
        };

        self.prefixes.insert(&node);
        let node_bytes = node.id.as_u64().to_le_bytes();

        debug_assert!(self.ranges.nodes.get_raw(&node_bytes).is_none());
        debug_assert!(self.ranges.labels.get_raw(&node_bytes).is_none());

        let mut edge_labels = Vec::new();
        let mut edge_nodes = Vec::new();

        for edge in edges {
            edge_labels.push(edge.label.clone());
            edge_nodes.push(if self.reversed {
                edge.from.id
            } else {
                edge.to.id
            });
        }

        let edge_labels_bytes =
            bincode::encode_to_vec(&edge_labels, bincode::config::standard()).unwrap();
        let edge_nodes_bytes =
            bincode::encode_to_vec(&edge_nodes, bincode::config::standard()).unwrap();

        let edge_labels_bytes = self.compression.compress(&edge_labels_bytes);
        let edge_nodes_bytes = self.compression.compress(&edge_nodes_bytes);

        let label_range = self.edge_labels_len..(self.edge_labels_len + edge_labels_bytes.len());
        let node_range = self.edge_nodes_len..(self.edge_nodes_len + edge_nodes_bytes.len());

        self.edge_labels_len += edge_labels_bytes.len();
        self.edge_nodes_len += edge_nodes_bytes.len();

        self.edge_labels_file.write_all(&edge_labels_bytes).unwrap();
        self.edge_nodes_file.write_all(&edge_nodes_bytes).unwrap();

        self.ranges.nodes.insert_raw(
            node_bytes.to_vec(),
            bincode::encode_to_vec(node_range, bincode::config::standard()).unwrap(),
        );

        self.ranges.labels.insert_raw(
            node_bytes.to_vec(),
            bincode::encode_to_vec(label_range, bincode::config::standard()).unwrap(),
        );
    }

    /// Build a new edge store from a set of edges.
    ///
    /// **IMPORTANT** The edges must be sorted by
    /// either the from or to node, depending on the value of `reversed`.
    pub fn build<P: AsRef<Path>>(
        path: P,
        compression: Compression,
        reversed: bool,
        edges: impl Iterator<Item = InnerEdge<String>>,
    ) -> Self {
        let mut s = Self::open(path, reversed, compression);

        let mut inserts_since_last_flush = 0;

        // create batches of consecutive edges with the same from/to node
        let mut batch = Vec::new();
        let mut last_node = None;
        for edge in edges {
            if let Some(last_node) = last_node {
                if (reversed && edge.to.id != last_node) || (!reversed && edge.from.id != last_node)
                {
                    batch.sort_unstable_by_key(
                        |e: &InnerEdge<_>| if reversed { e.from.id } else { e.to.id },
                    );
                    batch.dedup_by_key(|e| if reversed { e.from.id } else { e.to.id });
                    s.put(&batch);
                    batch.clear();
                    inserts_since_last_flush += 1;

                    if inserts_since_last_flush >= 1_000_000 {
                        s.flush();
                        inserts_since_last_flush = 0;
                    }
                }
            }

            last_node = Some(if reversed { edge.to.id } else { edge.from.id });
            batch.push(edge);
        }

        if !batch.is_empty() {
            batch.sort_unstable_by_key(
                |e: &InnerEdge<_>| if reversed { e.from.id } else { e.to.id },
            );
            batch.dedup_by_key(|e| if reversed { e.from.id } else { e.to.id });
            s.put(&batch);
        }

        s.flush();

        s
    }

    fn flush(&mut self) {
        self.prefixes.flush();

        self.ranges.labels.commit().unwrap();
        self.ranges.labels.merge_all_segments().unwrap();

        self.ranges.nodes.commit().unwrap();
        self.ranges.nodes.merge_all_segments().unwrap();

        self.edge_nodes_file.flush().unwrap();
        self.edge_labels_file.flush().unwrap();

        self.edge_nodes = unsafe { Mmap::map(&self.edge_nodes_file).unwrap() };
        self.edge_labels = unsafe { Mmap::map(&self.edge_labels_file).unwrap() };

        self.edge_nodes_len = self.edge_nodes.len();
        self.edge_labels_len = self.edge_labels.len();
    }

    pub fn get_with_label(&self, node: &NodeID) -> Vec<Edge<String>> {
        let node_bytes = node.as_u64().to_le_bytes();

        match (
            self.ranges.nodes.get_raw(&node_bytes),
            self.ranges.labels.get_raw(&node_bytes),
        ) {
            (Some(node_range_bytes), Some(edge_range_bytes)) => {
                let (node_range, _) = bincode::decode_from_slice::<Range<usize>, _>(
                    node_range_bytes.as_bytes(),
                    bincode::config::standard(),
                )
                .unwrap();
                let (edge_range, _) = bincode::decode_from_slice::<Range<usize>, _>(
                    edge_range_bytes.as_bytes(),
                    bincode::config::standard(),
                )
                .unwrap();

                let edge_labels = &self.edge_labels[edge_range];
                let edge_labels = self.compression.decompress(edge_labels);
                let (edge_labels, _): (Vec<_>, _) =
                    bincode::decode_from_slice(&edge_labels, bincode::config::standard()).unwrap();

                let edge_nodes = &self.edge_nodes[node_range];
                let edge_nodes = self.compression.decompress(edge_nodes);
                let (edge_nodes, _): (Vec<_>, _) =
                    bincode::decode_from_slice(&edge_nodes, bincode::config::standard()).unwrap();

                edge_labels
                    .into_iter()
                    .zip_eq(edge_nodes)
                    .map(|(label, other)| {
                        if self.reversed {
                            Edge {
                                from: other,
                                to: *node,
                                label,
                            }
                        } else {
                            Edge {
                                from: *node,
                                to: other,
                                label,
                            }
                        }
                    })
                    .collect()
            }
            _ => Vec::new(),
        }
    }

    pub fn get_without_label(&self, node: &NodeID) -> Vec<Edge<()>> {
        let node_bytes = node.as_u64().to_le_bytes();

        match self.ranges.nodes.get_raw(&node_bytes) {
            Some(node_range_bytes) => {
                let (node_range, _) = bincode::decode_from_slice::<Range<usize>, _>(
                    node_range_bytes.as_bytes(),
                    bincode::config::standard(),
                )
                .unwrap();

                let edge_nodes = &self.edge_nodes[node_range];
                let edge_nodes = self.compression.decompress(edge_nodes);
                let (edge_nodes, _): (Vec<_>, _) =
                    bincode::decode_from_slice(&edge_nodes, bincode::config::standard()).unwrap();

                edge_nodes
                    .into_iter()
                    .map(|other| {
                        if self.reversed {
                            Edge {
                                from: other,
                                to: *node,
                                label: (),
                            }
                        } else {
                            Edge {
                                from: *node,
                                to: other,
                                label: (),
                            }
                        }
                    })
                    .collect()
            }
            _ => Vec::new(),
        }
    }

    pub fn nodes_by_prefix(&self, prefix: &NodeID) -> Vec<NodeID> {
        self.prefixes.get(prefix)
    }

    pub fn iter_without_label(&self) -> impl Iterator<Item = Edge<()>> + '_ + Send + Sync {
        self.ranges.nodes.iter_raw().flat_map(move |(key, val)| {
            let node = u64::from_le_bytes((key.as_bytes()).try_into().unwrap());
            let node = NodeID::from(node);

            let (node_range, _) = bincode::decode_from_slice::<Range<usize>, _>(
                val.as_bytes(),
                bincode::config::standard(),
            )
            .unwrap();
            let edge_nodes = &self.edge_nodes[node_range];
            let edge_nodes = self.compression.decompress(edge_nodes);
            let (edge_nodes, _): (Vec<_>, _) =
                bincode::decode_from_slice(&edge_nodes, bincode::config::standard()).unwrap();

            edge_nodes.into_iter().map(move |other| {
                if self.reversed {
                    Edge {
                        from: other,
                        to: node,
                        label: (),
                    }
                } else {
                    Edge {
                        from: node,
                        to: other,
                        label: (),
                    }
                }
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert() {
        let mut kv: EdgeStoreWriter = EdgeStoreWriter::new(
            crate::gen_temp_path().join("test-segment"),
            Compression::default(),
            false,
        );

        let e = InnerEdge {
            from: FullNodeID {
                id: NodeID::from(0 as u64),
                prefix: NodeID::from(0 as u64),
            },
            to: FullNodeID {
                id: NodeID::from(1 as u64),
                prefix: NodeID::from(0 as u64),
            },
            label: "test".to_string(),
        };

        kv.put(e.clone());

        let store = kv.finalize();

        let edges: Vec<_> = store.get_with_label(&NodeID::from(0 as u64));

        assert_eq!(edges.len(), 1);
        assert_eq!(&edges[0], &Edge::from(e.clone()));

        let edges: Vec<_> = store.get_with_label(&NodeID::from(1 as u64));

        assert_eq!(edges.len(), 0);
    }

    #[test]
    fn test_reversed() {
        let mut kv: EdgeStoreWriter = EdgeStoreWriter::new(
            crate::gen_temp_path().join("test-segment"),
            Compression::default(),
            true,
        );

        let e = InnerEdge {
            from: FullNodeID {
                id: NodeID::from(0 as u64),
                prefix: NodeID::from(0 as u64),
            },
            to: FullNodeID {
                id: NodeID::from(1 as u64),
                prefix: NodeID::from(0 as u64),
            },
            label: "test".to_string(),
        };

        kv.put(e.clone());

        let store = kv.finalize();

        let edges: Vec<_> = store.get_with_label(&NodeID::from(0 as u64));
        assert_eq!(edges.len(), 0);

        let edges: Vec<_> = store.get_with_label(&NodeID::from(1 as u64));
        assert_eq!(edges.len(), 1);
        assert_eq!(&edges[0], &Edge::from(e.clone()));
    }
}
