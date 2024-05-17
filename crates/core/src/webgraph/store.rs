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

use std::{ops::Range, path::Path};

use file_store::{
    iterable::{ConstIterableStoreReader, IterableStoreReader},
    ConstSerializable,
};
use fst::Automaton;
use itertools::Itertools;

use super::{Compression, Edge, EdgeLimit, FullNodeID, NodeID};

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
    from_host: NodeID,
    to_host: NodeID,
    label: Vec<u8>,
}

pub struct HostDb {
    db: speedy_kv::Db<Vec<u8>, Vec<u8>>,
}

impl HostDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        let db = speedy_kv::Db::open_or_create(path).unwrap();

        Self { db }
    }

    fn optimize_read(&mut self) {
        self.db.merge_all_segments().unwrap();
    }

    pub fn insert(&mut self, node: &FullNodeID) {
        let key = [
            node.host.as_u64().to_le_bytes(),
            node.id.as_u64().to_le_bytes(),
        ]
        .concat();
        let value = vec![];

        self.db.insert_raw(key, value);
    }

    fn get(&self, host: &NodeID) -> Vec<NodeID> {
        let host = host.as_u64().to_le_bytes().to_vec();

        let query = speedy_kv::automaton::ExactMatch(&host).starts_with();

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

    pub fn flush(&mut self) {
        self.db.commit().unwrap();
        self.db.merge_all_segments().unwrap();
    }
}

pub struct RangesDb {
    nodes: speedy_kv::Db<NodeID, std::ops::Range<u64>>,
    labels: speedy_kv::Db<NodeID, std::ops::Range<u64>>,
}

impl RangesDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        let nodes = speedy_kv::Db::open_or_create(path.as_ref().join("nodes")).unwrap();
        let labels = speedy_kv::Db::open_or_create(path.as_ref().join("labels")).unwrap();

        Self { nodes, labels }
    }

    pub fn optimize_read(&mut self) {
        self.nodes.merge_all_segments().unwrap();
        self.labels.merge_all_segments().unwrap();
    }

    pub fn commit(&mut self) {
        self.nodes.commit().unwrap();
        self.labels.commit().unwrap();
    }

    pub fn nodes_get_raw<'a>(
        &'a self,
        key: &'a [u8],
    ) -> Option<speedy_kv::SerializedRef<'a, Range<u64>>> {
        self.nodes.get_raw(key)
    }

    pub fn labels_get_raw<'a>(
        &'a self,
        key: &'a [u8],
    ) -> Option<speedy_kv::SerializedRef<'a, Range<u64>>> {
        self.labels.get_raw(key)
    }

    pub fn insert_raw_node(&mut self, node: Vec<u8>, range: Vec<u8>) {
        self.nodes.insert_raw(node, range);
    }

    pub fn insert_raw_label(&mut self, node: Vec<u8>, range: Vec<u8>) {
        self.labels.insert_raw(node, range);
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
        let bytes = bincode::encode_to_vec(self, bincode::config::standard()).unwrap();
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
        let (res, _) = bincode::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        res
    }
}

pub struct EdgeStore {
    reversed: bool,
    ranges: RangesDb,
    hosts: HostDb,

    edge_labels: IterableStoreReader<CompressedLabelBlock>,
    edge_nodes: ConstIterableStoreReader<NodeID>,
}

impl EdgeStore {
    pub fn open<P: AsRef<Path>>(path: P, reversed: bool) -> Self {
        let ranges = RangesDb::open(path.as_ref().join("ranges"));

        let edge_labels = IterableStoreReader::open(path.as_ref().join("labels")).unwrap();

        let edge_nodes = ConstIterableStoreReader::open(path.as_ref().join("nodes")).unwrap();

        Self {
            ranges,
            hosts: HostDb::open(path.as_ref().join("hosts")),
            edge_labels,
            edge_nodes,
            reversed,
        }
    }

    pub fn optimize_read(&mut self) {
        self.ranges.optimize_read();
        self.hosts.optimize_read();
    }

    pub fn get_with_label(&self, node: &NodeID, limit: &EdgeLimit) -> Vec<Edge<String>> {
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

                let edge_labels = self
                    .edge_labels
                    .slice(edge_range)
                    .map(|r| r.unwrap().decompress())
                    .flat_map(|block| block.labels.into_iter());

                let edge_labels: Vec<_> = match limit {
                    EdgeLimit::Unlimited => edge_labels.collect(),
                    EdgeLimit::Limit(limit) => edge_labels.take(*limit).collect(),
                };

                let edge_nodes = self.edge_nodes.slice(node_range);

                let edge_nodes: Vec<_> = match limit {
                    EdgeLimit::Unlimited => edge_nodes.collect(),
                    EdgeLimit::Limit(limit) => edge_nodes.take(*limit).collect(),
                };

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

    pub fn get_without_label(&self, node: &NodeID, limit: &EdgeLimit) -> Vec<Edge<()>> {
        let node_bytes = node.as_u64().to_le_bytes();

        match self.ranges.nodes.get_raw(&node_bytes) {
            Some(node_range_bytes) => {
                let (node_range, _) = bincode::decode_from_slice::<Range<usize>, _>(
                    node_range_bytes.as_bytes(),
                    bincode::config::standard(),
                )
                .unwrap();

                let edge_nodes = self.edge_nodes.slice(node_range);

                let edge_nodes: Vec<_> = match limit {
                    EdgeLimit::Unlimited => edge_nodes.collect(),
                    EdgeLimit::Limit(limit) => edge_nodes.take(*limit).collect(),
                };

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

    pub fn nodes_by_host(&self, host: &NodeID) -> Vec<NodeID> {
        self.hosts.get(host)
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

            let edge_nodes = self.edge_nodes.slice(node_range).collect::<Vec<_>>();

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
    use std::sync::Arc;

    use crate::webgraph::{store_writer::EdgeStoreWriter, InnerEdge};

    use super::*;

    #[test]
    fn test_insert() {
        let mut kv: EdgeStoreWriter = EdgeStoreWriter::new(
            crate::gen_temp_path().join("test-segment"),
            Compression::default(),
            false,
            None,
        );

        let e = InnerEdge {
            from: FullNodeID {
                id: NodeID::from(0_u64),
                host: NodeID::from(0_u64),
            },
            to: FullNodeID {
                id: NodeID::from(1_u64),
                host: NodeID::from(0_u64),
            },
            label: "test".to_string(),
        };

        kv.put(e.clone());

        let store = kv.finalize();

        let edges: Vec<_> = store.get_with_label(&NodeID::from(0_u64), &EdgeLimit::Unlimited);

        assert_eq!(edges.len(), 1);
        assert_eq!(&edges[0], &Edge::from(e.clone()));

        let edges: Vec<_> = store.get_with_label(&NodeID::from(1_u64), &EdgeLimit::Unlimited);

        assert_eq!(edges.len(), 0);

        let edges = store.iter_without_label().collect::<Vec<_>>();

        assert_eq!(edges.len(), 1);
    }

    #[test]
    fn test_reversed() {
        let mut kv: EdgeStoreWriter = EdgeStoreWriter::new(
            crate::gen_temp_path().join("test-segment"),
            Compression::default(),
            true,
            None,
        );

        let e = InnerEdge {
            from: FullNodeID {
                id: NodeID::from(0_u64),
                host: NodeID::from(0_u64),
            },
            to: FullNodeID {
                id: NodeID::from(1_u64),
                host: NodeID::from(0_u64),
            },
            label: "test".to_string(),
        };

        kv.put(e.clone());

        let store = kv.finalize();

        let edges: Vec<_> = store.get_with_label(&NodeID::from(0_u64), &EdgeLimit::Unlimited);
        assert_eq!(edges.len(), 0);

        let edges: Vec<_> = store.get_with_label(&NodeID::from(1_u64), &EdgeLimit::Unlimited);
        assert_eq!(edges.len(), 1);
        assert_eq!(&edges[0], &Edge::from(e.clone()));
    }

    #[test]
    fn test_limit() {
        let mut kv: EdgeStoreWriter = EdgeStoreWriter::new(
            crate::gen_temp_path().join("test-segment"),
            Compression::default(),
            true,
            None,
        );

        for i in 0..10 {
            let e = InnerEdge {
                from: FullNodeID {
                    id: NodeID::from(i as u64),
                    host: NodeID::from(0_u64),
                },
                to: FullNodeID {
                    id: NodeID::from(1_u64),
                    host: NodeID::from(0_u64),
                },
                label: "test".to_string(),
            };

            kv.put(e.clone());
        }

        let store = kv.finalize();

        let edges: Vec<_> = store.get_with_label(&NodeID::from(1_u64), &EdgeLimit::Limit(5));
        assert_eq!(edges.len(), 5);
    }

    #[test]
    fn test_edge_ordering() {
        let mut rank_store =
            speedy_kv::Db::open_or_create(crate::gen_temp_path().join("test-rank-store")).unwrap();

        rank_store.insert(NodeID::from(2_u64), 1).unwrap();
        rank_store.insert(NodeID::from(3_u64), 2).unwrap();
        rank_store.insert(NodeID::from(1_u64), 3).unwrap();

        rank_store.commit().unwrap();

        let mut kv: EdgeStoreWriter = EdgeStoreWriter::new(
            crate::gen_temp_path().join("test-segment"),
            Compression::default(),
            true,
            Some(Arc::new(rank_store)),
        );

        let e1 = InnerEdge {
            from: FullNodeID {
                id: NodeID::from(1_u64),
                host: NodeID::from(1_u64),
            },
            to: FullNodeID {
                id: NodeID::from(0_u64),
                host: NodeID::from(0_u64),
            },
            label: "1".to_string(),
        };

        let e2 = InnerEdge {
            from: FullNodeID {
                id: NodeID::from(2_u64),
                host: NodeID::from(2_u64),
            },
            to: FullNodeID {
                id: NodeID::from(0_u64),
                host: NodeID::from(0_u64),
            },
            label: "2".to_string(),
        };

        let e3 = InnerEdge {
            from: FullNodeID {
                id: NodeID::from(3_u64),
                host: NodeID::from(3_u64),
            },
            to: FullNodeID {
                id: NodeID::from(0_u64),
                host: NodeID::from(0_u64),
            },
            label: "3".to_string(),
        };

        kv.put(e1.clone());
        kv.put(e2.clone());
        kv.put(e3.clone());

        let store = kv.finalize();

        let edges: Vec<_> = store.get_with_label(&NodeID::from(0_u64), &EdgeLimit::Unlimited);

        assert_eq!(edges.len(), 3);

        assert_eq!(&edges[0], &Edge::from(e2.clone()));
        assert_eq!(&edges[1], &Edge::from(e3.clone()));
        assert_eq!(&edges[2], &Edge::from(e1.clone()));
    }
}
