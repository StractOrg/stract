// Stract is an open source web search engine.
// Copyright (C) 2023 Stract ApS
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

use itertools::Itertools;
use memmap2::Mmap;
use rocksdb::BlockBasedOptions;

use super::{Compression, Edge, EdgeLabel, FullNodeID, InnerEdge, NodeID};

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
struct SerializedEdge {
    from_prefix: NodeID,
    to_prefix: NodeID,
    label: Vec<u8>,
}

pub const MAX_BATCH_SIZE: usize = 100_000;

pub struct EdgeStoreWriter {
    reversed: bool,
    db: rocksdb::DB,
    compression: Compression,
}

impl EdgeStoreWriter {
    pub fn open<P: AsRef<Path>>(path: P, compression: Compression, reversed: bool) -> Self {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);

        options.set_max_background_jobs(8);
        options.increase_parallelism(8);
        options.set_max_subcompactions(8);

        options.set_allow_mmap_reads(true);
        options.set_allow_mmap_writes(true);
        options.set_write_buffer_size(128 * 1024 * 1024); // 128 MB
        options.set_target_file_size_base(512 * 1024 * 1024); // 512 MB
        options.set_target_file_size_multiplier(10);

        options.set_max_write_buffer_number(4);
        options.set_min_write_buffer_number_to_merge(1);
        options.set_level_zero_slowdown_writes_trigger(-1);
        options.set_level_zero_stop_writes_trigger(-1);

        // some recommended settings (https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning)
        options.set_level_compaction_dynamic_level_bytes(true);
        options.set_bytes_per_sync(1048576);
        let mut block_options = BlockBasedOptions::default();
        block_options.set_block_size(16 * 1024);
        block_options.set_format_version(5);
        block_options.set_cache_index_and_filter_blocks(true);
        block_options.set_pin_l0_filter_and_index_blocks_in_cache(true);

        options.set_block_based_table_factory(&block_options);
        options.set_compression_type(rocksdb::DBCompressionType::Lz4);

        let db = rocksdb::DB::open(&options, path.as_ref().join("writer")).unwrap();

        Self {
            db,
            reversed,
            compression,
        }
    }

    pub fn put<'a, L: EdgeLabel + 'a>(&'a self, edges: impl Iterator<Item = &'a InnerEdge<L>>) {
        let mut batch = rocksdb::WriteBatch::default();

        let mut opts = rocksdb::WriteOptions::default();
        opts.disable_wal(true);

        for edge in edges {
            let value_bytes = L::to_bytes(&edge.label).unwrap();

            let value_bytes = bincode::serialize(&SerializedEdge {
                from_prefix: edge.from.prefix,
                to_prefix: edge.to.prefix,
                label: value_bytes.clone(),
            })
            .unwrap();

            let key_bytes = if self.reversed {
                [
                    edge.to.id.as_u64().to_le_bytes(),
                    edge.from.id.as_u64().to_le_bytes(),
                ]
                .concat()
            } else {
                [
                    edge.from.id.as_u64().to_le_bytes(),
                    edge.to.id.as_u64().to_le_bytes(),
                ]
                .concat()
            };

            batch.put(key_bytes, value_bytes);

            if batch.len() >= MAX_BATCH_SIZE {
                self.db.write_opt(batch, &opts).unwrap();
                batch = rocksdb::WriteBatch::default();
            }
        }

        self.db.write_opt(batch, &opts).unwrap();
    }

    pub fn iter<L: EdgeLabel>(&self) -> impl Iterator<Item = InnerEdge<L>> + '_ + Send + Sync {
        let mut read_opts = rocksdb::ReadOptions::default();
        read_opts.set_verify_checksums(false);
        read_opts.set_async_io(true);

        self.db
            .iterator_opt(rocksdb::IteratorMode::Start, read_opts)
            .filter_map(|res| {
                let (key, val) = res.ok()?;

                let (from, to) = if self.reversed {
                    (
                        u64::from_le_bytes(key[u64::BITS as usize / 8..].try_into().unwrap()),
                        u64::from_le_bytes(key[..u64::BITS as usize / 8].try_into().unwrap()),
                    )
                } else {
                    (
                        u64::from_le_bytes(key[..u64::BITS as usize / 8].try_into().unwrap()),
                        u64::from_le_bytes(key[u64::BITS as usize / 8..].try_into().unwrap()),
                    )
                };

                let val: SerializedEdge = bincode::deserialize(&val).unwrap();

                Some(InnerEdge {
                    from: FullNodeID {
                        prefix: val.from_prefix,
                        id: NodeID::from(from),
                    },
                    to: FullNodeID {
                        prefix: val.to_prefix,
                        id: NodeID::from(to),
                    },
                    label: L::from_bytes(&val.label).unwrap(),
                })
            })
    }

    pub fn flush(&self) {
        self.db.flush().unwrap();
    }

    pub fn finalize(self) -> EdgeStore {
        let s = EdgeStore::build(
            self.db.path().parent().unwrap(),
            self.compression,
            self.reversed,
            self.iter(),
        );

        // delete the writer db
        let p = self.db.path().to_owned();
        drop(self.db);
        std::fs::remove_dir_all(p).unwrap();

        s
    }
}

struct PrefixDb {
    db: rocksdb::DB,
}

impl PrefixDb {
    fn open<P: AsRef<Path>>(path: P) -> Self {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);

        options.set_write_buffer_size(128 * 1024 * 1024); // 128 MB
        options.set_target_file_size_base(512 * 1024 * 1024); // 512 MB
        options.set_target_file_size_multiplier(10);

        options.set_max_write_buffer_number(4);
        options.set_min_write_buffer_number_to_merge(2);

        options.set_target_file_size_base(512 * 1024 * 1024); // 512 MB
        options.set_target_file_size_multiplier(10);

        options.set_allow_mmap_reads(true);
        options.set_allow_mmap_writes(true);

        options.set_level_zero_slowdown_writes_trigger(-1);
        options.set_level_zero_stop_writes_trigger(-1);
        options.set_compression_type(rocksdb::DBCompressionType::None);

        // some recommended settings (https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning)
        options.set_level_compaction_dynamic_level_bytes(true);
        options.set_bytes_per_sync(1048576);
        let mut block_options = BlockBasedOptions::default();
        block_options.set_block_size(16 * 1024);
        block_options.set_format_version(5);
        block_options.set_cache_index_and_filter_blocks(true);
        block_options.set_pin_l0_filter_and_index_blocks_in_cache(true);

        options.set_block_based_table_factory(&block_options);

        let db = rocksdb::DB::open(&options, path).unwrap();

        Self { db }
    }

    fn insert(&self, node: &FullNodeID) {
        let mut opts = rocksdb::WriteOptions::default();
        opts.disable_wal(true);

        let key = [
            node.prefix.as_u64().to_le_bytes(),
            node.id.as_u64().to_le_bytes(),
        ]
        .concat();
        let value = [];

        self.db.put_opt(key, value, &opts).unwrap();
    }

    fn get(&self, prefix: &NodeID) -> Vec<NodeID> {
        let start = [
            prefix.as_u64().to_le_bytes().to_vec(),
            [0].repeat(u64::BITS as usize / 8).to_vec(),
        ]
        .concat();

        let mut res = Vec::new();

        let mut opts = rocksdb::ReadOptions::default();
        opts.set_verify_checksums(false);
        opts.set_async_io(true);

        let iter = self.db.iterator_opt(
            rocksdb::IteratorMode::From(&start, rocksdb::Direction::Forward),
            opts,
        );

        for item in iter {
            let (key, _) = item.unwrap();

            let p = u64::from_le_bytes(key[..(u64::BITS as usize / 8)].try_into().unwrap());
            let node = u64::from_le_bytes(key[(u64::BITS as usize / 8)..].try_into().unwrap());

            if p != prefix.as_u64() {
                break;
            }

            res.push(NodeID::from(node));
        }

        res
    }

    fn flush(&self) {
        self.db.flush().unwrap();
    }
}

pub struct EdgeStore {
    reversed: bool,
    ranges: rocksdb::DB, // column[nodes] = full_nodeid -> (start, end); column[labels] = nodeid -> (start, end)
    prefixes: PrefixDb,
    _cache: rocksdb::Cache,

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
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);

        options.set_max_background_jobs(8);
        options.increase_parallelism(8);
        options.set_max_subcompactions(8);

        options.set_target_file_size_base(512 * 1024 * 1024); // 512 MB
        options.set_target_file_size_multiplier(10);

        options.set_allow_mmap_reads(true);
        options.set_allow_mmap_writes(true);

        options.set_level_zero_slowdown_writes_trigger(-1);
        options.set_level_zero_stop_writes_trigger(-1);

        let cache = rocksdb::Cache::new_lru_cache(256 * 1024 * 1024); // 256 mb

        // some recommended settings (https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning)
        options.set_level_compaction_dynamic_level_bytes(true);
        options.set_bytes_per_sync(1048576);
        let mut block_options = BlockBasedOptions::default();
        block_options.set_block_size(32 * 1024);
        block_options.set_format_version(5);
        block_options.set_cache_index_and_filter_blocks(true);
        block_options.set_pin_l0_filter_and_index_blocks_in_cache(true);
        block_options.set_block_cache(&cache);

        block_options.set_ribbon_filter(10.0);

        options.set_block_based_table_factory(&block_options);
        options.set_compression_type(rocksdb::DBCompressionType::None);
        options.optimize_for_point_lookup(512);

        let ranges = match rocksdb::DB::open_cf_with_opts(
            &options,
            path.as_ref().join("ranges"),
            [("nodes", options.clone()), ("labels", options.clone())],
        ) {
            Ok(db) => db,
            Err(_) => {
                let mut ranges = rocksdb::DB::open(&options, path.as_ref().join("ranges")).unwrap();

                ranges.create_cf("nodes", &options).unwrap();
                ranges.create_cf("labels", &options).unwrap();

                ranges
            }
        };

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
            _cache: cache,
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

        let node_cf = self.ranges.cf_handle("nodes").unwrap();
        let label_cf = self.ranges.cf_handle("labels").unwrap();

        debug_assert!(self.ranges.get_cf(node_cf, node_bytes).unwrap().is_none());
        debug_assert!(self.ranges.get_cf(label_cf, node_bytes).unwrap().is_none());

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

        let edge_labels_bytes = bincode::serialize(&edge_labels).unwrap();
        let edge_nodes_bytes = bincode::serialize(&edge_nodes).unwrap();

        let edge_labels_bytes = self.compression.compress(&edge_labels_bytes);
        let edge_nodes_bytes = self.compression.compress(&edge_nodes_bytes);

        let label_range = self.edge_labels_len..(self.edge_labels_len + edge_labels_bytes.len());
        let node_range = self.edge_nodes_len..(self.edge_nodes_len + edge_nodes_bytes.len());

        self.edge_labels_len += edge_labels_bytes.len();
        self.edge_nodes_len += edge_nodes_bytes.len();

        self.edge_labels_file.write_all(&edge_labels_bytes).unwrap();
        self.edge_nodes_file.write_all(&edge_nodes_bytes).unwrap();

        let mut opt = rocksdb::WriteOptions::default();
        opt.disable_wal(true);

        self.ranges
            .put_cf_opt(
                node_cf,
                node_bytes,
                bincode::serialize(&node_range).unwrap(),
                &opt,
            )
            .unwrap();

        self.ranges
            .put_cf_opt(
                label_cf,
                node_bytes,
                bincode::serialize(&label_range).unwrap(),
                &opt,
            )
            .unwrap();
    }

    /// Build a new edge store from a set of edges. The edges must be sorted by
    /// either the from or to node, depending on the value of `reversed`.
    fn build<P: AsRef<Path>>(
        path: P,
        compression: Compression,
        reversed: bool,
        edges: impl Iterator<Item = InnerEdge<String>>,
    ) -> Self {
        let mut s = Self::open(path, reversed, compression);

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

        self.ranges.flush().unwrap();
        self.ranges
            .flush_cf(self.ranges.cf_handle("nodes").unwrap())
            .unwrap();
        self.ranges
            .flush_cf(self.ranges.cf_handle("labels").unwrap())
            .unwrap();

        self.edge_nodes_file.flush().unwrap();
        self.edge_labels_file.flush().unwrap();

        self.edge_nodes = unsafe { Mmap::map(&self.edge_nodes_file).unwrap() };
        self.edge_labels = unsafe { Mmap::map(&self.edge_labels_file).unwrap() };

        self.edge_nodes_len = self.edge_nodes.len();
        self.edge_labels_len = self.edge_labels.len();
    }

    pub fn get_with_label(&self, node: &NodeID) -> Vec<Edge<String>> {
        let node_bytes = node.as_u64().to_le_bytes();

        let node_cf = self.ranges.cf_handle("nodes").unwrap();
        let edge_cf = self.ranges.cf_handle("labels").unwrap();

        let mut opts = rocksdb::ReadOptions::default();
        opts.set_verify_checksums(false);

        match (
            self.ranges.get_cf_opt(node_cf, node_bytes, &opts).unwrap(),
            self.ranges.get_cf_opt(edge_cf, node_bytes, &opts).unwrap(),
        ) {
            (Some(node_range_bytes), Some(edge_range_bytes)) => {
                let node_range = bincode::deserialize::<Range<usize>>(&node_range_bytes).unwrap();
                let edge_range = bincode::deserialize::<Range<usize>>(&edge_range_bytes).unwrap();

                let edge_labels = &self.edge_labels[edge_range];
                let edge_labels = self.compression.decompress(edge_labels);
                let edge_labels: Vec<_> = bincode::deserialize(&edge_labels).unwrap();

                let edge_nodes = &self.edge_nodes[node_range];
                let edge_nodes = self.compression.decompress(edge_nodes);
                let edge_nodes: Vec<_> = bincode::deserialize(&edge_nodes).unwrap();

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

        let node_cf = self.ranges.cf_handle("nodes").unwrap();

        let mut opts = rocksdb::ReadOptions::default();
        opts.set_verify_checksums(false);

        match self.ranges.get_cf_opt(node_cf, node_bytes, &opts).unwrap() {
            Some(node_range_bytes) => {
                let node_range = bincode::deserialize::<Range<usize>>(&node_range_bytes).unwrap();

                let edge_nodes = &self.edge_nodes[node_range];
                let edge_nodes = self.compression.decompress(edge_nodes);
                let edge_nodes: Vec<_> = bincode::deserialize(&edge_nodes).unwrap();

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
        let node_cf = self.ranges.cf_handle("nodes").unwrap();

        let mut opts = rocksdb::ReadOptions::default();
        opts.set_verify_checksums(false);
        opts.set_async_io(true);

        self.ranges
            .iterator_cf_opt(node_cf, opts, rocksdb::IteratorMode::Start)
            .flat_map(move |res| {
                let (key, val) = res.unwrap();

                let node = u64::from_le_bytes((*key).try_into().unwrap());
                let node = NodeID::from(node);

                let node_range = bincode::deserialize::<Range<usize>>(&val).unwrap();
                let edge_nodes = &self.edge_nodes[node_range];
                let edge_nodes = self.compression.decompress(edge_nodes);
                let edge_nodes: Vec<_> = bincode::deserialize(&edge_nodes).unwrap();

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
        let kv: EdgeStoreWriter = EdgeStoreWriter::open(
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

        kv.put([e.clone()].iter());

        let store = kv.finalize();

        let edges: Vec<_> = store.get_with_label(&NodeID::from(0 as u64));

        assert_eq!(edges.len(), 1);
        assert_eq!(&edges[0], &Edge::from(e.clone()));

        let edges: Vec<_> = store.get_with_label(&NodeID::from(1 as u64));

        assert_eq!(edges.len(), 0);
    }

    #[test]
    fn test_reversed() {
        let kv: EdgeStoreWriter = EdgeStoreWriter::open(
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

        kv.put([e.clone()].iter());

        let store = kv.finalize();

        let edges: Vec<_> = store.get_with_label(&NodeID::from(0 as u64));
        assert_eq!(edges.len(), 0);

        let edges: Vec<_> = store.get_with_label(&NodeID::from(1 as u64));
        assert_eq!(edges.len(), 1);
        assert_eq!(&edges[0], &Edge::from(e.clone()));
    }
}
