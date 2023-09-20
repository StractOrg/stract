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

use std::path::Path;

use hashbrown::HashSet;
use rocksdb::BlockBasedOptions;

use super::{Edge, EdgeLabel, NodeID};

const MAX_BATCH_SIZE: usize = 50_000;

pub struct EdgeStore {
    reversed: bool,
    dedup: bool,
    db: rocksdb::DB,
}

impl EdgeStore {
    pub fn open<P: AsRef<Path>>(path: P, reversed: bool, dedup: bool) -> Self {
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

        let db = rocksdb::DB::open(&options, path.as_ref().to_str().unwrap()).unwrap();

        Self {
            db,
            reversed,
            dedup,
        }
    }

    pub fn get<L: EdgeLabel>(&self, node: NodeID) -> impl Iterator<Item = Edge<L>> + '_ {
        let prefix_bytes = node.bit_128().to_be_bytes();
        let suffix = 0u128.to_be_bytes();

        let key_bytes = [prefix_bytes, suffix].concat();

        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            &key_bytes,
            rocksdb::Direction::Forward,
        ));

        iter.take_while(move |r| {
            if let Ok((key, _)) = r.as_ref() {
                let cur_prefix = &key[..16];

                cur_prefix == prefix_bytes
            } else {
                false
            }
        })
        .filter_map(move |r| {
            let (key, value) = r.ok()?;

            let suffix = u128::from_be_bytes(key[16..32].try_into().unwrap());

            let label = L::from_bytes(&value).ok()?;

            if self.reversed {
                Some(Edge {
                    from: NodeID(suffix),
                    to: node,
                    label,
                })
            } else {
                Some(Edge {
                    from: node,
                    to: NodeID(suffix),
                    label,
                })
            }
        })
    }

    pub fn put<'a, L: EdgeLabel + 'a>(&'a self, edges: impl Iterator<Item = &'a Edge<L>>) {
        let mut batch = rocksdb::WriteBatch::default();
        let mut batch_keys = HashSet::new();

        let mut opts = rocksdb::WriteOptions::default();
        opts.disable_wal(true);

        for edge in edges {
            let (prefix, suffix) = if self.reversed {
                (edge.to, edge.from)
            } else {
                (edge.from, edge.to)
            };

            let prefix_bytes = prefix.bit_128().to_be_bytes();
            let suffix_bytes = suffix.bit_128().to_be_bytes();

            let key_bytes = [prefix_bytes, suffix_bytes].concat();

            if self.dedup {
                if batch_keys.contains(&key_bytes) || self.db.get(&key_bytes).unwrap().is_some() {
                    continue;
                }

                batch_keys.insert(key_bytes.clone());
            }

            let value_bytes = edge.label.to_bytes().unwrap();

            batch.put(key_bytes, value_bytes);

            if batch.len() >= MAX_BATCH_SIZE {
                self.db.write_opt(batch, &opts).unwrap();
                batch = rocksdb::WriteBatch::default();
                batch_keys.clear();
            }
        }

        self.db.write_opt(batch, &opts).unwrap();
    }

    pub fn iter<L: EdgeLabel>(&self) -> impl Iterator<Item = Edge<L>> + '_ + Send + Sync {
        let read_opts = rocksdb::ReadOptions::default();

        self.db
            .iterator_opt(rocksdb::IteratorMode::Start, read_opts)
            .map(|res| {
                let (key, val) = res.unwrap();

                let (from, to) = if self.reversed {
                    let from = u128::from_be_bytes(key[16..32].try_into().unwrap());
                    let from = NodeID(from);

                    let to = u128::from_be_bytes(key[0..16].try_into().unwrap());
                    let to = NodeID(to);

                    (from, to)
                } else {
                    let from = u128::from_be_bytes(key[0..16].try_into().unwrap());
                    let from = NodeID(from);

                    let to = u128::from_be_bytes(key[16..32].try_into().unwrap());
                    let to = NodeID(to);

                    (from, to)
                };

                let label = L::from_bytes(&val).unwrap();

                Edge { from, to, label }
            })
    }

    pub fn flush(&self) {
        self.db.flush().unwrap();
    }

    pub fn estimate_len(&self) -> usize {
        self.db
            .property_int_value("rocksdb.estimate-num-keys")
            .unwrap()
            .unwrap_or(0) as usize
    }

    pub fn merge_with(&self, other: &EdgeStore) {
        let mut batch = Vec::with_capacity(MAX_BATCH_SIZE);

        for edge in other.iter::<String>() {
            batch.push(edge);

            if batch.len() >= MAX_BATCH_SIZE {
                self.put(batch.iter());
                batch.clear();
            }
        }

        self.put(batch.iter());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert() {
        let kv: EdgeStore =
            EdgeStore::open(crate::gen_temp_path().join("test-segment"), false, false);

        let e = Edge {
            from: NodeID(0),
            to: NodeID(1),
            label: "test".to_string(),
        };

        kv.put([e.clone()].iter());

        kv.flush();

        let edges: Vec<_> = kv.get(NodeID(0)).collect();

        assert_eq!(edges.len(), 1);
        assert_eq!(&edges[0], &e);

        let edges: Vec<Edge<String>> = kv.get(NodeID(1)).collect();

        assert_eq!(edges.len(), 0);
    }

    #[test]
    fn test_reversed() {
        let kv: EdgeStore =
            EdgeStore::open(crate::gen_temp_path().join("test-segment"), true, false);

        let e = Edge {
            from: NodeID(0),
            to: NodeID(1),
            label: "test".to_string(),
        };

        kv.put([e.clone()].iter());

        kv.flush();

        let edges: Vec<Edge<String>> = kv.get(NodeID(0)).collect();

        assert_eq!(edges.len(), 0);

        let edges: Vec<Edge<String>> = kv.get(NodeID(1)).collect();

        assert_eq!(edges.len(), 1);
        assert_eq!(&edges[0], &e);
    }
}
