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

use super::{Edge, NodeID};

const MAX_BATCH_SIZE: usize = 1_000;

pub struct EdgeStore<L> {
    reversed: bool,
    db: rocksdb::DB,
    _phantom: std::marker::PhantomData<L>,
}

impl<L> EdgeStore<L>
where
    L: serde::Serialize + serde::de::DeserializeOwned + Send + Sync,
{
    pub fn open<P: AsRef<Path>>(path: P, reversed: bool) -> Self {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);

        let mut block_options = BlockBasedOptions::default();
        block_options.set_ribbon_filter(10.0);
        block_options.set_format_version(5);

        options.set_block_based_table_factory(&block_options);
        options.set_optimize_filters_for_hits(true);

        options.set_compression_type(rocksdb::DBCompressionType::Lz4);
        options.set_compaction_style(rocksdb::DBCompactionStyle::Universal);

        let db = rocksdb::DB::open(&options, path.as_ref().to_str().unwrap()).unwrap();

        Self {
            db,
            reversed,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn get(&self, node: NodeID) -> impl Iterator<Item = Edge<L>> + '_ {
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

            let label = bincode::deserialize(&value).ok()?;

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

    pub fn put(&self, edges: impl Iterator<Item = Edge<L>>) {
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

            if batch_keys.contains(&key_bytes) || self.db.get(&key_bytes).unwrap().is_some() {
                continue;
            }

            batch_keys.insert(key_bytes.clone());

            let value_bytes = bincode::serialize(&edge.label).unwrap();

            batch.put(key_bytes, value_bytes);

            if batch.len() >= MAX_BATCH_SIZE {
                self.db.write_opt(batch, &opts).unwrap();
                batch = rocksdb::WriteBatch::default();
                batch_keys.clear();
            }
        }

        self.db.write_opt(batch, &opts).unwrap();
    }

    pub fn iter(&self) -> impl Iterator<Item = Edge<L>> + '_ + Send + Sync {
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

                let label = bincode::deserialize(&val).unwrap();

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

    pub fn merge_with(&self, other: &EdgeStore<L>) {
        self.put(other.iter());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert() {
        let kv: EdgeStore<String> =
            EdgeStore::open(crate::gen_temp_path().join("test-segment"), false);

        let e = Edge {
            from: NodeID(0),
            to: NodeID(1),
            label: "test".to_string(),
        };

        kv.put(vec![e.clone()].into_iter());

        kv.flush();

        let edges: Vec<_> = kv.get(NodeID(0)).collect();

        assert_eq!(edges.len(), 1);
        assert_eq!(&edges[0], &e);

        let edges: Vec<_> = kv.get(NodeID(1)).collect();

        assert_eq!(edges.len(), 0);
    }

    #[test]
    fn test_reversed() {
        let kv: EdgeStore<String> =
            EdgeStore::open(crate::gen_temp_path().join("test-segment"), true);

        let e = Edge {
            from: NodeID(0),
            to: NodeID(1),
            label: "test".to_string(),
        };

        kv.put(vec![e.clone()].into_iter());

        kv.flush();

        let edges: Vec<_> = kv.get(NodeID(0)).collect();

        assert_eq!(edges.len(), 0);

        let edges: Vec<_> = kv.get(NodeID(1)).collect();

        assert_eq!(edges.len(), 1);
        assert_eq!(&edges[0], &e);
    }
}
