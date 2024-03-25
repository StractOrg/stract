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
// along with this program.  If not, see <https://www.gnu.org/license

use std::path::Path;

use super::{Node, NodeID};

pub struct Id2NodeDb {
    db: rocksdb::DB,
    _cache: rocksdb::Cache, // needs to be kept alive for as long as the db is alive
}

impl Id2NodeDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.optimize_for_point_lookup(512);

        opts.set_allow_mmap_reads(true);
        opts.set_allow_mmap_writes(true);
        opts.set_write_buffer_size(128 * 1024 * 1024); // 128 MB
        opts.set_target_file_size_base(512 * 1024 * 1024); // 512 MB
        opts.set_target_file_size_multiplier(10);

        opts.set_compression_type(rocksdb::DBCompressionType::None);

        let mut block_opts = rocksdb::BlockBasedOptions::default();
        let cache = rocksdb::Cache::new_lru_cache(1024 * 1024 * 1024); // 1 gb
        block_opts.set_ribbon_filter(10.0);

        // some recommended settings (https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning)
        opts.set_level_compaction_dynamic_level_bytes(true);
        opts.set_bytes_per_sync(1048576);

        block_opts.set_block_size(32 * 1024); // 32 kb
        block_opts.set_format_version(5);
        block_opts.set_cache_index_and_filter_blocks(true);
        block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
        block_opts.set_block_cache(&cache);

        opts.set_block_based_table_factory(&block_opts);

        let db = rocksdb::DB::open(&opts, path).unwrap();

        Self { db, _cache: cache }
    }

    pub fn put(&mut self, id: &NodeID, node: &Node) {
        let mut opts = rocksdb::WriteOptions::default();
        opts.disable_wal(true);

        self.db
            .put_opt(
                id.as_u64().to_le_bytes(),
                bincode::serialize(node).unwrap(),
                &opts,
            )
            .unwrap();
    }

    pub fn get(&self, id: &NodeID) -> Option<Node> {
        let mut opts = rocksdb::ReadOptions::default();
        opts.set_verify_checksums(false);

        self.db
            .get_opt(id.as_u64().to_le_bytes(), &opts)
            .unwrap()
            .map(|bytes| bincode::deserialize(&bytes).unwrap())
    }

    pub fn keys(&self) -> impl Iterator<Item = NodeID> + '_ {
        let mut opts = rocksdb::ReadOptions::default();
        opts.set_verify_checksums(false);
        opts.set_async_io(true);

        self.db
            .iterator_opt(rocksdb::IteratorMode::Start, opts)
            .filter_map(|r| {
                let (key, _) = r.ok()?;
                Some(NodeID::from(u64::from_le_bytes((*key).try_into().unwrap())))
            })
    }

    pub fn estimate_num_keys(&self) -> usize {
        self.db
            .property_int_value("rocksdb.estimate-num-keys")
            .ok()
            .flatten()
            .unwrap_or_default() as usize
    }

    pub fn iter(&self) -> impl Iterator<Item = (NodeID, Node)> + '_ {
        let mut opts = rocksdb::ReadOptions::default();
        opts.set_verify_checksums(false);

        self.db
            .iterator_opt(rocksdb::IteratorMode::Start, opts)
            .filter_map(|r| {
                let (key, value) = r.ok()?;

                Some((
                    NodeID::from(u64::from_le_bytes((*key).try_into().unwrap())),
                    bincode::deserialize(&value).unwrap(),
                ))
            })
    }

    pub fn batch_put(&mut self, iter: impl Iterator<Item = (NodeID, Node)>) {
        let mut batch = rocksdb::WriteBatch::default();
        let mut count = 0;

        for (id, node) in iter {
            batch.put(
                id.as_u64().to_le_bytes(),
                bincode::serialize(&node).unwrap(),
            );
            count += 1;

            if count > 10_000 {
                self.db.write(batch).unwrap();
                batch = rocksdb::WriteBatch::default();
                count = 0;
            }
        }

        if count > 0 {
            self.db.write(batch).unwrap();
        }
    }

    pub fn flush(&self) {
        self.db.flush().unwrap();
    }
}
