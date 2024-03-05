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

use std::{fs, marker::PhantomData};

use rocksdb::{
    BlockBasedOptions, DBIteratorWithThreadMode, DBWithThreadMode, IteratorMode, Options,
    SingleThreaded, DB,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::kv::Kv;

pub struct RocksDbStore<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    db: DB,
    _cache: rocksdb::Cache,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> RocksDbStore<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    fn options(cache: &rocksdb::Cache) -> Options {
        let mut options = Options::default();
        options.create_if_missing(true);

        options.set_max_background_jobs(8);
        options.increase_parallelism(8);
        options.set_max_subcompactions(8);

        // some recommended settings (https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning)
        options.set_level_compaction_dynamic_level_bytes(true);
        options.set_bytes_per_sync(1048576);
        let mut block_options = BlockBasedOptions::default();
        block_options.set_block_size(1024 * 1024 * 1024); // 1 GB
        block_options.set_format_version(5);
        block_options.set_cache_index_and_filter_blocks(true);
        block_options.set_pin_l0_filter_and_index_blocks_in_cache(true);
        block_options.set_block_cache(cache);

        block_options.set_ribbon_filter(10.0);

        options.set_block_based_table_factory(&block_options);
        options.set_compression_type(rocksdb::DBCompressionType::Lz4);

        options.optimize_for_point_lookup(512); // 512 MB

        options
    }

    pub fn open_read_only<P>(path: P) -> Self
    where
        P: AsRef<std::path::Path>,
    {
        if !path.as_ref().exists() {
            fs::create_dir_all(path.as_ref()).expect("faild to create dir");
        }

        let cache = rocksdb::Cache::new_lru_cache(256 * 1024 * 1024); // 256 mb
        let options = Self::options(&cache);

        // create db to ensure it exists
        DB::open(&options, &path).expect("unable to open rocks db");

        let db = DB::open_for_read_only(&options, path, false).expect("unable to open rocks db");

        Self {
            db,
            _cache: cache,
            _phantom: PhantomData,
        }
    }

    pub fn open<P>(path: P) -> Self
    where
        P: AsRef<std::path::Path>,
    {
        if !path.as_ref().exists() {
            fs::create_dir_all(path.as_ref()).expect("faild to create dir");
        }

        let cache = rocksdb::Cache::new_lru_cache(256 * 1024 * 1024); // 256 mb
        let options = Self::options(&cache);

        let db = DB::open(&options, path).expect("unable to open rocks db");

        Self {
            db,
            _cache: cache,
            _phantom: PhantomData,
        }
    }
}

impl<K, V> Kv<K, V> for RocksDbStore<K, V>
where
    K: Serialize + DeserializeOwned + 'static + Send + Sync,
    V: Serialize + DeserializeOwned + 'static + Send + Sync,
{
    fn get_raw(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut opts = rocksdb::ReadOptions::default();
        opts.set_verify_checksums(false);

        self.db.get_opt(key, &opts).expect("failed to retrieve key")
    }

    fn insert_raw(&self, key: Vec<u8>, value: Vec<u8>) {
        let mut opt = rocksdb::WriteOptions::default();
        opt.disable_wal(true);

        self.db
            .put_opt(key, value, &opt)
            .expect("failed to insert value");
    }

    fn flush(&self) {
        if let Err(err) = self.db.flush() {
            match err.kind() {
                rocksdb::ErrorKind::NotSupported => {}
                _ => panic!("failed to flush: {err:?}"),
            }
        }
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (K, V)> + 'a> {
        let mut opts = rocksdb::ReadOptions::default();
        opts.set_verify_checksums(false);
        opts.set_async_io(true);

        let iter = self.db.iterator_opt(IteratorMode::Start, opts);

        Box::new(IntoIter {
            inner: iter,
            key: PhantomData,
            value: PhantomData,
        })
    }
}

pub struct IntoIter<'a, K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    inner: DBIteratorWithThreadMode<'a, DBWithThreadMode<SingleThreaded>>,
    key: PhantomData<K>,
    value: PhantomData<V>,
}

impl<'a, K, V> Iterator for IntoIter<'a, K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .and_then(|r| r.ok())
            .map(|(key_bytes, value_bytes)| {
                (
                    bincode::deserialize(&key_bytes).expect("Failed to deserialize key"),
                    bincode::deserialize(&value_bytes).expect("Failed to deserialize value"),
                )
            })
    }
}
