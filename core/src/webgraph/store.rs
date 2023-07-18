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

use rocksdb::BlockBasedOptions;

pub struct Store<K, V> {
    db: rocksdb::DB,
    _phantom: std::marker::PhantomData<(K, V)>,
}

impl<K, V> Store<K, V>
where
    K: serde::de::DeserializeOwned + serde::Serialize,
    V: serde::de::DeserializeOwned + serde::Serialize,
{
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.increase_parallelism(8);
        options.set_write_buffer_size(256 * 1024 * 1024); // 256 MB memtable
        options.set_max_write_buffer_number(8);

        let mut block_options = BlockBasedOptions::default();
        block_options.set_bloom_filter(128.0, true);

        let cache = rocksdb::Cache::new_lru_cache(1024 * 1024 * 1024).unwrap(); // 1024 MB cache
        block_options.set_block_cache(&cache);

        block_options.set_block_size(128 * 1024); // 128 KB block size

        options.set_block_based_table_factory(&block_options);

        let db = rocksdb::DB::open(&options, path.as_ref().to_str().unwrap()).unwrap();

        Self {
            db,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let bytes = bincode::serialize(key).unwrap();

        let mut readopts = rocksdb::ReadOptions::default();
        readopts.set_readahead_size(4_194_304);

        self.db
            .get_pinned_opt(bytes, &readopts)
            .unwrap()
            .map(|bytes| bincode::deserialize(&bytes).unwrap())
    }

    pub fn put(&self, key: &K, value: &V) {
        let key_bytes = bincode::serialize(key).unwrap();
        let value_bytes = bincode::serialize(value).unwrap();

        self.db.put(key_bytes, value_bytes).unwrap();
    }

    pub fn batch_put<'a>(&'a self, it: impl Iterator<Item = (&'a K, &'a V)>) {
        let mut batch = rocksdb::WriteBatch::default();

        for (key, value) in it {
            let key_bytes = bincode::serialize(&key).unwrap();
            let value_bytes = bincode::serialize(&value).unwrap();

            batch.put(key_bytes, value_bytes);
        }

        self.db.write(batch).unwrap();
    }

    pub fn contains_key(&self, key: &K) -> bool {
        let bytes = bincode::serialize(key).unwrap();

        self.db.get(bytes).unwrap().is_some()
    }

    pub fn keys(&self) -> impl Iterator<Item = K> + '_ {
        let mut read_opts = rocksdb::ReadOptions::default();

        read_opts.set_readahead_size(4_194_304); // 4 MB

        self.db
            .iterator_opt(rocksdb::IteratorMode::Start, read_opts)
            .map(|res| {
                let (key, _) = res.unwrap();
                bincode::deserialize(&key).unwrap()
            })
    }

    pub fn values(&self) -> impl Iterator<Item = V> + '_ {
        let mut read_opts = rocksdb::ReadOptions::default();

        read_opts.set_readahead_size(4_194_304); // 4 MB

        self.db
            .iterator_opt(rocksdb::IteratorMode::Start, read_opts)
            .map(|res| {
                let (_, val) = res.unwrap();
                bincode::deserialize(&val).unwrap()
            })
    }

    pub fn iter(&self) -> impl Iterator<Item = (K, V)> + '_ {
        let mut read_opts = rocksdb::ReadOptions::default();

        read_opts.set_readahead_size(4_194_304); // 4 MB

        self.db
            .iterator_opt(rocksdb::IteratorMode::Start, read_opts)
            .map(|res| {
                let (key, val) = res.unwrap();
                (
                    bincode::deserialize(&key).unwrap(),
                    bincode::deserialize(&val).unwrap(),
                )
            })
    }

    pub fn flush(&self) {
        self.db.flush().unwrap();
    }

    pub fn remove(&self, key: &K) {
        let bytes = bincode::serialize(key).unwrap();
        self.db.delete(bytes).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize)]
    struct TestStruct {
        a: String,
        b: i32,
    }

    #[test]
    fn test_insert() {
        let kv = Store::<String, TestStruct>::open(crate::gen_temp_path().join("test-segment"));

        assert!(kv.get(&"test".to_string()).is_none());

        let test_struct = TestStruct {
            a: "test".to_string(),
            b: 5,
        };

        kv.put(&"test".to_string(), &test_struct);
        kv.flush();

        let archived = kv.get(&"test".to_string()).unwrap();
        assert_eq!(test_struct, archived);
    }

    #[test]
    fn test_re_open() {
        let path = crate::gen_temp_path();
        let segment_name = "test-segment";
        let test_struct = TestStruct {
            a: "test".to_string(),
            b: 5,
        };

        {
            let kv = Store::<String, TestStruct>::open(path.join(segment_name));

            assert!(kv.get(&"test".to_string()).is_none());

            kv.put(&"test".to_string(), &test_struct);
            kv.flush();
        }

        let kv = Store::<String, TestStruct>::open(path.join(segment_name));

        let archived = kv.get(&"test".to_string()).unwrap();
        assert_eq!(test_struct, archived);
    }
}
