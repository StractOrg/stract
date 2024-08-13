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

//! On-disk key-value store designed for very read-heavy workloads without many small writes.
//! Lookups are O(k) where k is the size of the largest key.
//!
//! The design is loosely inspired by tantivy column store.

// TODO: When V is !Sized, there is no need to store the value in the blob store
// and therefore also not in the `BlobIndex`. This would save space.

// TODO: Handle the case when K is !Sized. I think this would currently crash fst mmap
// as one cannot mmap an empty file. All lookups should probably just return Ok(None) in this case.

use std::{
    collections::BTreeMap,
    ops::{Range, RangeBounds},
    path::{Path, PathBuf},
};

use self::{
    blob_id_index::{BlobIdIndex, BlobIdIndexWriter},
    blob_index::{BlobIndex, BlobIndexWriter},
    blob_store::{BlobStore, BlobStoreWriter},
    segment::{Segment, SegmentWriter},
};

type Result<T, E = anyhow::Error> = std::result::Result<T, E>;

pub mod automaton;
mod blob_id_index;
mod blob_index;
mod blob_store;
mod segment;
mod serialized;

use file_store::{ConstSerializable, Peekable};
use segment::SortedSegments;
pub use serialized::{Serialized, SerializedRef};

struct BlobPointer {
    key: Range<u64>,
    value: Range<u64>,
}

impl BlobPointer {
    const fn size() -> usize {
        std::mem::size_of::<BlobPointer>()
    }

    fn as_bytes(&self) -> [u8; BlobPointer::size()] {
        let mut res = [0; BlobPointer::size()];
        let mut offset = 0;

        for b in [
            self.key.start,
            self.key.end,
            self.value.start,
            self.value.end,
        ] {
            let bytes = b.to_le_bytes();

            res[offset..(bytes.len() + offset)].copy_from_slice(&bytes[..]);

            offset += bytes.len();
        }

        res
    }

    fn from_bytes(bytes: [u8; BlobPointer::size()]) -> Self {
        Self {
            key: Range {
                start: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                end: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            },
            value: Range {
                start: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
                end: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
            },
        }
    }
}

impl ConstSerializable for BlobPointer {
    const BYTES: usize = Self::size();

    fn serialize(&self, buf: &mut [u8]) {
        buf.copy_from_slice(&self.as_bytes());
    }

    fn deserialize(buf: &[u8]) -> Self {
        Self::from_bytes(buf.try_into().unwrap())
    }
}

#[derive(Clone, Copy)]
struct BlobId(file_store::random_lookup::ItemId);

#[derive(Debug)]
struct LiveSegment<K, V> {
    db: BTreeMap<Vec<u8>, Vec<u8>>,
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> Default for LiveSegment<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> LiveSegment<K, V> {
    fn new() -> Self {
        Self {
            db: BTreeMap::new(),
            _marker: std::marker::PhantomData,
        }
    }

    fn len(&self) -> usize {
        self.db.len()
    }

    fn insert_raw<SerializedKey, SerializedVal>(&mut self, key: SerializedKey, value: SerializedVal)
    where
        SerializedKey: Into<Serialized<K>>,
        SerializedVal: Into<Serialized<V>>,
    {
        let key: Serialized<K> = key.into();
        let value: Serialized<V> = value.into();

        self.db.insert(key.into(), value.into());
    }

    fn get_raw<'a, SerializedKey>(&'a self, key: SerializedKey) -> Option<SerializedRef<'a, V>>
    where
        SerializedKey: Into<SerializedRef<'a, K>>,
    {
        let key: SerializedRef<'a, K> = key.into();

        self.db
            .get(key.as_bytes())
            .map(|v| SerializedRef::from(v.as_slice()))
    }

    fn iter(&self) -> impl Iterator<Item = (SerializedRef<'_, K>, SerializedRef<'_, V>)> {
        self.db.iter().map(|(k, v)| {
            (
                SerializedRef::from(k.as_slice()),
                SerializedRef::from(v.as_slice()),
            )
        })
    }

    fn store<P: AsRef<Path>>(self, uuid: uuid::Uuid, folder: P) -> Result<Segment<K, V>> {
        if !folder.as_ref().exists() {
            std::fs::create_dir_all(&folder)?;
        }

        let id_index = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(folder.as_ref().join(BlobIdIndex::<K>::file_name(uuid)))?;

        let blob_index = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(folder.as_ref().join(BlobIndex::file_name(uuid)))?;

        let store = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(folder.as_ref().join(BlobStore::<K, V>::file_name(uuid)))?;

        let bloom = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(folder.as_ref().join(Segment::<K, V>::bloom_file_name(uuid)))?;

        let writers = segment::Writers {
            id_index: BlobIdIndexWriter::new(id_index)?,
            blob_index: BlobIndexWriter::new(blob_index),
            store: BlobStoreWriter::new(store),
            bloom,
        };

        SegmentWriter::new(self.db.len(), writers).write_sorted_it(self.iter())?;

        let res = Segment::open(uuid, folder)?;

        Ok(res)
    }
}

impl<K, V> LiveSegment<K, V>
where
    K: bincode::Encode,
    V: bincode::Encode,
{
    fn insert(&mut self, key: K, value: V) -> Result<()> {
        let key = Serialized::new(&key)?;
        let value = Serialized::new(&value)?;

        self.insert_raw(key, value);

        Ok(())
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct Meta {
    segments: Vec<uuid::Uuid>,
}

pub struct Db<K, V> {
    folder: PathBuf,
    meta: Meta,

    live_segment: LiveSegment<K, V>,

    segments: Vec<Segment<K, V>>,
}

impl<K, V> Db<K, V> {
    pub fn open_or_create<P: AsRef<Path>>(folder: P) -> Result<Self> {
        let folder = folder.as_ref().to_path_buf();
        if !folder.exists() {
            std::fs::create_dir_all(&folder)?;
        }

        if !folder.is_dir() {
            anyhow::bail!("Path is not a directory");
        }

        let meta_path = folder.join("meta.json");

        let meta = if meta_path.exists() {
            let meta = std::fs::read_to_string(&meta_path)?;
            serde_json::from_str(&meta)?
        } else {
            Meta::default()
        };

        let segments = meta
            .segments
            .iter()
            .map(|uuid| Segment::open(*uuid, &folder))
            .collect::<Result<Vec<_>>>()?;

        let s = Self {
            folder,
            meta,
            live_segment: LiveSegment::default(),
            segments,
        };

        s.save_meta()?;

        Ok(s)
    }

    pub fn folder(&self) -> &Path {
        &self.folder
    }

    pub fn uncommitted_inserts(&self) -> usize {
        self.live_segment.len()
    }

    fn save_meta(&self) -> Result<()> {
        let meta_path = self.folder.join("meta.json");
        let meta = serde_json::to_string_pretty(&self.meta)?;
        std::fs::write(meta_path, meta)?;
        Ok(())
    }

    pub fn merge_all_segments(&mut self) -> Result<()> {
        let segments = std::mem::take(&mut self.segments);

        if let Some(new) = Segment::merge(segments, &self.folder)? {
            self.meta.segments.clear();
            self.meta.segments.push(new.uuid());

            self.segments.push(new);

            self.save_meta()?;
        }

        Ok(())
    }

    pub fn insert_raw<SerializedKey, SerializedVal>(
        &mut self,
        key: SerializedKey,
        value: SerializedVal,
    ) where
        SerializedKey: Into<Serialized<K>>,
        SerializedVal: Into<Serialized<V>>,
    {
        self.live_segment.insert_raw(key, value);
    }

    pub fn get_raw_with_live<'a, SerializedKey>(
        &'a self,
        key: SerializedKey,
    ) -> Option<SerializedRef<'a, V>>
    where
        SerializedKey: Into<SerializedRef<'a, K>>,
    {
        let key: SerializedRef<'a, K> = key.into();
        if let Some(value) = self.live_segment.get_raw(key) {
            return Some(value);
        }

        self.segments
            .iter()
            .rev()
            .find_map(|segment| segment.get_raw(key.as_bytes()).ok().flatten())
    }

    pub fn get_raw<'a, SerializedKey>(&'a self, key: SerializedKey) -> Option<SerializedRef<'a, V>>
    where
        SerializedKey: Into<SerializedRef<'a, K>>,
    {
        let key: SerializedRef<'a, K> = key.into();

        self.segments
            .iter()
            .rev()
            .find_map(|segment| segment.get_raw(key.as_bytes()).ok().flatten())
    }

    pub fn search_raw<'a, A>(
        &'a self,
        query: A,
    ) -> impl Iterator<Item = (SerializedRef<'a, K>, SerializedRef<'a, V>)> + 'a
    where
        A: fst::Automaton + Clone + 'a,
    {
        self.segments
            .iter()
            .rev()
            .flat_map(move |segment| segment.search_raw(query.clone()))
    }

    pub fn range_raw<'a, R>(
        &'a self,
        range: R,
    ) -> impl Iterator<Item = (SerializedRef<'a, K>, SerializedRef<'a, V>)> + 'a
    where
        R: RangeBounds<SerializedRef<'a, K>> + Clone + 'a,
    {
        self.segments
            .iter()
            .rev()
            .flat_map(move |segment| segment.range_raw(range.clone()))
    }

    pub fn commit(&mut self) -> Result<()> {
        if self.live_segment.db.is_empty() {
            return Ok(());
        }

        let uuid = uuid::Uuid::new_v4();
        let live_segment = std::mem::take(&mut self.live_segment);
        let segment = live_segment.store(uuid, &self.folder)?;

        self.segments.push(segment);
        self.meta.segments.push(uuid);

        self.save_meta()?;

        Ok(())
    }

    pub fn merge(&mut self, other: Self) -> Result<()> {
        let other_folder = other.folder().to_path_buf();
        for mut segment in other.segments {
            segment.move_to(self.folder())?;
            self.segments.push(segment);
        }

        self.meta.segments = self.segments.iter().map(|s| s.uuid()).collect();
        self.save_meta()?;

        std::fs::remove_dir_all(other_folder)?;

        Ok(())
    }
}

impl<K, V> Db<K, V>
where
    K: Send + Sync,
    V: Send + Sync,
{
    pub fn iter_raw(
        &self,
    ) -> impl Iterator<Item = (SerializedRef<'_, K>, SerializedRef<'_, V>)> + '_ {
        self.segments.iter().flat_map(|segment| segment.iter_raw())
    }

    pub fn sorted_iter_raw(
        &self,
    ) -> impl Iterator<Item = (SerializedRef<'_, K>, SerializedRef<'_, V>)> + '_ {
        SortedSegments::new(
            self.segments
                .iter()
                .map(|s| Peekable::new(s.iter_raw()))
                .collect(),
        )
    }
}

impl<K, V> Db<K, V>
where
    K: bincode::Encode,
    V: bincode::Encode,
{
    pub fn insert(&mut self, key: K, value: V) -> Result<()> {
        self.live_segment.insert(key, value)?;
        Ok(())
    }
}

impl<K, V> Db<K, V>
where
    K: bincode::Encode,
    V: bincode::Decode,
{
    pub fn get(&self, key: &K) -> Result<Option<V>> {
        let key = bincode::encode_to_vec(key, common::bincode_config())?;

        match self.get_raw(key.as_slice()) {
            Some(v) => {
                let (v, _) = bincode::decode_from_slice(v.as_bytes(), common::bincode_config())?;
                Ok(Some(v))
            }
            None => Ok(None),
        }
    }
}
impl<K, V> Db<K, V>
where
    K: bincode::Decode,
    V: bincode::Decode,
{
    pub fn search<'a, A>(&'a self, query: A) -> impl Iterator<Item = (K, V)> + 'a
    where
        A: fst::Automaton + Clone + 'a,
    {
        self.search_raw(query).filter_map(|(k, v)| {
            let (k, _) = bincode::decode_from_slice(k.as_bytes(), common::bincode_config()).ok()?;
            let (v, _) = bincode::decode_from_slice(v.as_bytes(), common::bincode_config()).ok()?;

            Some((k, v))
        })
    }

    pub fn len(&self) -> usize {
        self.segments.iter().map(|s| s.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.segments.iter().all(|s| s.is_empty())
    }
}

impl<K, V> Db<K, V>
where
    K: bincode::Decode + Send + Sync,
    V: bincode::Decode + Send + Sync,
{
    pub fn iter(&self) -> impl Iterator<Item = (K, V)> + '_ {
        self.iter_raw().filter_map(|(k, v)| {
            let (k, _) = bincode::decode_from_slice(k.as_bytes(), common::bincode_config()).ok()?;
            let (v, _) = bincode::decode_from_slice(v.as_bytes(), common::bincode_config()).ok()?;

            Some((k, v))
        })
    }

    pub fn sorted_iter(&self) -> impl Iterator<Item = (K, V)> + '_ {
        self.sorted_iter_raw().filter_map(|(k, v)| {
            let (k, _) = bincode::decode_from_slice(k.as_bytes(), common::bincode_config()).ok()?;
            let (v, _) = bincode::decode_from_slice(v.as_bytes(), common::bincode_config()).ok()?;

            Some((k, v))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // taken from https://docs.rs/sled/0.34.7/src/sled/config.rs.html#445
    fn gen_temp_path() -> PathBuf {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::SystemTime;

        static SALT_COUNTER: AtomicUsize = AtomicUsize::new(0);

        let seed = SALT_COUNTER.fetch_add(1, Ordering::SeqCst) as u128;

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            << 48;

        let pid = u128::from(std::process::id());

        let salt = (pid << 16) + now + seed;

        if cfg!(target_os = "linux") {
            // use shared memory for temporary linux files
            format!("/dev/shm/pagecache.tmp.{salt}").into()
        } else {
            std::env::temp_dir().join(format!("pagecache.tmp.{salt}"))
        }
    }

    #[test]
    fn test_simple() {
        let mut db = Db::open_or_create(gen_temp_path()).unwrap();

        db.insert(1, 2).unwrap();
        db.insert(2, 3).unwrap();

        db.commit().unwrap();

        assert_eq!(db.get(&1).unwrap(), Some(2));
        assert_eq!(db.get(&2).unwrap(), Some(3));
    }

    #[test]
    fn test_multiple_segments() {
        let mut db = Db::open_or_create(gen_temp_path()).unwrap();

        db.insert(1, 2).unwrap();
        db.insert(2, 3).unwrap();

        db.commit().unwrap();

        db.insert(3, 4).unwrap();
        db.insert(4, 5).unwrap();

        db.commit().unwrap();

        assert_eq!(db.get(&1).unwrap(), Some(2));
        assert_eq!(db.get(&2).unwrap(), Some(3));
        assert_eq!(db.get(&3).unwrap(), Some(4));
        assert_eq!(db.get(&4).unwrap(), Some(5));
    }

    #[test]
    fn test_segment_merge() {
        let mut db = Db::open_or_create(gen_temp_path()).unwrap();

        db.insert(1, 2).unwrap();
        db.insert(2, 3).unwrap();

        db.commit().unwrap();

        db.insert(3, 4).unwrap();
        db.insert(4, 5).unwrap();

        db.commit().unwrap();

        db.insert(5, 6).unwrap();
        db.insert(6, 7).unwrap();

        db.commit().unwrap();
        db.merge_all_segments().unwrap();

        assert_eq!(db.get(&1).unwrap(), Some(2));
        assert_eq!(db.get(&2).unwrap(), Some(3));
        assert_eq!(db.get(&3).unwrap(), Some(4));
        assert_eq!(db.get(&4).unwrap(), Some(5));
        assert_eq!(db.get(&5).unwrap(), Some(6));
        assert_eq!(db.get(&6).unwrap(), Some(7));
    }

    #[test]
    fn test_overwrite_key() {
        let mut db = Db::open_or_create(gen_temp_path()).unwrap();

        db.insert(1, 2).unwrap();
        db.insert(1, 3).unwrap();

        db.commit().unwrap();

        assert_eq!(db.get(&1).unwrap(), Some(3));

        db.insert(1, 4).unwrap();
        db.commit().unwrap();

        assert_eq!(db.get(&1).unwrap(), Some(4));

        db.merge_all_segments().unwrap();

        assert_eq!(db.get(&1).unwrap(), Some(4));
    }

    #[test]
    fn test_len() {
        let mut db = Db::open_or_create(gen_temp_path()).unwrap();

        assert_eq!(db.len(), 0);

        db.insert(1, 2).unwrap();
        db.insert(2, 3).unwrap();

        assert_eq!(db.len(), 0);

        db.commit().unwrap();

        assert_eq!(db.len(), 2);
    }

    #[test]
    fn test_merge_db() {
        let mut db1 = Db::open_or_create(gen_temp_path()).unwrap();
        let mut db2 = Db::open_or_create(gen_temp_path()).unwrap();

        db1.insert(1, 2).unwrap();
        db1.insert(2, 3).unwrap();

        db1.commit().unwrap();

        db2.insert(3, 4).unwrap();
        db2.insert(4, 5).unwrap();

        db2.commit().unwrap();

        db1.merge(db2).unwrap();

        assert_eq!(db1.get(&1).unwrap(), Some(2));
        assert_eq!(db1.get(&2).unwrap(), Some(3));
        assert_eq!(db1.get(&3).unwrap(), Some(4));
        assert_eq!(db1.get(&4).unwrap(), Some(5));

        let path = db1.folder().to_path_buf();
        drop(db1);

        let db = Db::open_or_create(path).unwrap();

        assert_eq!(db.get(&1).unwrap(), Some(2));
        assert_eq!(db.get(&2).unwrap(), Some(3));
        assert_eq!(db.get(&3).unwrap(), Some(4));
        assert_eq!(db.get(&4).unwrap(), Some(5));
    }
}
