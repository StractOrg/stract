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
// along with this program.  If not, see <https://www.gnu.org/licenses/>

use std::{
    collections::BinaryHeap,
    io::{BufWriter, Write},
    ops::RangeBounds,
    path::{Path, PathBuf},
};

use file_store::Peekable;

use super::{
    blob_id_index::{BlobIdIndex, BlobIdIndexWriter},
    blob_index::{BlobIndex, BlobIndexWriter},
    blob_store::{BlobStore, BlobStoreWriter},
    Serialized, SerializedRef,
};
use crate::Result;

pub struct Writers<K, V, W>
where
    W: Write,
{
    pub id_index: BlobIdIndexWriter<K, W>,
    pub blob_index: BlobIndexWriter<W>,
    pub store: BlobStoreWriter<K, V, W>,
    pub bloom: W,
}

pub struct SegmentWriter<K, V, W>
where
    W: Write,
{
    writers: Writers<K, V, W>,
    bloom: bloom::BytesBloomFilter<Serialized<K>>,
}

impl<K, V, W> SegmentWriter<K, V, W>
where
    W: Write,
{
    pub fn new(num_items: usize, writers: Writers<K, V, W>) -> Self {
        let bloom = bloom::BytesBloomFilter::new(num_items as u64, 0.01);
        Self { writers, bloom }
    }
}

impl<K, V, W> SegmentWriter<K, V, W>
where
    W: Write,
{
    fn insert(&mut self, key: SerializedRef<'_, K>, value: SerializedRef<'_, V>) -> Result<()> {
        let ptr = self.writers.store.write(key, value)?;
        let id = self.writers.blob_index.write(&ptr)?;
        self.writers.id_index.insert(key.as_bytes(), &id)?;

        self.bloom.insert_raw(key.as_bytes());

        Ok(())
    }

    fn finish(self) -> Result<()> {
        self.writers.id_index.finish()?;
        self.writers.blob_index.finish()?;
        self.writers.store.finish()?;

        let mut wrt = BufWriter::new(self.writers.bloom);
        bincode::encode_into_std_write(self.bloom, &mut wrt, bincode::config::standard())?;
        wrt.flush()?;

        Ok(())
    }

    pub fn write_sorted_it<'a, I>(mut self, it: I) -> Result<()>
    where
        I: Iterator<Item = (SerializedRef<'a, K>, SerializedRef<'a, V>)>,
    {
        for (key, value) in it {
            self.insert(key, value)?;
        }

        self.finish()
    }
}

pub struct Segment<K, V> {
    id_index: BlobIdIndex<K>,
    blob_index: BlobIndex,
    store: BlobStore<K, V>,
    bloom: bloom::BytesBloomFilter<Serialized<K>>,

    folder: PathBuf,
    uuid: uuid::Uuid,
}

impl<K, V> Segment<K, V> {
    pub fn open<P: AsRef<Path>>(uuid: uuid::Uuid, folder: P) -> Result<Self> {
        let id_index = BlobIdIndex::open(folder.as_ref().join(BlobIdIndex::<K>::file_name(uuid)))?;
        let blob_index = BlobIndex::open(folder.as_ref().join(BlobIndex::file_name(uuid)))?;
        let store = BlobStore::open(folder.as_ref().join(BlobStore::<K, V>::file_name(uuid)))?;

        let mut bloom = std::fs::OpenOptions::new()
            .read(true)
            .open(folder.as_ref().join(Segment::<K, V>::bloom_file_name(uuid)))?;

        let bloom = bincode::decode_from_std_read(&mut bloom, bincode::config::standard())?;

        Ok(Self {
            uuid,
            id_index,
            blob_index,
            store,
            bloom,
            folder: folder.as_ref().to_path_buf(),
        })
    }

    pub fn bloom_file_name(uuid: uuid::Uuid) -> String {
        format!("{}.blm", uuid)
    }

    pub fn merge<P: AsRef<Path>>(
        segments: Vec<Segment<K, V>>,
        folder: P,
    ) -> Result<Option<Segment<K, V>>> {
        if segments.is_empty() {
            return Ok(None);
        }

        if segments.len() == 1 {
            return Ok(Some(segments.into_iter().next().unwrap()));
        }

        let uuid = uuid::Uuid::new_v4();

        let it = SortedSegments::new(
            segments
                .iter()
                .map(|s| Peekable::new(s.iter_raw()))
                .collect(),
        );

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

        let writers = Writers {
            id_index: BlobIdIndexWriter::new(id_index)?,
            blob_index: BlobIndexWriter::new(blob_index),
            store: BlobStoreWriter::new(store),
            bloom,
        };

        let num_items: usize = segments.iter().map(|s| s.len()).sum();

        SegmentWriter::new(num_items, writers).write_sorted_it(it)?;

        // cleanup old segments
        for segment in segments {
            std::fs::remove_file(segment.blob_index.path())?;
            std::fs::remove_file(segment.id_index.path())?;
            std::fs::remove_file(segment.store.path())?;
            std::fs::remove_file(segment.bloom_path())?;
        }

        Ok(Some(Segment::open(uuid, folder)?))
    }

    pub fn iter_raw(
        &self,
    ) -> impl Iterator<Item = (SerializedRef<'_, K>, SerializedRef<'_, V>)> + '_ {
        self.blob_index.iter().map(|ptr| {
            let blob = self.store.get_raw(&ptr).unwrap();
            (blob.key, blob.value)
        })
    }

    pub fn get_raw(&self, key: &[u8]) -> Result<Option<SerializedRef<'_, V>>> {
        if !self.bloom.contains_raw(key) {
            return Ok(None);
        }

        if let Some(id) = self.id_index.get(key) {
            let ptr = self.blob_index.get(id);
            let blob = self.store.get_raw(&ptr)?;
            Ok(Some(blob.value))
        } else {
            Ok(None)
        }
    }

    pub fn search_raw<'a, A>(
        &'a self,
        query: A,
    ) -> impl Iterator<Item = (SerializedRef<'a, K>, SerializedRef<'a, V>)> + 'a
    where
        A: fst::Automaton + 'a,
    {
        self.id_index.search(query).map(move |(_, id)| {
            let ptr = self.blob_index.get(id);
            let blob = self.store.get_raw(&ptr).unwrap();

            (blob.key, blob.value)
        })
    }

    pub fn range_raw<'a, R>(
        &'a self,
        range: R,
    ) -> impl Iterator<Item = (SerializedRef<'a, K>, SerializedRef<'a, V>)> + 'a
    where
        R: RangeBounds<SerializedRef<'a, K>>,
    {
        self.id_index.range(range).map(move |(_, id)| {
            let ptr = self.blob_index.get(id);
            let blob = self.store.get_raw(&ptr).unwrap();

            (blob.key, blob.value)
        })
    }

    pub fn uuid(&self) -> uuid::Uuid {
        self.uuid
    }

    pub fn len(&self) -> usize {
        self.id_index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.id_index.is_empty()
    }

    fn bloom_path(&self) -> PathBuf {
        self.folder.join(Self::bloom_file_name(self.uuid))
    }

    pub fn move_to<P: AsRef<Path>>(&mut self, new_folder: P) -> Result<()> {
        if !new_folder.as_ref().exists() {
            std::fs::create_dir_all(&new_folder)?;
        }

        std::fs::rename(
            self.blob_index.path(),
            new_folder
                .as_ref()
                .join(self.blob_index.path().file_name().unwrap()),
        )?;
        std::fs::rename(
            self.id_index.path(),
            new_folder
                .as_ref()
                .join(self.id_index.path().file_name().unwrap()),
        )?;
        std::fs::rename(
            self.store.path(),
            new_folder
                .as_ref()
                .join(self.store.path().file_name().unwrap()),
        )?;
        std::fs::rename(
            self.bloom_path(),
            new_folder.as_ref().join(Self::bloom_file_name(self.uuid)),
        )?;

        *self = Segment::open(self.uuid, new_folder)?;

        Ok(())
    }
}

struct SortedPeekable<'a, K, V, I>
where
    I: Iterator<Item = (SerializedRef<'a, K>, SerializedRef<'a, V>)>,
{
    segment_ord: usize,
    iter: Peekable<I>,
}

impl<'a, K, V, I> PartialOrd for SortedPeekable<'a, K, V, I>
where
    I: Iterator<Item = (SerializedRef<'a, K>, SerializedRef<'a, V>)>,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a, K, V, I> Ord for SortedPeekable<'a, K, V, I>
where
    I: Iterator<Item = (SerializedRef<'a, K>, SerializedRef<'a, V>)>,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self.iter.peek(), other.iter.peek()) {
            (Some((a, _)), Some((b, _))) => a
                .cmp(b)
                .reverse()
                .then_with(|| self.segment_ord.cmp(&other.segment_ord)),
            (Some(_), None) => std::cmp::Ordering::Greater,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (None, None) => std::cmp::Ordering::Equal,
        }
    }
}

impl<'a, K, V, I> PartialEq for SortedPeekable<'a, K, V, I>
where
    I: Iterator<Item = (SerializedRef<'a, K>, SerializedRef<'a, V>)>,
{
    fn eq(&self, other: &Self) -> bool {
        self.iter == other.iter && self.segment_ord == other.segment_ord
    }
}

impl<'a, K, V, I> Eq for SortedPeekable<'a, K, V, I> where
    I: Iterator<Item = (SerializedRef<'a, K>, SerializedRef<'a, V>)>
{
}

pub struct SortedSegments<'a, K, V, I>
where
    I: Iterator<Item = (SerializedRef<'a, K>, SerializedRef<'a, V>)>,
{
    segments: BinaryHeap<SortedPeekable<'a, K, V, I>>,
}

impl<'a, K, V, I> SortedSegments<'a, K, V, I>
where
    I: Iterator<Item = (SerializedRef<'a, K>, SerializedRef<'a, V>)>,
{
    pub fn new(segments: Vec<Peekable<I>>) -> Self {
        Self {
            segments: segments
                .into_iter()
                .enumerate()
                .map(|(segment_ord, iter)| SortedPeekable { segment_ord, iter })
                .collect(),
        }
    }
}

impl<'a, K, V, I> Iterator for SortedSegments<'a, K, V, I>
where
    I: Iterator<Item = (SerializedRef<'a, K>, SerializedRef<'a, V>)>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = {
            let mut min = self.segments.peek_mut()?;
            min.iter.next()?
        };

        // advance all segments with the same key
        while let Some(mut peek) = self.segments.peek_mut() {
            if peek.iter.peek().map(|(k, _)| k) != Some(&key) {
                break;
            }

            peek.iter.next();
        }

        Some((key, value))
    }
}
