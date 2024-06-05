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

use anyhow::Result;
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, Write},
};

struct Chunk<T> {
    data: Vec<T>,
}

impl<T> Chunk<T> {
    fn new() -> Self {
        Self { data: Vec::new() }
    }

    fn push(&mut self, item: T) {
        self.data.push(item);
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn sort(&mut self)
    where
        T: Ord,
    {
        self.data.sort();
    }

    fn write_into(self, file: &mut File) -> Result<()>
    where
        T: bincode::Encode,
    {
        let mut wrt = BufWriter::new(file);

        for item in self.data {
            let bytes = bincode::encode_to_vec(&item, bincode::config::standard())?;
            let size = bytes.len() as u64;

            wrt.write_all(&size.to_le_bytes())?;
            wrt.write_all(&bytes)?;
        }

        wrt.flush()?;

        Ok(())
    }

    fn store(mut self, mut file: TempFile) -> Result<StoredChunk<T>>
    where
        T: bincode::Encode + Ord,
    {
        self.sort();
        self.write_into(&mut file.inner)?;

        StoredChunk::new(file)
    }
}

struct TempDir {
    path: std::path::PathBuf,
}

impl TempDir {
    fn new() -> Result<Self> {
        let path = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());

        std::fs::create_dir(&path)?;

        Ok(Self { path })
    }

    fn as_ref(&self) -> &std::path::Path {
        self.path.as_ref()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        if self.path.exists() {
            std::fs::remove_dir_all(&self.path).unwrap();
        }
    }
}

struct TempFile {
    inner: File,
    path: std::path::PathBuf,
}

impl TempFile {
    fn new(dir: &TempDir) -> Result<Self> {
        let path = dir.as_ref().join(uuid::Uuid::new_v4().to_string());

        Ok(Self {
            inner: OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)?,
            path,
        })
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        if self.path.exists() {
            std::fs::remove_file(&self.path).unwrap();
        }
    }
}

impl Read for TempFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

impl Seek for TempFile {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

struct StoredChunk<T> {
    buf: Vec<u8>,
    data: BufReader<TempFile>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> StoredChunk<T> {
    fn new(mut file: TempFile) -> Result<Self> {
        file.seek(std::io::SeekFrom::Start(0))?;
        Ok(Self {
            buf: Vec::new(),
            data: BufReader::new(file),
            _marker: std::marker::PhantomData,
        })
    }

    fn next(&mut self) -> Option<T>
    where
        T: bincode::Decode,
    {
        self.buf.clear();
        self.buf.resize(u64::BITS as usize / 8, 0);

        self.data.read_exact(&mut self.buf).ok()?;

        let next_size: u64 = u64::from_le_bytes(self.buf.as_slice().try_into().ok()?);

        self.buf.clear();
        self.buf.resize(next_size as usize, 0);

        self.data.read_exact(&mut self.buf).ok()?;

        let (next, _) = bincode::decode_from_slice(&self.buf, bincode::config::standard()).ok()?;

        Some(next)
    }
}

pub struct ExternalSorter<T> {
    chunk_size: usize,
    _marker: std::marker::PhantomData<T>,
}

impl Default for ExternalSorter<()> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ExternalSorter<T> {
    pub fn new() -> Self {
        Self {
            chunk_size: 1_000_000,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    pub fn sort<I>(self, iter: I) -> Result<SortedIterator<T>>
    where
        I: Iterator<Item = T>,
        T: bincode::Encode + bincode::Decode + Ord,
    {
        let dir = TempDir::new()?;
        let mut stored_chunks = Vec::new();
        let mut chunk = Chunk::new();

        for item in iter {
            chunk.push(item);

            if chunk.len() >= self.chunk_size {
                let file = TempFile::new(&dir)?;
                let stored_chunk = chunk.store(file)?;
                stored_chunks.push(stored_chunk);
                chunk = Chunk::new();
            }
        }

        if !chunk.is_empty() {
            let file = TempFile::new(&dir)?;
            let stored_chunk = chunk.store(file)?;
            stored_chunks.push(stored_chunk);
        }

        let heads = stored_chunks
            .into_iter()
            .filter_map(|chunk| Head::new(chunk))
            .map(Reverse)
            .collect::<BinaryHeap<_>>();

        Ok(SortedIterator {
            _dir: dir,
            chunks: heads,
        })
    }
}

struct Head<T> {
    item: T,
    rest: StoredChunk<T>,
}

impl<T> Head<T>
where
    T: bincode::Decode,
{
    fn new(mut chunk: StoredChunk<T>) -> Option<Self> {
        let item = chunk.next()?;

        Some(Self { item, rest: chunk })
    }
}

impl<T> Ord for Head<T>
where
    T: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.item.cmp(&other.item)
    }
}

impl<T> PartialOrd for Head<T>
where
    T: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Eq for Head<T> where T: Eq {}

impl<T> PartialEq for Head<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.item.eq(&other.item)
    }
}

impl<T> std::fmt::Debug for Head<T>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Head").field("item", &self.item).finish()
    }
}

pub struct SortedIterator<T> {
    // We need to keep the temp dir alive for the lifetime of the iterator
    _dir: TempDir,
    chunks: BinaryHeap<Reverse<Head<T>>>,
}

impl<T> Iterator for SortedIterator<T>
where
    T: bincode::Decode + Ord,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let mut head = self.chunks.pop()?.0;

        if let Some(next) = head.rest.next() {
            self.chunks.push(Reverse(Head {
                item: next,
                rest: head.rest,
            }));
        }

        Some(head.item)
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    #[test]
    fn test_external_sort() {
        let mut rng = rand::thread_rng();

        let mut expected = Vec::new();
        let mut data = Vec::new();

        for _ in 0..1_000_000 {
            let item = rng.gen_range(0..1_000_000);

            expected.push(item);
            data.push(item);
        }

        expected.sort_unstable();

        let sorted = ExternalSorter::new()
            .with_chunk_size(100_000)
            .sort(data.into_iter())
            .unwrap()
            .collect::<Vec<_>>();

        assert_eq!(sorted.len(), 1_000_000);
        assert_eq!(sorted, expected);
    }
}
