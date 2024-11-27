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

use fst::{IntoStreamer, Streamer};

use crate::Result;
use std::{
    io::{BufWriter, Write},
    ops::RangeBounds,
    path::{Path, PathBuf},
};

use super::{BlobId, Serialized, SerializedRef};

pub struct BlobIdIndex<K> {
    path: PathBuf,
    fst: fst::Map<memmap2::Mmap>,
    _marker: std::marker::PhantomData<K>,
}

impl<K> BlobIdIndex<K> {
    pub fn open<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let mmap = unsafe { memmap2::Mmap::map(&std::fs::File::open(&path)?)? };

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            fst: fst::Map::new(mmap)?,
            _marker: std::marker::PhantomData,
        })
    }

    pub fn file_name(uuid: uuid::Uuid) -> String {
        format!("{}.ids", uuid)
    }

    pub fn get(&self, key: &[u8]) -> Option<BlobId> {
        let mut stream = self
            .fst
            .search(super::automaton::ExactMatch(key))
            .into_stream();

        stream
            .next()
            .map(|(_, v)| BlobId(file_store::random_lookup::ItemId::from_inner(v)))
    }

    pub fn search<'a, A>(
        &'a self,
        automaton: A,
    ) -> impl Iterator<Item = (Serialized<K>, BlobId)> + 'a
    where
        A: fst::Automaton + 'a,
    {
        BlobIdIndexIter::new(self.fst.search(automaton).into_stream())
    }

    pub fn range<'a, R>(&'a self, range: R) -> impl Iterator<Item = (Serialized<K>, BlobId)> + 'a
    where
        R: RangeBounds<SerializedRef<'a, K>>,
    {
        let builder = self.fst.range();

        let builder = match range.start_bound() {
            std::ops::Bound::Included(s) => builder.ge(s),
            std::ops::Bound::Excluded(s) => builder.gt(s),
            std::ops::Bound::Unbounded => builder,
        };

        let builder = match range.end_bound() {
            std::ops::Bound::Included(e) => builder.le(e),
            std::ops::Bound::Excluded(e) => builder.lt(e),
            std::ops::Bound::Unbounded => builder,
        };

        BlobIdIndexIter::new(builder.into_stream())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn len(&self) -> usize {
        self.fst.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fst.is_empty()
    }
}

struct BlobIdIndexIter<'a, K, A>
where
    A: fst::Automaton,
{
    stream: fst::map::Stream<'a, A>,
    _marker: std::marker::PhantomData<K>,
}

impl<'a, K, A> BlobIdIndexIter<'a, K, A>
where
    A: fst::Automaton,
{
    fn new(stream: fst::map::Stream<'a, A>) -> Self {
        Self {
            stream,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<K, A> Iterator for BlobIdIndexIter<'_, K, A>
where
    A: fst::Automaton,
{
    type Item = (Serialized<K>, BlobId);

    fn next(&mut self) -> Option<Self::Item> {
        self.stream.next().map(|(k, v)| {
            (
                k.to_vec().into(),
                BlobId(file_store::random_lookup::ItemId::from_inner(v)),
            )
        })
    }
}

pub struct BlobIdIndexWriter<K, W>
where
    W: Write,
{
    fst_builder: fst::MapBuilder<BufWriter<W>>,
    _marker: std::marker::PhantomData<K>,
}

impl<K, W> BlobIdIndexWriter<K, W>
where
    W: Write,
{
    pub fn new(wrt: W) -> Result<Self> {
        let fst_builder = fst::MapBuilder::new(BufWriter::new(wrt))?;

        Ok(Self {
            fst_builder,
            _marker: std::marker::PhantomData,
        })
    }

    pub fn insert(&mut self, key: &[u8], id: &BlobId) -> Result<()> {
        self.fst_builder.insert(key, id.0.into_inner())?;

        Ok(())
    }

    pub fn finish(self) -> Result<()> {
        self.fst_builder.finish()?;
        Ok(())
    }
}
