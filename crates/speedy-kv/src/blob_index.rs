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

use crate::Result;
use std::{
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use super::{BlobId, BlobPointer};

pub struct BlobIndex {
    path: PathBuf,
    bytes: memmap2::Mmap,
}

impl BlobIndex {
    pub fn open<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let bytes = unsafe { memmap2::Mmap::map(&std::fs::File::open(&path)?)? };

        Ok(Self {
            bytes,
            path: path.as_ref().to_path_buf(),
        })
    }

    pub fn file_name(uuid: uuid::Uuid) -> String {
        format!("{}.bid", uuid)
    }

    pub fn get(&self, id: BlobId) -> BlobPointer {
        let offset = id.0 as usize * BlobPointer::size();
        let bytes = self.bytes.as_ref();
        let bytes = &bytes[offset..offset + BlobPointer::size()];
        BlobPointer::from_bytes(bytes.try_into().unwrap())
    }

    pub fn iter(&self) -> impl Iterator<Item = BlobPointer> + '_ {
        BlobIndexIter::new(self.bytes.as_ref())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

struct BlobIndexIter<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> BlobIndexIter<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
}

impl<'a> Iterator for BlobIndexIter<'a> {
    type Item = BlobPointer;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.bytes.len() {
            return None;
        }

        let ptr = BlobPointer::from_bytes(
            self.bytes[self.offset..self.offset + BlobPointer::size()]
                .try_into()
                .unwrap(),
        );
        self.offset += BlobPointer::size();

        Some(ptr)
    }
}

pub struct BlobIndexWriter<W>
where
    W: Write,
{
    wrt: BufWriter<W>,
    _marker: std::marker::PhantomData<W>,
}

impl<W> BlobIndexWriter<W>
where
    W: Write,
{
    pub fn new(wrt: W) -> Self {
        Self {
            wrt: BufWriter::new(wrt),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn write(&mut self, ptr: &BlobPointer) -> Result<()> {
        self.wrt.write_all(&ptr.as_bytes())?;
        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        self.wrt.flush()?;
        Ok(())
    }
}
