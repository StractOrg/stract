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

use super::{BlobPointer, SerializedRef};

pub struct Blob<'a, K, V> {
    pub key: SerializedRef<'a, K>,
    pub value: SerializedRef<'a, V>,
}

pub struct BlobStore<K, V> {
    path: PathBuf,
    bytes: Option<memmap::Mmap>,
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> BlobStore<K, V> {
    pub fn open<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let bytes = unsafe { memmap::Mmap::map(&std::fs::File::open(path.as_ref())?).ok() };

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            bytes,
            _marker: std::marker::PhantomData,
        })
    }

    pub fn file_name(uuid: uuid::Uuid) -> String {
        format!("{}.blobs", uuid)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn get_bytes<T>(&self, range: &std::ops::Range<u64>) -> SerializedRef<'_, T> {
        let bytes = match self.bytes.as_ref() {
            Some(bytes) => bytes.as_ref(),
            None => [].as_ref(),
        };

        if range.end as usize > bytes.len() || range.start as usize > bytes.len() {
            panic!("Blob pointer out of bounds");
        }

        let bytes = &bytes[range.start as usize..range.end as usize];

        bytes.into()
    }

    pub fn get_raw(&self, ptr: &BlobPointer) -> Result<Blob<'_, K, V>> {
        let key = self.get_bytes(&ptr.key);
        let value = self.get_bytes(&ptr.value);

        Ok(Blob { key, value })
    }
}

pub struct BlobStoreWriter<K, V, W>
where
    W: Write,
{
    wrt: BufWriter<W>,
    offset: u64,
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V, W> BlobStoreWriter<K, V, W>
where
    W: Write,
{
    pub fn new(wrt: W) -> Self {
        Self {
            wrt: BufWriter::new(wrt),
            offset: 0,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn write(
        &mut self,
        key: SerializedRef<'_, K>,
        value: SerializedRef<'_, V>,
    ) -> Result<BlobPointer> {
        let key_range = self.offset..self.offset + key.as_bytes().len() as u64;
        self.wrt.write_all(key.as_bytes())?;
        self.offset += key.as_bytes().len() as u64;

        let value_range = self.offset..self.offset + value.as_bytes().len() as u64;
        self.wrt.write_all(value.as_bytes())?;
        self.offset += value.as_bytes().len() as u64;

        Ok(BlobPointer {
            key: key_range,
            value: value_range,
        })
    }

    pub fn finish(mut self) -> Result<()> {
        self.wrt.flush()?;
        Ok(())
    }
}
