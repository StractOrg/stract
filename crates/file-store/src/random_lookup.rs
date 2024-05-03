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
// along with this program.  If not, see <https://www.gnu.org/licenses/

use std::{
    io::{self, Write},
    path::Path,
};

use crate::{owned_bytes::OwnedBytes, ConstSerializable};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ItemId(u64);

impl ItemId {
    pub fn into_inner(self) -> u64 {
        self.0
    }

    pub fn from_inner(inner: u64) -> Self {
        ItemId(inner)
    }
}

pub struct RandomLookupWriter<V, W>
where
    W: io::Write,
{
    next_id: u64,
    writer: io::BufWriter<W>,
    buf: Vec<u8>,
    _phantom: std::marker::PhantomData<V>,
}

impl<V, W> RandomLookupWriter<V, W>
where
    W: io::Write,
    V: ConstSerializable,
{
    pub fn new(writer: W) -> Self {
        RandomLookupWriter {
            next_id: 0,
            writer: io::BufWriter::new(writer),
            buf: Vec::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn write(&mut self, item: &V) -> io::Result<ItemId> {
        self.buf.resize(V::BYTES, 0);
        item.serialize(&mut self.buf);

        assert_eq!(self.buf.len(), V::BYTES);

        self.writer.write_all(&self.buf)?;

        let id = ItemId(self.next_id);
        self.next_id += 1;

        Ok(id)
    }

    pub fn finish(mut self) -> io::Result<W> {
        self.writer.flush()?;

        Ok(self.writer.into_inner()?)
    }
}

pub struct RandomLookup<V> {
    data: OwnedBytes,
    _phantom: std::marker::PhantomData<V>,
}

impl<V> RandomLookup<V>
where
    V: ConstSerializable,
{
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let data = OwnedBytes::mmap_from_path(path)?;

        Ok(RandomLookup {
            data,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Returns the value at the given item id.
    /// Panics if the item id is not from this store.
    pub fn get(&self, id: ItemId) -> V {
        let item_size: usize = V::BYTES;

        let start = id.0 as usize * item_size;

        V::deserialize(&self.data[start..start + V::BYTES])
    }

    pub fn iter(&self) -> impl Iterator<Item = (ItemId, V)> + '_ {
        RandomLookupIter {
            data: &self.data,
            next_id: 0,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V> From<OwnedBytes> for RandomLookup<V> {
    fn from(data: OwnedBytes) -> Self {
        RandomLookup {
            data,
            _phantom: std::marker::PhantomData,
        }
    }
}

pub struct RandomLookupIter<'a, V> {
    data: &'a OwnedBytes,
    next_id: u64,
    _phantom: std::marker::PhantomData<V>,
}

impl<'a, V> Iterator for RandomLookupIter<'a, V>
where
    V: ConstSerializable,
{
    type Item = (ItemId, V);

    fn next(&mut self) -> Option<Self::Item> {
        let item_size: usize = V::BYTES;
        let start = self.next_id as usize * item_size;

        if start >= self.data.len() {
            return None;
        }

        let id = ItemId::from_inner(self.next_id);

        self.next_id += 1;

        let val = V::deserialize(&self.data[start..start + V::BYTES]);

        Some((id, val))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple() {
        let mut writer = RandomLookupWriter::new(Vec::new());

        let value = 42u64;

        let id = writer.write(&value).unwrap();
        let bytes = writer.finish().unwrap();

        let store = RandomLookup::<u64>::from(OwnedBytes::new(bytes));

        let value2 = store.get(id);

        assert_eq!(value, value2);
    }

    #[test]
    fn test_iter() {
        let mut writer = RandomLookupWriter::new(Vec::new());

        let items = vec![1u64, 2, 3, 4, 5];

        for item in &items {
            writer.write(item).unwrap();
        }

        let bytes = writer.finish().unwrap();

        let store = RandomLookup::<u64>::from(OwnedBytes::new(bytes));

        let res = store.iter().map(|(_, val)| val).collect::<Vec<_>>();

        assert_eq!(items, res);
    }
}
