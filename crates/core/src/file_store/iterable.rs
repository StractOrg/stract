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

//! This module contains the implementation of the `IterableStoreWriter` and `IterableStoreReader`.
//! The iterable store is a simple format for storing a sequence of items in a file such that they
//! can be read back in order. The format is as follows:
//!
//! 1. A 64-bit little-endian integer representing the number of bytes in the upcoming item.
//! 2. The serialized item.
//! 3. Repeat 1-2 until the end of the file.

use crate::Result;
use std::io::{self, Read, Write};

use super::Peekable;

struct IterableHeader {
    num_upcoming_bytes: u64,
}

impl IterableHeader {
    fn serialize<W>(&self, writer: &mut W) -> io::Result<()>
    where
        W: io::Write,
    {
        writer.write_all(&self.num_upcoming_bytes.to_le_bytes())
    }

    fn deserialize<R>(reader: &mut R) -> io::Result<Self>
    where
        R: io::Read,
    {
        let mut num_upcoming_bytes = [0; 8];
        reader.read_exact(&mut num_upcoming_bytes)?;
        Ok(IterableHeader {
            num_upcoming_bytes: u64::from_le_bytes(num_upcoming_bytes),
        })
    }
}

pub struct IterableStoreWriter<T, W>
where
    W: io::Write,
{
    writer: io::BufWriter<W>,
    _marker: std::marker::PhantomData<T>,
}

impl<T, W> IterableStoreWriter<T, W>
where
    T: bincode::Encode,
    W: io::Write,
{
    pub fn new(writer: W) -> Self {
        Self {
            writer: io::BufWriter::new(writer),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn write(&mut self, item: &T) -> Result<()> {
        let serialized = bincode::encode_to_vec(item, bincode::config::standard())?;
        let header = IterableHeader {
            num_upcoming_bytes: serialized.len() as u64,
        };
        header.serialize(&mut self.writer)?;
        self.writer.write_all(&serialized)?;
        Ok(())
    }

    pub fn finalize(mut self) -> Result<W> {
        self.writer.flush()?;

        self.writer.into_inner().map_err(|e| anyhow::anyhow!("{e}"))
    }
}

pub struct IterableStoreReader<T, R> {
    reader: io::BufReader<R>,
    _marker: std::marker::PhantomData<T>,
}

impl<T, R> IterableStoreReader<T, R>
where
    R: io::Read,
{
    pub fn new(reader: R) -> Self {
        Self {
            reader: io::BufReader::new(reader),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T, R> Iterator for IterableStoreReader<T, R>
where
    T: bincode::Decode,
    R: io::Read,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        let header = match IterableHeader::deserialize(&mut self.reader) {
            Ok(header) => header,
            Err(_err) => return None,
        };

        let mut serialized = vec![0; header.num_upcoming_bytes as usize];
        match self.reader.read_exact(&mut serialized) {
            Ok(()) => (),
            Err(err) => return Some(Err(err.into())),
        }

        Some(
            match bincode::decode_from_slice(&serialized, bincode::config::standard()) {
                Ok((item, _)) => Ok(item),
                Err(err) => Err(err.into()),
            },
        )
    }
}

pub struct SortedIterableStoreReader<T, R>
where
    R: io::Read,
    T: bincode::Decode,
{
    readers: Vec<Peekable<IterableStoreReader<T, R>>>,
}

impl<T, R> SortedIterableStoreReader<T, R>
where
    T: Ord + bincode::Decode,
    R: io::Read,
{
    pub fn new(readers: Vec<IterableStoreReader<T, R>>) -> Self {
        let readers = readers.into_iter().map(Peekable::new).collect::<Vec<_>>();

        Self { readers }
    }
}

impl<T, R> Iterator for SortedIterableStoreReader<T, R>
where
    T: Ord + bincode::Decode,
    R: io::Read,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut min_index = None;

        let num_readers = self.readers.len();
        for i in 0..num_readers {
            let reader = &self.readers[i];
            if let Some(item) = reader.peek() {
                match item {
                    Ok(item) => match min_index {
                        Some(cur_min) => {
                            let cur_min_reader: &Peekable<IterableStoreReader<T, R>> =
                                &self.readers[cur_min];

                            match cur_min_reader.peek().unwrap().as_ref() {
                                Ok(cur_min_item) => {
                                    if item < cur_min_item {
                                        min_index = Some(i);
                                    }
                                }
                                Err(err) => return Some(Err(anyhow::anyhow!("{err}"))),
                            }
                        }
                        None => min_index = Some(i),
                    },
                    Err(err) => return Some(Err(anyhow::anyhow!("{err}"))),
                }
            }
        }

        match min_index {
            Some(min_index) => Some(self.readers[min_index].next().unwrap()),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iterable_store() {
        let mut writer = IterableStoreWriter::new(Vec::new());
        writer.write(&1).unwrap();
        writer.write(&2).unwrap();
        writer.write(&3).unwrap();
        let writer = writer.finalize().unwrap();

        let reader = IterableStoreReader::new(writer.as_slice());

        let items: Vec<i32> = reader.map(|item| item.unwrap()).collect();
        assert_eq!(items, vec![1, 2, 3]);
    }

    #[test]
    fn test_sorted_iterable_store() {
        let mut writer1 = IterableStoreWriter::new(Vec::new());
        writer1.write(&1).unwrap();
        writer1.write(&3).unwrap();
        writer1.write(&5).unwrap();
        let writer1 = writer1.finalize().unwrap();

        let mut writer2 = IterableStoreWriter::new(Vec::new());
        writer2.write(&2).unwrap();
        writer2.write(&4).unwrap();
        writer2.write(&6).unwrap();
        let writer2 = writer2.finalize().unwrap();

        let reader1 = IterableStoreReader::new(writer1.as_slice());

        let reader2 = IterableStoreReader::new(writer2.as_slice());

        let reader = SortedIterableStoreReader::new(vec![reader1, reader2]);

        let items: Vec<i32> = reader.map(|item| item.unwrap()).collect();
        assert_eq!(items, vec![1, 2, 3, 4, 5, 6]);
    }
}
