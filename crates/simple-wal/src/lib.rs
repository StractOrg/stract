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

use anyhow::Result;
use std::{
    fs::File,
    path::{Path, PathBuf},
};

pub struct Wal<T> {
    writer: file_store::iterable::IterableStoreWriter<T, File>,
    path: PathBuf,
}

impl<T> Wal<T> {
    pub fn open<P: AsRef<Path>>(file: P) -> Result<Self> {
        let path = file.as_ref().to_path_buf();
        let file = if file.as_ref().exists() {
            File::open(file)?
        } else {
            File::create(file)?
        };

        Ok(Wal {
            writer: file_store::iterable::IterableStoreWriter::new(file),
            path,
        })
    }

    pub fn clear(&mut self) -> Result<()> {
        std::fs::remove_file(&self.path)?;
        self.writer = file_store::iterable::IterableStoreWriter::new(File::create(&self.path)?);

        Ok(())
    }
}

impl<T> Wal<T>
where
    T: bincode::Encode,
{
    pub fn write(&mut self, item: &T) -> Result<()> {
        self.writer.write(item)?;
        self.writer.flush()?;

        Ok(())
    }
}

impl<T> Wal<T>
where
    T: bincode::Decode,
{
    pub fn iter(&self) -> Result<WalIterator<T>> {
        WalIterator::open(&self.path)
    }
}

pub struct WalIterator<T> {
    iter: file_store::iterable::IterableStoreReader<T>,
}

impl<T> WalIterator<T> {
    pub fn open<P: AsRef<Path>>(file: P) -> Result<Self> {
        Ok(Self {
            iter: file_store::iterable::IterableStoreReader::open(file)?,
        })
    }
}

impl<T> Iterator for WalIterator<T>
where
    T: bincode::Decode,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_write_read() -> Result<()> {
        let mut writer = Wal::open(file_store::gen_temp_path())?;

        writer.write(&1u64)?;
        writer.write(&2u64)?;
        writer.write(&3u64)?;

        let res: Vec<_> = writer.iter()?.collect();

        assert_eq!(&res, &[1, 2, 3]);

        writer.write(&4u64)?;

        let res: Vec<_> = writer.iter()?.collect();

        assert_eq!(&res, &[1, 2, 3, 4]);

        Ok(())
    }

    #[test]
    fn test_empty_write() -> Result<()> {
        let writer: Wal<u64> = Wal::open(file_store::gen_temp_path())?;

        let res: Vec<_> = writer.iter()?.collect();

        assert!(res.is_empty());

        Ok(())
    }

    #[test]
    fn test_clear() -> Result<()> {
        let mut writer = Wal::open(file_store::gen_temp_path())?;

        writer.write(&1u64)?;
        writer.write(&2u64)?;
        writer.write(&3u64)?;

        let res: Vec<_> = writer.iter()?.collect();

        assert_eq!(&res, &[1, 2, 3]);

        writer.clear()?;

        let res: Vec<_> = writer.iter()?.collect();
        assert!(res.is_empty());

        Ok(())
    }
}
