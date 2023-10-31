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
use memmap2::Mmap;
use std::{
    fs::File,
    io::{BufWriter, Read, Seek, Write},
    path::PathBuf,
};

const POINTER_KEY: &str = "pointer";
const DATA_KEY: &str = "data";

#[derive(Clone, Copy, serde::Serialize, serde::Deserialize)]
struct Header {
    body_size: usize,
}

pub struct FileQueueWriter<T> {
    path: PathBuf,
    writer: BufWriter<File>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> FileQueueWriter<T>
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    pub fn new(path: &std::path::Path) -> Result<Self> {
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path.join(DATA_KEY))?;

        Ok(Self {
            path: path.to_path_buf(),
            writer: BufWriter::new(file),
            _marker: std::marker::PhantomData,
        })
    }

    pub fn push(&mut self, item: T) -> Result<()> {
        let body = bincode::serialize(&item)?;
        let header = Header {
            body_size: body.len(),
        };

        let header_bytes = bincode::serialize(&header)?;

        self.writer.write_all(&header_bytes)?;
        self.writer.write_all(&body)?;

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;

        Ok(())
    }

    pub fn finalize(mut self) -> Result<FileQueue<T>> {
        self.flush()?;

        let file = self.writer.into_inner()?;

        Ok(FileQueue {
            pointer: FilePointer::new(&self.path)?,
            file: unsafe { Mmap::map(&file)? },
            _marker: std::marker::PhantomData,
        })
    }
}

struct FilePointer {
    file: File,
}

impl FilePointer {
    fn new(path: &std::path::Path) -> Result<Self> {
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path.join(POINTER_KEY))?;

        Ok(Self { file })
    }

    fn get(&mut self) -> usize {
        self.file.seek(std::io::SeekFrom::Start(0)).unwrap();
        let mut buf = [0u8; 8];

        if self.file.read_exact(&mut buf).is_err() {
            return 0;
        }

        usize::from_be_bytes(buf)
    }

    fn set(&mut self, pointer: usize) -> Result<()> {
        self.file.seek(std::io::SeekFrom::Start(0))?;
        self.file.write_all(&pointer.to_be_bytes())?;
        self.file.flush()?;

        Ok(())
    }
}

pub struct FileQueue<T> {
    pointer: FilePointer,
    file: Mmap,
    _marker: std::marker::PhantomData<T>,
}

impl<T> FileQueue<T>
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    pub fn new(path: &std::path::Path) -> Result<Self> {
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

        let file = File::open(path.join(DATA_KEY))?;
        let file = unsafe { Mmap::map(&file)? };

        Ok(Self {
            pointer: FilePointer::new(path)?,
            file,
            _marker: std::marker::PhantomData,
        })
    }

    pub fn pop(&mut self) -> Result<Option<T>> {
        let cur_pointer = self.pointer.get();

        if cur_pointer >= self.file.len() {
            return Ok(None);
        }

        let header_size = std::mem::size_of::<Header>();
        let header: Header =
            bincode::deserialize(&self.file[cur_pointer..cur_pointer + header_size])?;

        let body =
            &self.file[cur_pointer + header_size..cur_pointer + header_size + header.body_size];
        let item = bincode::deserialize(body)?;

        self.pointer
            .set(cur_pointer + header_size + header.body_size)?;

        Ok(Some(item))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn simple() {
        let mut writer = FileQueueWriter::new(&crate::gen_temp_path()).unwrap();

        writer.push("Hello".to_string()).unwrap();
        writer.push("World".to_string()).unwrap();

        let mut queue = writer.finalize().unwrap();

        assert_eq!(queue.pop().unwrap().unwrap(), "Hello");
        assert_eq!(queue.pop().unwrap().unwrap(), "World");
        assert_eq!(queue.pop().unwrap(), None);
    }

    proptest! {
        #[test]
        fn prop(data: Vec<String>) {
            let expected = data.clone();

            let mut writer = FileQueueWriter::new(&crate::gen_temp_path()).unwrap();

            for item in data {
                writer.push(item).unwrap();
            }

            let mut queue = writer.finalize().unwrap();
            let mut actual = Vec::new();

            while let Some(item) = queue.pop().unwrap() {
                actual.push(item);
            }

            prop_assert_eq!(actual, expected);
        }
    }
}
