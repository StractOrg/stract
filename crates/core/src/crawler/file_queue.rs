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
use postcard::experimental::max_size::MaxSize;
use std::{
    fs::File,
    io::{BufWriter, Read, Seek, Write},
    path::PathBuf,
};

const POINTER_KEY: &str = "pointer";
const DATA_KEY: &str = "data";

#[derive(Debug, Clone, Copy, MaxSize, serde::Serialize, serde::Deserialize)]
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
    T: bincode::Encode + bincode::Decode,
{
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        if !path.as_ref().exists() {
            std::fs::create_dir_all(path.as_ref())?;
        }

        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(path.as_ref().join(DATA_KEY))?;

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            writer: BufWriter::new(file),
            _marker: std::marker::PhantomData,
        })
    }

    pub fn push(&mut self, item: T) -> Result<()> {
        let body = bincode::encode_to_vec(&item, bincode::config::standard())?;
        let header = Header {
            body_size: body.len(),
        };

        let mut header_bytes = postcard::to_allocvec(&header).unwrap();

        if header_bytes.len() < Header::POSTCARD_MAX_SIZE {
            header_bytes.resize(Header::POSTCARD_MAX_SIZE, 0);
        }

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
            pointer: FilePointer::open(self.path)?,
            file: unsafe { Mmap::map(&file)? },
            _marker: std::marker::PhantomData,
        })
    }
}

struct FilePointer {
    file: File,
}

impl FilePointer {
    fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        if !path.as_ref().exists() {
            std::fs::create_dir_all(path.as_ref())?;
        }

        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(path.as_ref().join(POINTER_KEY))?;

        Ok(Self { file })
    }

    fn get(&mut self) -> usize {
        self.file.seek(std::io::SeekFrom::Start(0)).unwrap();
        let mut buf = [0u8; 8];

        if self.file.read_exact(&mut buf).is_err() {
            return 0;
        }

        usize::from_le_bytes(buf)
    }

    fn set(&mut self, pointer: usize) -> Result<()> {
        self.file.seek(std::io::SeekFrom::Start(0))?;
        self.file.write_all(&pointer.to_le_bytes())?;
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
    T: bincode::Encode + bincode::Decode,
{
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        if !path.as_ref().exists() {
            std::fs::create_dir_all(path.as_ref())?;
        }

        let file = File::open(path.as_ref().join(DATA_KEY))?;
        let file = unsafe { Mmap::map(&file)? };

        Ok(Self {
            pointer: FilePointer::open(path)?,
            file,
            _marker: std::marker::PhantomData,
        })
    }

    pub fn pop(&mut self) -> Result<Option<T>> {
        let cur_pointer = self.pointer.get();

        if cur_pointer >= self.file.len() {
            return Ok(None);
        }

        let header_size = Header::POSTCARD_MAX_SIZE;

        let header_bytes = &self.file[cur_pointer..cur_pointer + header_size];

        let header: Header = postcard::from_bytes(header_bytes).unwrap();

        let body =
            &self.file[cur_pointer + header_size..cur_pointer + header_size + header.body_size];
        let (item, _) = bincode::decode_from_slice(body, bincode::config::standard())?;

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
        let mut writer = FileQueueWriter::new(crate::gen_temp_path()).unwrap();

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

            let mut writer = FileQueueWriter::new(crate::gen_temp_path()).unwrap();

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
