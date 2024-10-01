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
    fs::{File, OpenOptions},
    io::{Read, Seek, Write},
};

use crate::gen_temp_path;

pub struct TempDir {
    path: std::path::PathBuf,
}

impl TempDir {
    pub fn new() -> anyhow::Result<Self> {
        let path = gen_temp_path();

        std::fs::create_dir(&path)?;

        Ok(Self { path })
    }
}

impl AsRef<std::path::Path> for TempDir {
    fn as_ref(&self) -> &std::path::Path {
        self.path.as_ref()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        if self.path.exists() {
            if let Err(e) = std::fs::remove_dir_all(&self.path) {
                tracing::error!("Failed to remove temp dir: {e}");
            }
        }
    }
}

pub struct TempFile {
    inner: File,
    path: std::path::PathBuf,
}

impl TempFile {
    pub fn new(dir: &TempDir) -> anyhow::Result<Self> {
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

    pub fn inner_mut(&mut self) -> &mut File {
        &mut self.inner
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        if self.path.exists() {
            if let Err(e) = std::fs::remove_file(&self.path) {
                tracing::error!("Failed to remove temp file: {e}");
            }
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

impl Write for TempFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
