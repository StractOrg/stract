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
    io::Write,
    path::{Path, PathBuf},
};

use super::{BlobId, BlobPointer};

pub struct BlobIndex {
    path: PathBuf,
    data: file_store::random_lookup::RandomLookup<BlobPointer>,
}

impl BlobIndex {
    pub fn open<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let data = file_store::random_lookup::RandomLookup::open(&path)?;

        Ok(Self {
            data,
            path: path.as_ref().to_path_buf(),
        })
    }

    pub fn file_name(uuid: uuid::Uuid) -> String {
        format!("{}.bid", uuid)
    }

    pub fn get(&self, id: BlobId) -> BlobPointer {
        self.data.get(id.0)
    }

    pub fn iter(&self) -> impl Iterator<Item = BlobPointer> + '_ {
        self.data.iter().map(|(_, v)| v)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub struct BlobIndexWriter<W>
where
    W: Write,
{
    wrt: file_store::random_lookup::RandomLookupWriter<BlobPointer, W>,
}

impl<W> BlobIndexWriter<W>
where
    W: Write,
{
    pub fn new(wrt: W) -> Self {
        Self {
            wrt: file_store::random_lookup::RandomLookupWriter::new(wrt),
        }
    }

    pub fn write(&mut self, ptr: &BlobPointer) -> Result<BlobId> {
        let id = self.wrt.write(ptr)?;
        Ok(BlobId(id))
    }

    pub fn finish(self) -> Result<()> {
        self.wrt.finish()?;
        Ok(())
    }
}
