// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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

use crate::Result;
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

#[derive(Serialize, Deserialize, Debug)]
pub enum DirEntry {
    Folder {
        name: String,
        entries: Vec<DirEntry>,
    },
    File {
        name: String,
        content: Vec<u8>,
    },
}

fn iterate_children(path: &str) -> Result<Vec<DirEntry>> {
    dbg!(&path);
    let mut res = Vec::new();

    for f in fs::read_dir(path)? {
        let entry = dbg!(f?);
        let metadata = entry.metadata()?;

        if metadata.is_dir() {
            res.push(DirEntry::Folder {
                name: entry.path().as_os_str().to_str().unwrap().to_string(),
                entries: iterate_children(entry.path().as_os_str().to_str().unwrap())?,
            })
        } else if metadata.is_file() {
            res.push(DirEntry::File {
                name: entry.path().as_os_str().to_str().unwrap().to_string(),
                content: fs::read(entry.path())?,
            })
        }
    }

    Ok(res)
}

fn recreate_folder(entry: &DirEntry) -> Result<()> {
    match entry {
        DirEntry::Folder { name, entries } => {
            if Path::new(name).exists() {
                fs::remove_dir_all(name)?;
            }
            fs::create_dir(name)?;

            for entry in entries {
                recreate_folder(entry)?;
            }

            Ok(())
        }
        DirEntry::File { name, content } => Ok(fs::write(name, content)?),
    }
}

pub fn scan_folder(path: String) -> Result<DirEntry> {
    Ok(DirEntry::Folder {
        entries: iterate_children(&path)?,
        name: path,
    })
}

pub fn serialize(path: String) -> Result<Vec<u8>> {
    let folder = scan_folder(path)?;
    Ok(bincode::serialize(&folder)?)
}

pub fn deserialize(bytes: &[u8]) -> Result<String> {
    let entry = bincode::deserialize(bytes)?;
    recreate_folder(&entry)?;

    match entry {
        DirEntry::Folder { name, entries: _ } => Ok(name),
        DirEntry::File { name, content: _ } => Ok(name),
    }
}
