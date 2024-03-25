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
// along with this program.  If not, see <https://www.gnu.org/license

use std::{collections::BTreeMap, io::Write, path::Path};

use super::{Node, NodeID};
use crate::Result;

#[derive(serde::Serialize, serde::Deserialize)]
struct StoredPtr {
    file_offset: usize,
    len: usize,
}

impl StoredPtr {
    fn range(&self) -> std::ops::Range<usize> {
        self.file_offset..self.file_offset + self.len
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Index {
    map: BTreeMap<NodeID, StoredPtr>,
}

impl Index {
    fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    fn open_or_new<P: AsRef<Path>>(path: P) -> Result<Self> {
        if path.as_ref().exists() {
            let file = std::fs::File::open(path)?;
            let reader = std::io::BufReader::new(file);

            Ok(bincode::deserialize_from(reader)?)
        } else {
            Ok(Self::new())
        }
    }

    fn save<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let file = std::fs::File::create(path)?;
        let writer = std::io::BufWriter::new(file);

        bincode::serialize_into(writer, self)?;

        Ok(())
    }

    fn insert(&mut self, id: NodeID, ptr: StoredPtr) {
        self.map.insert(id, ptr);
    }

    fn get(&self, id: &NodeID) -> Option<&StoredPtr> {
        self.map.get(id)
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn iter(&self) -> impl Iterator<Item = (&NodeID, &StoredPtr)> {
        self.map.iter()
    }

    fn contains(&self, id: &NodeID) -> bool {
        self.map.contains_key(id)
    }
}

struct FileStore {
    writer: std::io::BufWriter<std::fs::File>,
    mmap: memmap2::MmapMut,
    cur_offset: usize,
}

impl FileStore {
    fn open_or_new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.as_ref())?;
        let cur_offset = file.metadata()?.len() as usize;

        let mmap = unsafe { memmap2::MmapMut::map_mut(&file)? };

        Ok(Self {
            writer: std::io::BufWriter::new(file),
            mmap,
            cur_offset,
        })
    }

    fn write(&mut self, node: &Node) -> Result<StoredPtr> {
        let bytes = bincode::serialize(node)?;
        self.writer.write_all(&bytes)?;

        let file_offset = self.cur_offset;
        self.cur_offset += bytes.len();

        Ok(StoredPtr {
            file_offset,
            len: bytes.len(),
        })
    }

    fn read(&self, ptr: &StoredPtr) -> Result<Option<Node>> {
        let range = ptr.range();

        if range.start > self.mmap.len() || range.end > self.mmap.len() {
            return Ok(None);
        }

        let bytes = &self.mmap[range];
        let node = bincode::deserialize(bytes)?;
        Ok(Some(node))
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;

        self.mmap = unsafe { memmap2::MmapMut::map_mut(self.writer.get_ref())? };

        Ok(())
    }
}

pub struct Id2NodeDb {
    path: std::path::PathBuf,
    index: Index,
    store: FileStore,
}

impl Id2NodeDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        if !path.as_ref().exists() {
            std::fs::create_dir_all(&path).unwrap();
        }

        Self {
            path: path.as_ref().to_path_buf(),
            index: Index::open_or_new(path.as_ref().join("index.bin")).unwrap(),
            store: FileStore::open_or_new(path.as_ref().join("store.bin")).unwrap(),
        }
    }

    pub fn put(&mut self, id: &NodeID, node: &Node) {
        if self.index.contains(id) {
            return;
        }

        let ptr = self.store.write(node).unwrap();
        self.index.insert(*id, ptr);
    }

    pub fn get(&self, id: &NodeID) -> Option<Node> {
        match self.index.get(id) {
            Some(ptr) => self.store.read(ptr).unwrap(),
            None => None,
        }
    }

    pub fn keys(&self) -> impl Iterator<Item = NodeID> + '_ {
        self.index.iter().map(|(id, _)| *id)
    }

    pub fn estimate_num_keys(&self) -> usize {
        self.index.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (NodeID, Node)> + '_ {
        self.index
            .iter()
            .filter_map(move |(id, ptr)| self.store.read(ptr).unwrap().map(|node| (*id, node)))
    }

    pub fn batch_put(&mut self, iter: impl Iterator<Item = (NodeID, Node)>) {
        let mut num_inserts = 0;

        for (id, node) in iter {
            self.put(&id, &node);

            num_inserts += 1;
            if num_inserts >= 1000 {
                self.flush();
                num_inserts = 0;
            }
        }

        if num_inserts > 0 {
            self.flush();
        }
    }

    pub fn flush(&mut self) {
        self.index
            .save(self.path.as_path().join("index.bin"))
            .unwrap();
        self.store.flush().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gen_temp_path;

    #[test]
    fn test_id2node_db() {
        let mut db = Id2NodeDb::open(gen_temp_path());

        let a_node = Node::from("a".to_string());
        let a_id = NodeID::from(0 as u64);

        db.put(&a_id, &a_node);
        db.flush();

        assert_eq!(db.get(&a_id), Some(a_node.clone()));

        let b_node = Node::from("b".to_string());
        let b_id = NodeID::from(1 as u64);

        assert_eq!(db.get(&b_id), None);

        db.put(&b_id, &b_node);
        db.flush();

        assert_eq!(db.get(&b_id), Some(b_node));
        assert_eq!(db.get(&a_id), Some(a_node));
    }
}
