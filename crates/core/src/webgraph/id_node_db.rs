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

use std::path::Path;

use super::{Node, NodeID};
use crate::Result;

pub struct Id2NodeDb {
    db: speedy_kv::Db<NodeID, Node>,
}

impl Id2NodeDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Self {
            db: speedy_kv::Db::open_or_create(path)?,
        })
    }

    pub fn put(&mut self, id: &NodeID, node: &Node) -> Result<()> {
        self.db.insert(*id, node.clone())?;
        Ok(())
    }

    pub fn get(&self, id: &NodeID) -> Result<Option<Node>> {
        self.db.get(id)
    }

    pub fn keys(&self) -> impl Iterator<Item = NodeID> + '_ {
        self.db.iter().map(|(id, _)| id)
    }

    pub fn iter_with_offset(&self, offset: u64) -> impl Iterator<Item = (NodeID, Node)> + '_ {
        self.db.iter_with_offset(offset)
    }

    pub fn estimate_num_keys(&self) -> usize {
        self.db.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (NodeID, Node)> + '_ {
        self.db.iter()
    }

    pub fn merge(&mut self, other: Self) -> Result<()> {
        self.db.merge(other.db)?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.db.commit()?;
        Ok(())
    }

    pub fn optimize_read(&mut self) -> Result<()> {
        self.db.merge_all_segments()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id2node_db() {
        let temp_dir = crate::gen_temp_dir().unwrap();
        let mut db = Id2NodeDb::open(&temp_dir).unwrap();

        let a_node = Node::from("a".to_string());
        let a_id = NodeID::from(0_u64);

        db.put(&a_id, &a_node).unwrap();
        db.flush().unwrap();

        assert_eq!(db.get(&a_id).unwrap(), Some(a_node.clone()));

        let b_node = Node::from("b".to_string());
        let b_id = NodeID::from(1_u64);

        assert_eq!(db.get(&b_id).unwrap(), None);

        db.put(&b_id, &b_node).unwrap();
        db.flush().unwrap();

        assert_eq!(db.get(&b_id).unwrap(), Some(b_node));
        assert_eq!(db.get(&a_id).unwrap(), Some(a_node));
    }
}
