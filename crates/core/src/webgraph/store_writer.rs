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

use std::{
    fs::File,
    path::{Path, PathBuf},
};

use crate::file_store::{
    self,
    iterable::{IterableStoreReader, SortedIterableStoreReader},
};
use crate::Result;

use super::{store::EdgeStore, Compression, EdgeLabel, InnerEdge, NodeID};

#[derive(bincode::Encode, bincode::Decode)]
struct SortableEdge<L: EdgeLabel> {
    sort_node: NodeID,
    edge: InnerEdge<L>,
}

impl<L: EdgeLabel> PartialOrd for SortableEdge<L> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<L: EdgeLabel> Ord for SortableEdge<L> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sort_node.cmp(&other.sort_node)
    }
}

impl<L: EdgeLabel> PartialEq for SortableEdge<L> {
    fn eq(&self, other: &Self) -> bool {
        self.sort_node == other.sort_node
    }
}

impl<L: EdgeLabel> Eq for SortableEdge<L> {}

pub const MAX_BATCH_SIZE: usize = 100_000;

struct SortedEdgeIterator<M, D>
where
    M: Iterator<Item = SortableEdge<String>>,
    D: Iterator<Item = SortableEdge<String>>,
{
    mem: file_store::Peekable<M>,
    file_reader: file_store::Peekable<D>,
}

impl<M, D> Iterator for SortedEdgeIterator<M, D>
where
    M: Iterator<Item = SortableEdge<String>>,
    D: Iterator<Item = SortableEdge<String>>,
{
    type Item = SortableEdge<String>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(edge) = self.mem.peek() {
            if let Some(file_edge) = self.file_reader.peek() {
                if edge.sort_node < file_edge.sort_node {
                    self.mem.next()
                } else {
                    self.file_reader.next()
                }
            } else {
                self.mem.next()
            }
        } else {
            self.file_reader.next()
        }
    }
}

pub struct EdgeStoreWriter {
    reversed: bool,
    path: PathBuf,
    edges: Vec<SortableEdge<String>>,
    stored_writers: Vec<PathBuf>,
    compression: Compression,
}

impl EdgeStoreWriter {
    pub fn new<P: AsRef<Path>>(path: P, compression: Compression, reversed: bool) -> Self {
        let path = path.as_ref().join("writer");

        if !path.exists() {
            std::fs::create_dir_all(&path).unwrap();
        }

        Self {
            edges: Vec::new(),
            reversed,
            path: path.to_path_buf(),
            compression,
            stored_writers: Vec::new(),
        }
    }

    fn flush_to_file(&mut self) -> Result<()> {
        let file_path = self
            .path
            .join(format!("{}.store", self.stored_writers.len()));
        let file = File::create(&file_path)?;

        let mut writer = file_store::iterable::IterableStoreWriter::new(file);

        for edge in &self.edges {
            writer.write(edge)?;
        }
        writer.finalize()?;

        self.edges.clear();

        self.stored_writers.push(file_path);

        Ok(())
    }

    pub fn put(&mut self, edge: InnerEdge<String>) {
        let sort_node = if self.reversed {
            edge.to.id
        } else {
            edge.from.id
        };

        self.edges.push(SortableEdge { sort_node, edge });

        if self.edges.len() >= MAX_BATCH_SIZE {
            self.flush_to_file().unwrap();
        }
    }

    fn sorted_edges(mut self) -> impl Iterator<Item = SortableEdge<String>> {
        self.edges.sort_unstable();
        let readers = self
            .stored_writers
            .iter()
            .map(|p| {
                let file = File::open(p).unwrap();
                IterableStoreReader::new(file)
            })
            .collect();
        let file_reader = SortedIterableStoreReader::new(readers).map(|r| r.unwrap());

        let edges = std::mem::take(&mut self.edges);

        SortedEdgeIterator {
            mem: file_store::Peekable::new(edges.into_iter()),
            file_reader: file_store::Peekable::new(file_reader),
        }
    }

    pub fn finalize(self) -> EdgeStore {
        let p = self.path.parent().unwrap().to_path_buf();

        EdgeStore::build(
            p,
            self.compression,
            self.reversed,
            self.sorted_edges().map(|e| e.edge),
        )
    }
}

impl Drop for EdgeStoreWriter {
    fn drop(&mut self) {
        // cleanup writer directory
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}
