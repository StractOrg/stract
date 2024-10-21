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

use std::io::Write;
use std::path::{Path, PathBuf};
use std::{fs, io};

use crate::ampc::dht::ShardId;
use crate::Result;

use itertools::Itertools;
pub use query::{Collector, Query};
use rand::seq::{IteratorRandom, SliceRandom};
use rustc_hash::FxHashSet;
use store::EdgeStore;

pub use builder::WebgraphBuilder;
pub use document::*;
pub use node::*;
pub use shortest_path::ShortestPaths;

use searcher::Searcher;

mod builder;
pub mod centrality;
mod doc_address;
mod document;
mod node;
pub mod query;
pub mod remote;
mod schema;
mod searcher;
mod shortest_path;
mod store;

#[cfg(test)]
pub mod tests;

pub const MAX_LABEL_LENGTH: usize = 256;

#[derive(
    Debug, Clone, Copy, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
)]
pub enum EdgeLimit {
    Unlimited,
    Limit(usize),
    LimitAndOffset { limit: usize, offset: usize },
}

impl EdgeLimit {
    pub fn limit(&self) -> Option<usize> {
        match self {
            EdgeLimit::Unlimited => None,
            EdgeLimit::Limit(limit) => Some(*limit),
            EdgeLimit::LimitAndOffset { limit, .. } => Some(*limit),
        }
    }

    pub fn offset(&self) -> Option<usize> {
        match self {
            EdgeLimit::LimitAndOffset { offset, .. } => Some(*offset),
            _ => None,
        }
    }
}

pub struct Webgraph {
    path: String,
    store: EdgeStore,
}

impl Webgraph {
    pub fn builder<P: AsRef<Path>>(path: P, shard_id: ShardId) -> WebgraphBuilder {
        WebgraphBuilder::new(path, shard_id)
    }

    pub fn path(&self) -> PathBuf {
        PathBuf::from(&self.path)
    }

    pub fn open<P: AsRef<Path>>(path: P, shard_id: ShardId) -> Result<Self> {
        fs::create_dir_all(&path)?;

        let store = EdgeStore::open(path.as_ref().join("edges"), shard_id)?;

        Ok(Self {
            store,
            path: path.as_ref().as_os_str().to_str().unwrap().to_string(),
        })
    }

    pub fn insert(&mut self, edge: Edge) -> Result<()> {
        self.store.insert(edge)?;
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        self.store.commit()?;
        Ok(())
    }

    pub fn merge(&mut self, other: Webgraph) -> Result<()> {
        let other_folder = other.path.clone();
        self.store.merge(other.store)?;

        fs::remove_dir_all(other_folder)?;

        Ok(())
    }

    pub fn optimize_read(&mut self) -> Result<()> {
        self.store.optimize_read()?;
        Ok(())
    }

    pub fn search_initial<Q: Query>(
        &self,
        query: &Q,
    ) -> Result<<Q::Collector as Collector>::Fruit> {
        self.store.search_initial(query)
    }

    pub fn retrieve<Q: Query>(
        &self,
        query: &Q,
        fruit: <Q::Collector as Collector>::Fruit,
    ) -> Result<Q::IntermediateOutput> {
        self.store.retrieve(query, fruit)
    }

    pub fn search<Q: Query>(&self, query: &Q) -> Result<Q::Output> {
        self.store.search(query)
    }

    pub fn host_nodes(&self) -> FxHashSet<NodeID> {
        self.store.iter_host_node_ids(0, u32::MAX).collect()
    }

    pub fn page_nodes(&self) -> FxHashSet<NodeID> {
        self.store.iter_page_node_ids(0, u32::MAX).collect()
    }

    pub fn random_page_nodes_with_outgoing(&self, num: usize) -> Vec<NodeID> {
        let mut rng = rand::thread_rng();
        let mut nodes = self
            .page_edges()
            .map(|e| e.from)
            .unique()
            .choose_multiple(&mut rng, num);
        nodes.shuffle(&mut rng);
        nodes
    }

    pub fn page_node_ids_with_offset(&self, offset: u64, limit: u64) -> Vec<NodeID> {
        self.store
            .iter_page_node_ids(offset as u32, limit as u32)
            .collect()
    }

    /// Iterate all edges in the graph at least once.
    /// Some edges may be returned multiple times.
    /// This happens if they are present in more than one segment.
    pub fn page_edges(&self) -> impl Iterator<Item = SmallEdge> + '_ {
        self.store.iter_pages_small()
    }

    /// Iterate all host edges in the graph at least once.
    /// Some edges may be returned multiple times.
    /// This happens if they are present in more than one segment.
    pub fn host_edges(&self) -> impl Iterator<Item = SmallEdge> + '_ {
        self.store.iter_hosts_small()
    }
}
