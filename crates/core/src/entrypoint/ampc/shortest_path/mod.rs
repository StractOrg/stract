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

//! Single-source shortest path.

pub mod coordinator;
mod mapper;
mod updated_nodes;
pub mod worker;

pub use updated_nodes::UpdatedNodes;

use crate::distributed::member::ShardId;
use crate::{
    ampc::{prelude::*, DefaultDhtTable},
    webgraph,
};

pub use self::mapper::ShortestPathMapper;
pub use self::worker::{RemoteShortestPathWorker, ShortestPathWorker};

#[derive(
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    Debug,
    Clone,
    PartialEq,
    Eq,
)]
pub struct Meta {
    round_had_changes: bool,
    round: u64,
}

#[derive(bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct ShortestPathTables {
    pub distances: DefaultDhtTable<webgraph::NodeID, u64>,
    pub meta: DefaultDhtTable<(), Meta>,
    pub changed_nodes: DefaultDhtTable<ShardId, UpdatedNodes>,
}

impl_dht_tables!(ShortestPathTables, [distances, meta, changed_nodes]);

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct ShortestPathJob {
    pub shard: ShardId,
    pub source: webgraph::NodeID,
}

impl Job for ShortestPathJob {
    type DhtTables = ShortestPathTables;
    type Worker = ShortestPathWorker;
    type Mapper = ShortestPathMapper;

    fn is_schedulable(&self, worker: &RemoteShortestPathWorker) -> bool {
        self.shard == worker.shard()
    }
}
