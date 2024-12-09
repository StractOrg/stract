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
pub mod worker;

use bloom::U64BloomFilter;

use crate::distributed::member::ShardId;
use crate::{
    ampc::{prelude::*, DefaultDhtTable},
    webgraph,
};

use self::mapper::ShortestPathMapper;
use self::worker::{RemoteShortestPathWorker, ShortestPathWorker};

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
}

#[derive(bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct ShortestPathTables {
    distances: DefaultDhtTable<webgraph::NodeID, u64>,
    meta: DefaultDhtTable<(), Meta>,
    changed_nodes: DefaultDhtTable<ShardId, U64BloomFilter>,
}

impl_dht_tables!(ShortestPathTables, [distances, meta, changed_nodes]);

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct ShortestPathJob {
    shard: ShardId,
    source: webgraph::NodeID,
}

impl Job for ShortestPathJob {
    type DhtTables = ShortestPathTables;
    type Worker = ShortestPathWorker;
    type Mapper = ShortestPathMapper;

    fn is_schedulable(&self, worker: &RemoteShortestPathWorker) -> bool {
        self.shard == worker.shard()
    }
}
