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

pub mod coordinator;
mod mapper;
pub mod worker;

use std::net::SocketAddr;

use crate::distributed::member::ShardId;
use crate::{
    ampc::{prelude::*, DefaultDhtTable},
    kahan_sum::KahanSum,
    webgraph,
};

use self::mapper::ApproxCentralityMapper;
use self::worker::{ApproxCentralityWorker, RemoteApproxCentralityWorker};

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct Meta {
    round: u64,
    num_samples_per_worker: u64,
}

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct ApproxCentralityTables {
    centrality: DefaultDhtTable<webgraph::NodeID, KahanSum>,
    meta: DefaultDhtTable<(), Meta>,
}

impl_dht_tables!(ApproxCentralityTables, [centrality, meta]);

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct ApproxCentralityJob {
    shard: ShardId,
    max_distance: u8,
    norm: f64,
    all_workers: Vec<(ShardId, SocketAddr)>,
}

impl Job for ApproxCentralityJob {
    type DhtTables = ApproxCentralityTables;
    type Worker = ApproxCentralityWorker;
    type Mapper = ApproxCentralityMapper;

    fn is_schedulable(&self, worker: &RemoteApproxCentralityWorker) -> bool {
        self.shard == worker.shard()
    }
}
