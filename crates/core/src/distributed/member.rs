// Stract is an open source web search engine.
// Copyright (C) 2023 Stract ApS
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

use std::net::SocketAddr;

use crate::config::WebgraphGranularity;

#[derive(
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    PartialEq,
    Eq,
    Hash,
    Clone,
    Copy,
    Debug,
    PartialOrd,
    Ord,
)]
pub struct ShardId(u64);

impl ShardId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<u64> for ShardId {
    fn from(id: u64) -> Self {
        Self(id)
    }
}

impl From<ShardId> for u64 {
    fn from(id: ShardId) -> u64 {
        id.0
    }
}

#[derive(
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    PartialEq,
    Eq,
    Hash,
    Clone,
    Debug,
)]
pub enum Service {
    Searcher {
        host: SocketAddr,
        shard: ShardId,
    },
    EntitySearcher {
        host: SocketAddr,
    },
    LiveIndex {
        host: SocketAddr,
        split_id: crate::feed::scheduler::SplitId,
    },
    Api {
        host: SocketAddr,
    },
    Webgraph {
        host: SocketAddr,
        shard: ShardId,
        granularity: WebgraphGranularity,
    },
    Dht {
        host: SocketAddr,
        shard: ShardId,
    },
    HarmonicWorker {
        host: SocketAddr,
        shard: ShardId,
    },
    HarmonicCoordinator {
        host: SocketAddr,
    },
    ApproxHarmonicWorker {
        host: SocketAddr,
        shard: ShardId,
    },
    ApproxHarmonicCoordinator {
        host: SocketAddr,
    },
}

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct Member {
    pub id: String,
    pub service: Service,
}
