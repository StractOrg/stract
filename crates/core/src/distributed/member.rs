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

use std::net::SocketAddr;

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

impl std::fmt::Display for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ShardId({})", self.0)
    }
}

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
pub enum LiveIndexState {
    InSetup,
    Ready,
}

impl std::fmt::Display for LiveIndexState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LiveIndexState::InSetup => write!(f, "setup"),
            LiveIndexState::Ready => write!(f, "ready"),
        }
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
        shard: crate::inverted_index::ShardId,
    },
    EntitySearcher {
        host: SocketAddr,
    },
    LiveIndex {
        host: SocketAddr,
        search_host: SocketAddr,
        shard: crate::inverted_index::ShardId,
        state: LiveIndexState,
    },
    Api {
        host: SocketAddr,
    },
    Webgraph {
        host: SocketAddr,
        shard: ShardId,
    },
    Dht {
        host: SocketAddr,
        shard: ShardId,
    },
    HarmonicWorker {
        host: SocketAddr,
        shard: ShardId,
    },
    ApproxHarmonicWorker {
        host: SocketAddr,
        shard: ShardId,
    },
    ShortestPathWorker {
        host: SocketAddr,
        shard: ShardId,
    },
}

impl std::fmt::Display for Service {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Searcher { host, shard } => write!(f, "Searcher {} {}", host, shard),
            Self::EntitySearcher { host } => write!(f, "EntitySearcher {}", host),
            Self::LiveIndex {
                host,
                search_host,
                shard,
                state,
            } => {
                write!(f, "LiveIndex {} {} {} {}", host, search_host, shard, state)
            }
            Self::Api { host } => write!(f, "Api {}", host),
            Self::Webgraph { host, shard } => {
                write!(f, "Webgraph {} {}", host, shard)
            }
            Self::Dht { host, shard } => write!(f, "Dht {} {}", host, shard),
            Self::HarmonicWorker { host, shard } => write!(f, "HarmonicWorker {} {}", host, shard),
            Self::ApproxHarmonicWorker { host, shard } => {
                write!(f, "ApproxHarmonicWorker {} {}", host, shard)
            }
            Self::ShortestPathWorker { host, shard } => {
                write!(f, "ShortestPathWorker {} {}", host, shard)
            }
        }
    }
}

impl Service {
    pub fn is_searcher(&self) -> bool {
        matches!(self, Self::Searcher { .. })
    }
}

#[derive(PartialEq, Eq, Hash, Clone, Debug, bincode::Encode, bincode::Decode)]
pub struct Member {
    pub id: String,
    pub service: Service,
}

impl Member {
    pub fn new(service: Service) -> Self {
        let id = uuid::Uuid::new_v4().to_string();
        Self { id, service }
    }
}
