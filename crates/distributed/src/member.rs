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

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Debug)]
pub struct ShardId(u64);

impl ShardId {
    pub fn new(id: u64) -> ShardId {
        ShardId(id)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Debug)]
pub enum Service {
    Searcher { host: SocketAddr, shard: ShardId },
    Api { host: SocketAddr },
    Webgraph { host: SocketAddr },
    Alice { host: SocketAddr },
}

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct Member {
    pub id: String,
    pub service: Service,
}