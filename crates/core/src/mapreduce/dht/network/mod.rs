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

pub mod api;
mod raft;

use api::{Get, Set};
use std::{net::SocketAddr, sync::Arc};

use openraft::{error::RaftError, BasicNode, Raft, RaftNetworkFactory};

use crate::{distributed::sonic::replication::RemoteClient, sonic_service};

use super::{store::StateMachineStore, NodeId, TypeConfig};

#[derive(Clone)]
pub struct Network {
    pub raft: Raft<TypeConfig>,
    pub state_machine_store: Arc<StateMachineStore>,
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, _target: NodeId, node: &BasicNode) -> Self::Network {
        let addr: SocketAddr = node.addr.parse().expect("addr is not a valid address");

        let client = RemoteClient::new(addr);

        Self::Network {
            client,
            raft: self.raft.clone(),
            state_machine_store: self.state_machine_store.clone(),
        }
    }
}

pub type AppendEntriesRequest = openraft::raft::AppendEntriesRequest<TypeConfig>;
pub type AppendEntriesResponse = openraft::raft::AppendEntriesResponse<NodeId>;

pub type InstallSnapshotRequest = openraft::raft::InstallSnapshotRequest<TypeConfig>;
pub type InstallSnapshotResponse = openraft::raft::InstallSnapshotResponse<NodeId>;

pub type VoteRequest = openraft::raft::VoteRequest<NodeId>;
pub type VoteResponse = openraft::raft::VoteResponse<NodeId>;

type RPCError<E = openraft::error::Infallible> =
    openraft::error::RPCError<NodeId, BasicNode, RaftError<NodeId, E>>;

sonic_service!(
    NetworkConnection,
    [
        AppendEntriesRequest,
        InstallSnapshotRequest,
        VoteRequest,
        Get,
        Set
    ]
);

pub struct NetworkConnection {
    raft: Raft<TypeConfig>,
    client: RemoteClient<NetworkConnection>,
    state_machine_store: Arc<StateMachineStore>,
}
