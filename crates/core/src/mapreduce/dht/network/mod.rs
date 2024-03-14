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

use std::net::SocketAddr;

use openraft::{
    error::{InstallSnapshotError, RaftError},
    network::RPCOption,
    BasicNode, Raft, RaftNetwork, RaftNetworkFactory,
};

use crate::{
    distributed::sonic::{self, replication::RemoteClient, service::ResilientConnection},
    sonic_service,
};

use super::{NodeId, TypeConfig};

#[derive(Clone)]
pub struct Network {
    pub raft: Raft<TypeConfig>,
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, _target: NodeId, node: &BasicNode) -> Self::Network {
        let addr: SocketAddr = node.addr.parse().expect("addr is not a valid address");

        let client = RemoteClient::new(addr);

        Self::Network {
            client,
            raft: self.raft.clone(),
        }
    }
}

type AppendEntriesRequest = openraft::raft::AppendEntriesRequest<TypeConfig>;
type AppendEntriesResponse = openraft::raft::AppendEntriesResponse<NodeId>;

type InstallSnapshotRequest = openraft::raft::InstallSnapshotRequest<TypeConfig>;
type InstallSnapshotResponse = openraft::raft::InstallSnapshotResponse<NodeId>;

type VoteRequest = openraft::raft::VoteRequest<NodeId>;
type VoteResponse = openraft::raft::VoteResponse<NodeId>;

type RPCError<E = openraft::error::Infallible> =
    openraft::error::RPCError<NodeId, BasicNode, RaftError<NodeId, E>>;

sonic_service!(
    NetworkConnection,
    [AppendEntriesRequest, InstallSnapshotRequest, VoteRequest]
);

impl sonic::service::Message<NetworkConnection> for AppendEntriesRequest {
    type Response = AppendEntriesResponse;

    async fn handle(self, server: &NetworkConnection) -> sonic::Result<Self::Response> {
        server
            .raft
            .append_entries(self)
            .await
            .map_err(|e| sonic::Error::Application(e.into()))
    }
}

impl sonic::service::Message<NetworkConnection> for InstallSnapshotRequest {
    type Response = InstallSnapshotResponse;

    async fn handle(self, server: &NetworkConnection) -> sonic::Result<Self::Response> {
        server
            .raft
            .install_snapshot(self)
            .await
            .map_err(|e| sonic::Error::Application(e.into()))
    }
}

impl sonic::service::Message<NetworkConnection> for VoteRequest {
    type Response = VoteResponse;

    async fn handle(self, server: &NetworkConnection) -> sonic::Result<Self::Response> {
        server
            .raft
            .vote(self)
            .await
            .map_err(|e| sonic::Error::Application(e.into()))
    }
}

pub struct NetworkConnection {
    raft: Raft<TypeConfig>,
    client: RemoteClient<NetworkConnection>,
}

impl NetworkConnection {
    async fn conn<E: std::error::Error>(
        &self,
    ) -> Result<ResilientConnection<NetworkConnection>, RPCError<E>> {
        self.client
            .conn()
            .await
            .map_err(|e| RPCError::Unreachable(openraft::error::Unreachable::new(&e)))
    }

    async fn send_rpc<R, E>(
        &mut self,
        rpc: R,
        option: RPCOption,
    ) -> Result<R::Response, RPCError<E>>
    where
        R: sonic::service::Wrapper<NetworkConnection>,
        E: std::error::Error,
    {
        let conn = self.conn().await?;
        conn.send_with_timeout(&rpc, option.soft_ttl())
            .await
            .map_err(|e| match e {
                sonic::Error::ConnectionTimeout | sonic::Error::RequestTimeout => {
                    RPCError::Unreachable(openraft::error::Unreachable::new(&e))
                }
                _ => {
                    panic!("unexpected error: {:?}", e)
                }
            })
    }
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse, RPCError> {
        self.send_rpc(rpc, option).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest,
        option: RPCOption,
    ) -> Result<InstallSnapshotResponse, RPCError<InstallSnapshotError>> {
        self.send_rpc(rpc, option).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest,
        option: RPCOption,
    ) -> Result<VoteResponse, RPCError> {
        self.send_rpc(rpc, option).await
    }
}
