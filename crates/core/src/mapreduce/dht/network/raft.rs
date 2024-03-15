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
    BasicNode, RaftNetwork,
};

use crate::{
    distributed::sonic::{self, service::ResilientConnection},
    mapreduce::dht::{NodeId, TypeConfig},
};

use super::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    Server, VoteRequest, VoteResponse,
};

impl sonic::service::Message<Server> for AppendEntriesRequest {
    type Response = Result<AppendEntriesResponse, RaftError<NodeId>>;

    async fn handle(self, server: &Server) -> Self::Response {
        tracing::debug!("received append entries request: {:?}", self);
        server.raft.append_entries(self).await
    }
}

impl sonic::service::Message<Server> for InstallSnapshotRequest {
    type Response = Result<InstallSnapshotResponse, RaftError<NodeId, InstallSnapshotError>>;

    async fn handle(self, server: &Server) -> Self::Response {
        tracing::debug!("received install snapshot request: {:?}", self);
        server.raft.install_snapshot(self).await
    }
}

impl sonic::service::Message<Server> for VoteRequest {
    type Response = Result<VoteResponse, RaftError<NodeId>>;

    async fn handle(self, server: &Server) -> Self::Response {
        tracing::debug!("received vote request: {:?}", self);
        server.raft.vote(self).await
    }
}

type RPCError<E = openraft::error::Infallible> =
    openraft::error::RPCError<NodeId, BasicNode, RaftError<NodeId, E>>;

pub struct RemoteClient {
    target: NodeId,
    node: BasicNode,
    inner: sonic::replication::RemoteClient<Server>,
}

impl RemoteClient {
    pub fn new(target: NodeId, node: BasicNode) -> Self {
        let addr: SocketAddr = node.addr.parse().expect("addr is not a valid address");
        let inner = sonic::replication::RemoteClient::new(addr);

        Self {
            target,
            node,
            inner,
        }
    }
    async fn raft_conn<E: std::error::Error>(
        &self,
    ) -> Result<ResilientConnection<Server>, RPCError<E>> {
        self.inner
            .conn()
            .await
            .map_err(|e| RPCError::Unreachable(openraft::error::Unreachable::new(&e)))
    }

    async fn send_raft_rpc<R, E>(
        &mut self,
        rpc: R,
        option: RPCOption,
    ) -> Result<R::Response, RPCError<E>>
    where
        R: sonic::service::Wrapper<Server>,
        E: std::error::Error,
    {
        let conn = self.raft_conn().await?;
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

impl RaftNetwork<TypeConfig> for RemoteClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse, RPCError> {
        self.send_raft_rpc(rpc, option).await?.map_err(|e| {
            openraft::error::RemoteError {
                target: self.target,
                target_node: Some(self.node.clone()),
                source: e,
            }
            .into()
        })
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest,
        option: RPCOption,
    ) -> Result<InstallSnapshotResponse, RPCError<InstallSnapshotError>> {
        self.send_raft_rpc(rpc, option).await?.map_err(|e| {
            openraft::error::RemoteError {
                target: self.target,
                target_node: Some(self.node.clone()),
                source: e,
            }
            .into()
        })
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest,
        option: RPCOption,
    ) -> Result<VoteResponse, RPCError> {
        self.send_raft_rpc(rpc, option).await?.map_err(|e| {
            openraft::error::RemoteError {
                target: self.target,
                target_node: Some(self.node.clone()),
                source: e,
            }
            .into()
        })
    }
}
