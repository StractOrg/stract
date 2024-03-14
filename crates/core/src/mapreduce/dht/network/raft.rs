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

use openraft::{error::InstallSnapshotError, network::RPCOption, RaftNetwork};

use crate::{
    distributed::sonic::{self, service::ResilientConnection},
    mapreduce::dht::TypeConfig,
};

use super::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    NetworkConnection, RPCError, VoteRequest, VoteResponse,
};

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

impl NetworkConnection {
    async fn raft_conn<E: std::error::Error>(
        &self,
    ) -> Result<ResilientConnection<NetworkConnection>, RPCError<E>> {
        self.client
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
        R: sonic::service::Wrapper<NetworkConnection>,
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

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse, RPCError> {
        self.send_raft_rpc(rpc, option).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest,
        option: RPCOption,
    ) -> Result<InstallSnapshotResponse, RPCError<InstallSnapshotError>> {
        self.send_raft_rpc(rpc, option).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest,
        option: RPCOption,
    ) -> Result<VoteResponse, RPCError> {
        self.send_raft_rpc(rpc, option).await
    }
}
