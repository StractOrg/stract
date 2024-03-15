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

use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    time::Duration,
};

use openraft::{
    error::{ClientWriteError, ForwardToLeader, InstallSnapshotError, RaftError},
    network::RPCOption,
    BasicNode, ChangeMembers, RaftNetwork,
};
use tokio::sync::RwLock;

use crate::{
    distributed::{
        retry_strategy::ExponentialBackoff,
        sonic::{self, service::ResilientConnection},
    },
    mapreduce::dht::{NodeId, TypeConfig},
    Result,
};

use super::{
    AddLearnerRequest, AddNodesRequest, AppendEntriesRequest, AppendEntriesResponse,
    InstallSnapshotRequest, InstallSnapshotResponse, Server, VoteRequest, VoteResponse,
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

impl sonic::service::Message<Server> for AddLearnerRequest {
    type Response = Result<(), RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>>;

    async fn handle(self, server: &Server) -> Self::Response {
        tracing::debug!("received add learner request: {:?}", self);

        let mut rem = BTreeSet::new();
        rem.insert(self.id);

        server
            .raft
            .change_membership(ChangeMembers::RemoveVoters(rem.clone()), true)
            .await?;
        server
            .raft
            .change_membership(ChangeMembers::RemoveNodes(rem.clone()), true)
            .await?;

        let node = BasicNode::new(self.addr);
        server.raft.add_learner(self.id, node, false).await?;

        Ok(())
    }
}

impl sonic::service::Message<Server> for AddNodesRequest {
    type Response = Result<(), RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>>;

    async fn handle(self, server: &Server) -> Self::Response {
        tracing::debug!("received add nodes request: {:?}", self);
        server
            .raft
            .change_membership(ChangeMembers::AddNodes(self.members), true)
            .await?;

        Ok(())
    }
}

type RPCError<E = openraft::error::Infallible> =
    openraft::error::RPCError<NodeId, BasicNode, RaftError<NodeId, E>>;

pub struct RemoteClient {
    target: NodeId,
    node: BasicNode,
    inner: sonic::replication::RemoteClient<Server>,
    likely_leader: RwLock<sonic::replication::RemoteClient<Server>>,
}

impl RemoteClient {
    pub fn new(target: NodeId, node: BasicNode) -> Self {
        let addr: SocketAddr = node.addr.parse().expect("addr is not a valid address");
        let inner = sonic::replication::RemoteClient::new(addr);
        let likely_leader = RwLock::new(inner.clone());

        Self {
            target,
            node,
            inner,
            likely_leader,
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
        &self,
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

    async fn add_learner(&self, id: NodeId, addr: SocketAddr) -> Result<()> {
        let rpc = AddLearnerRequest { id, addr };
        let retry = ExponentialBackoff::from_millis(500)
            .with_limit(Duration::from_secs(60))
            .take(5);

        for backoff in retry {
            let res = self.likely_leader.read().await.send(&rpc).await;

            match res {
                Ok(res) => match res {
                    Ok(_) => return Ok(()),
                    Err(RaftError::APIError(e)) => match e {
                        ClientWriteError::ForwardToLeader(ForwardToLeader {
                            leader_id: _,
                            leader_node,
                        }) => match leader_node {
                            Some(leader_node) => {
                                let mut likely_leader = self.likely_leader.write().await;
                                *likely_leader = sonic::replication::RemoteClient::new(
                                    leader_node
                                        .addr
                                        .parse()
                                        .expect("node addr should always be valid addr"),
                                );
                            }
                            None => tokio::time::sleep(backoff).await,
                        },
                        ClientWriteError::ChangeMembershipError(_) => {
                            tokio::time::sleep(backoff).await
                        }
                    },
                    Err(RaftError::Fatal(e)) => return Err(e.into()),
                },
                Err(e) => match e {
                    sonic::Error::IO(_)
                    | sonic::Error::Serialization(_)
                    | sonic::Error::ConnectionTimeout
                    | sonic::Error::RequestTimeout
                    | sonic::Error::PoolCreation => {
                        tokio::time::sleep(backoff).await;
                    }
                    sonic::Error::BadRequest
                    | sonic::Error::BodyTooLarge {
                        body_size: _,
                        max_size: _,
                    }
                    | sonic::Error::Application(_) => return Err(e.into()),
                },
            }
        }

        Err(anyhow::anyhow!("failed to add learner"))
    }

    async fn add_nodes(&self, members: BTreeMap<NodeId, BasicNode>) -> Result<()> {
        let rpc = AddNodesRequest { members };
        let retry = ExponentialBackoff::from_millis(500).with_limit(Duration::from_secs(10));

        for backoff in retry {
            let res = self.likely_leader.read().await.send(&rpc).await;

            match res {
                Ok(res) => match res {
                    Ok(_) => return Ok(()),
                    Err(RaftError::APIError(e)) => match e {
                        ClientWriteError::ForwardToLeader(ForwardToLeader {
                            leader_id: _,
                            leader_node,
                        }) => match leader_node {
                            Some(leader_node) => {
                                let mut likely_leader = self.likely_leader.write().await;
                                *likely_leader = sonic::replication::RemoteClient::new(
                                    leader_node
                                        .addr
                                        .parse()
                                        .expect("node addr should always be valid addr"),
                                );
                            }
                            None => tokio::time::sleep(backoff).await,
                        },
                        ClientWriteError::ChangeMembershipError(_) => {
                            tokio::time::sleep(backoff).await
                        }
                    },
                    Err(RaftError::Fatal(e)) => return Err(e.into()),
                },
                Err(e) => match e {
                    sonic::Error::IO(_)
                    | sonic::Error::Serialization(_)
                    | sonic::Error::ConnectionTimeout
                    | sonic::Error::RequestTimeout
                    | sonic::Error::PoolCreation => {
                        tokio::time::sleep(backoff).await;
                    }
                    sonic::Error::BadRequest
                    | sonic::Error::BodyTooLarge {
                        body_size: _,
                        max_size: _,
                    }
                    | sonic::Error::Application(_) => return Err(e.into()),
                },
            }
        }

        unreachable!("should continue to retry");
    }

    pub async fn join(
        &self,
        id: NodeId,
        addr: SocketAddr,
        new_all_nodes: BTreeMap<NodeId, BasicNode>,
    ) -> Result<()> {
        self.add_learner(id, addr).await?;
        self.add_nodes(new_all_nodes).await?;

        Ok(())
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
