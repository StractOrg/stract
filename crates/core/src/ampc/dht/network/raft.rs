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
    ChangeMembers, RaftMetrics, RaftNetwork,
};
use tokio::sync::RwLock;

use crate::{
    ampc::{
        self,
        dht::{BasicNode, NodeId, TypeConfig},
    },
    distributed::{
        retry_strategy::ExponentialBackoff,
        sonic::{self, service::Connection},
    },
    Result,
};

use super::{
    AddLearner, AddNodes, AppendEntries, AppendEntriesResponse, InstallSnapshot,
    InstallSnapshotResponse, Metrics, Server, Vote, VoteResponse,
};

impl sonic::service::Message<Server> for AppendEntries {
    type Response =
        Result<AppendEntriesResponse, crate::bincode_utils::SerdeCompat<RaftError<NodeId>>>;

    async fn handle(self, server: &Server) -> Self::Response {
        tracing::debug!("received append entries request: {:?}", self);
        Ok(AppendEntriesResponse(
            server
                .raft
                .append_entries(self.0)
                .await
                .map_err(crate::bincode_utils::SerdeCompat)?,
        ))
    }
}

impl sonic::service::Message<Server> for InstallSnapshot {
    type Response = Result<
        InstallSnapshotResponse,
        crate::bincode_utils::SerdeCompat<RaftError<NodeId, InstallSnapshotError>>,
    >;

    async fn handle(self, server: &Server) -> Self::Response {
        tracing::debug!("received install snapshot request: {:?}", self);
        Ok(InstallSnapshotResponse(
            server
                .raft
                .install_snapshot(self.0)
                .await
                .map_err(crate::bincode_utils::SerdeCompat)?,
        ))
    }
}

impl sonic::service::Message<Server> for Vote {
    type Response = Result<VoteResponse, crate::bincode_utils::SerdeCompat<RaftError<NodeId>>>;

    async fn handle(self, server: &Server) -> Self::Response {
        tracing::debug!("received vote request: {:?}", self);
        Ok(VoteResponse(
            server
                .raft
                .vote(self.0)
                .await
                .map_err(crate::bincode_utils::SerdeCompat)?,
        ))
    }
}

impl sonic::service::Message<Server> for AddLearner {
    type Response = Result<
        (),
        crate::bincode_utils::SerdeCompat<RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>>,
    >;

    async fn handle(self, server: &Server) -> Self::Response {
        tracing::debug!("received add learner request: {:?}", self);

        let mut rem = BTreeSet::new();
        rem.insert(self.id);

        server
            .raft
            .change_membership(ChangeMembers::RemoveVoters(rem.clone()), false)
            .await
            .map_err(crate::bincode_utils::SerdeCompat)?;
        server
            .raft
            .change_membership(ChangeMembers::RemoveNodes(rem.clone()), false)
            .await
            .map_err(crate::bincode_utils::SerdeCompat)?;

        let node = BasicNode::new(self.addr);
        server
            .raft
            .add_learner(self.id, node, false)
            .await
            .map_err(crate::bincode_utils::SerdeCompat)?;

        Ok(())
    }
}

impl sonic::service::Message<Server> for Metrics {
    type Response = crate::bincode_utils::SerdeCompat<RaftMetrics<NodeId, BasicNode>>;

    async fn handle(self, server: &Server) -> Self::Response {
        tracing::debug!("received metrics request: {:?}", self);
        let metrics = server.raft.metrics().borrow().clone();

        crate::bincode_utils::SerdeCompat(metrics)
    }
}

impl sonic::service::Message<Server> for AddNodes {
    type Response = Result<
        (),
        crate::bincode_utils::SerdeCompat<RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>>,
    >;

    async fn handle(self, server: &Server) -> Self::Response {
        tracing::debug!("received add nodes request: {:?}", self);
        server
            .raft
            .change_membership(ChangeMembers::AddNodes(self.members), false)
            .await
            .map_err(crate::bincode_utils::SerdeCompat)?;

        Ok(())
    }
}

type RPCError<E = openraft::error::Infallible> =
    openraft::error::RPCError<NodeId, BasicNode, RaftError<NodeId, E>>;

async fn metrics(
    client: &sonic::replication::RemoteClient<Server>,
) -> Result<RaftMetrics<NodeId, BasicNode>> {
    let rpc = Metrics;
    let retry = ExponentialBackoff::from_millis(500)
        .with_limit(Duration::from_secs(60))
        .take(5);

    for backoff in retry {
        let res = client
            .send_with_timeout(&rpc, Duration::from_secs(30))
            .await;

        match res {
            Ok(crate::bincode_utils::SerdeCompat(res)) => return Ok(res),
            Err(_) => tokio::time::sleep(backoff).await,
        };
    }

    Err(anyhow::anyhow!("failed to get metrics"))
}

pub struct RemoteClient {
    target: NodeId,
    node: BasicNode,
    inner: sonic::replication::RemoteClient<Server>,
    likely_leader: RwLock<sonic::replication::RemoteClient<Server>>,
}

impl RemoteClient {
    pub async fn new(addr: SocketAddr) -> Result<Self> {
        let inner = sonic::replication::RemoteClient::new(addr);
        let likely_leader = RwLock::new(inner.clone());
        let metrics = metrics(&inner).await?;

        Ok(Self {
            target: metrics.id,
            node: BasicNode::new(addr),
            inner,
            likely_leader,
        })
    }
    async fn raft_conn<E: std::error::Error>(
        &self,
    ) -> Result<Connection<Server>, crate::bincode_utils::SerdeCompat<RPCError<E>>> {
        self.inner.conn().await.map_err(|e| {
            crate::bincode_utils::SerdeCompat(RPCError::Unreachable(
                openraft::error::Unreachable::new(&e),
            ))
        })
    }

    async fn send_raft_rpc<R, E>(
        &self,
        rpc: R,
        option: RPCOption,
    ) -> Result<R::Response, crate::bincode_utils::SerdeCompat<RPCError<E>>>
    where
        R: sonic::service::Wrapper<Server>,
        E: std::error::Error,
    {
        let conn = self.raft_conn().await?;
        conn.send_with_timeout(&rpc, option.soft_ttl())
            .await
            .map_err(|e| match e {
                sonic::Error::ConnectionTimeout | sonic::Error::RequestTimeout => {
                    crate::bincode_utils::SerdeCompat(RPCError::Unreachable(
                        openraft::error::Unreachable::new(&e),
                    ))
                }
                _ => {
                    panic!("unexpected error: {:?}", e)
                }
            })
    }

    pub async fn metrics(&self) -> Result<RaftMetrics<NodeId, BasicNode>> {
        metrics(&self.inner).await
    }

    async fn add_learner(&self, id: NodeId, addr: SocketAddr) -> Result<()> {
        let rpc = AddLearner { id, addr };
        let retry = ExponentialBackoff::from_millis(500)
            .with_limit(Duration::from_secs(60))
            .take(5);

        for backoff in retry {
            let res = self
                .likely_leader
                .read()
                .await
                .send_with_timeout(&rpc, Duration::from_secs(30))
                .await;

            match res {
                Ok(res) => match res {
                    Ok(_) => return Ok(()),
                    Err(crate::bincode_utils::SerdeCompat(RaftError::APIError(e))) => match e {
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
                    Err(crate::bincode_utils::SerdeCompat(RaftError::Fatal(e))) => {
                        return Err(e.into())
                    }
                },
                Err(e) => match e {
                    sonic::Error::IO(_)
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
        let rpc = AddNodes { members };
        let retry = ExponentialBackoff::from_millis(500).with_limit(Duration::from_secs(10));

        for backoff in retry {
            let res = self
                .likely_leader
                .read()
                .await
                .send_with_timeout(&rpc, Duration::from_secs(30))
                .await;

            match res {
                Ok(res) => match res {
                    Ok(_) => return Ok(()),
                    Err(crate::bincode_utils::SerdeCompat(RaftError::APIError(e))) => match e {
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
                    Err(crate::bincode_utils::SerdeCompat(RaftError::Fatal(e))) => {
                        return Err(e.into())
                    }
                },
                Err(e) => match e {
                    sonic::Error::IO(_)
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

    pub async fn join(&self, id: NodeId, addr: SocketAddr) -> Result<()> {
        self.add_learner(id, addr).await?;
        let metrics = self.metrics().await?;

        let nodes_in_cluster = metrics
            .membership_config
            .nodes()
            .map(|(nid, node)| (*nid, node.clone()))
            .collect::<BTreeMap<_, _>>();

        debug_assert!(
            nodes_in_cluster.contains_key(&id),
            "node should be in the cluster"
        );

        self.add_nodes(nodes_in_cluster).await?;

        Ok(())
    }
}

impl RaftNetwork<TypeConfig> for RemoteClient {
    async fn append_entries(
        &mut self,
        rpc: openraft::raft::AppendEntriesRequest<ampc::dht::TypeConfig>,
        option: RPCOption,
    ) -> Result<openraft::raft::AppendEntriesResponse<NodeId>, RPCError> {
        Ok(self
            .send_raft_rpc(AppendEntries(rpc), option)
            .await
            .map_err(|crate::bincode_utils::SerdeCompat(e)| e)?
            .map_err(|crate::bincode_utils::SerdeCompat(e)| -> RPCError {
                openraft::error::RemoteError {
                    target: self.target,
                    target_node: Some(self.node.clone()),
                    source: e,
                }
                .into()
            })?
            .0)
    }

    async fn install_snapshot(
        &mut self,
        rpc: openraft::raft::InstallSnapshotRequest<ampc::dht::TypeConfig>,
        option: RPCOption,
    ) -> Result<openraft::raft::InstallSnapshotResponse<NodeId>, RPCError<InstallSnapshotError>>
    {
        Ok(self
            .send_raft_rpc(InstallSnapshot(rpc), option)
            .await
            .map_err(|crate::bincode_utils::SerdeCompat(e)| e)?
            .map_err(
                |crate::bincode_utils::SerdeCompat(e)| -> RPCError<InstallSnapshotError> {
                    openraft::error::RemoteError {
                        target: self.target,
                        target_node: Some(self.node.clone()),
                        source: e,
                    }
                    .into()
                },
            )?
            .0)
    }

    async fn vote(
        &mut self,
        rpc: openraft::raft::VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<openraft::raft::VoteResponse<NodeId>, RPCError> {
        Ok(self
            .send_raft_rpc(Vote(rpc), option)
            .await
            .map_err(|crate::bincode_utils::SerdeCompat(e)| e)?
            .map_err(|crate::bincode_utils::SerdeCompat(e)| -> RPCError {
                openraft::error::RemoteError {
                    target: self.target,
                    target_node: Some(self.node.clone()),
                    source: e,
                }
                .into()
            })?
            .0)
    }
}
