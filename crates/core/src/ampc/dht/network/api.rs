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
    net::SocketAddr,
    ops::{Bound, Range},
    sync::Arc,
    time::Duration,
};

use crate::{
    ampc::dht::{
        store::{Key, Table, Value},
        upsert::UpsertEnum,
        BasicNode, UpsertAction,
    },
    distributed::retry_strategy::RandomBackoff,
    Result,
};
use anyhow::anyhow;
use openraft::error::{ClientWriteError, ForwardToLeader, RaftError};
use tokio::sync::RwLock;

use crate::{ampc::dht::NodeId, distributed::sonic};

use super::Server;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct Set {
    pub table: Table,
    pub key: Key,
    pub value: Value,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct BatchSet {
    pub table: Table,
    pub values: Arc<Vec<(Key, Value)>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct Upsert {
    pub table: Table,
    pub key: Key,
    pub value: Value,
    pub upsert_fn: UpsertEnum,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct BatchUpsert {
    pub table: Table,
    pub values: Arc<Vec<(Key, Value)>>,
    pub upsert_fn: UpsertEnum,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct Get {
    pub table: Table,
    pub key: Key,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct BatchGet {
    pub table: Table,
    pub keys: Arc<Vec<Key>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct DropTable {
    pub table: Table,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct CreateTable {
    pub table: Table,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct AllTables;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct CloneTable {
    pub from: Table,
    pub to: Table,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct RangeGet {
    pub table: Table,
    pub range: Range<Bound<Key>>,
    pub limit: Option<usize>,
}

impl sonic::service::Message<Server> for Set {
    type Response = Result<
        (),
        crate::bincode_utils::SerdeCompat<RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>>,
    >;

    async fn handle(self, server: &Server) -> Self::Response {
        tracing::debug!("received set request: {:?}", self);

        match server.raft.client_write(self.into()).await {
            Ok(_) => Ok(()),
            Err(e) => Err(crate::bincode_utils::SerdeCompat(e)),
        }
    }
}

impl sonic::service::Message<Server> for BatchSet {
    type Response = Result<
        (),
        crate::bincode_utils::SerdeCompat<RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>>,
    >;

    async fn handle(self, server: &Server) -> Self::Response {
        tracing::debug!("received batch set request: {:?}", self);

        match server.raft.client_write(self.into()).await {
            Ok(_) => Ok(()),
            Err(e) => Err(crate::bincode_utils::SerdeCompat(e)),
        }
    }
}

impl sonic::service::Message<Server> for Upsert {
    type Response = Result<
        UpsertAction,
        crate::bincode_utils::SerdeCompat<RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>>,
    >;

    async fn handle(self, server: &Server) -> Self::Response {
        tracing::debug!("received upsert request: {:?}", self);

        match server.raft.client_write(self.into()).await {
            Ok(res) => match res.data {
                crate::ampc::dht::Response::Upsert(res) => res,
                _ => panic!("unexpected response from raft"),
            },
            Err(e) => Err(crate::bincode_utils::SerdeCompat(e)),
        }
    }
}

impl sonic::service::Message<Server> for BatchUpsert {
    type Response = Result<
        Vec<(Key, UpsertAction)>,
        crate::bincode_utils::SerdeCompat<RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>>,
    >;

    async fn handle(self, server: &Server) -> Self::Response {
        tracing::debug!("received batch upsert request: {:?}", self);

        match server.raft.client_write(self.into()).await {
            Ok(res) => match res.data {
                crate::ampc::dht::Response::BatchUpsert(res) => res,
                _ => panic!("unexpected response from raft"),
            },
            Err(e) => Err(crate::bincode_utils::SerdeCompat(e)),
        }
    }
}

impl sonic::service::Message<Server> for Get {
    type Response = Option<Value>;

    async fn handle(self, server: &Server) -> Self::Response {
        server
            .state_machine_store
            .state_machine
            .read()
            .await
            .db
            .get(&self.table, &self.key)
    }
}

impl sonic::service::Message<Server> for BatchGet {
    type Response = Vec<(Key, Value)>;

    async fn handle(self, server: &Server) -> Self::Response {
        server
            .state_machine_store
            .state_machine
            .read()
            .await
            .db
            .batch_get(&self.table, &self.keys)
    }
}

impl sonic::service::Message<Server> for DropTable {
    type Response = Result<
        (),
        crate::bincode_utils::SerdeCompat<RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>>,
    >;

    async fn handle(self, server: &Server) -> Self::Response {
        match server.raft.client_write(self.into()).await {
            Ok(_) => Ok(()),
            Err(e) => Err(crate::bincode_utils::SerdeCompat(e)),
        }
    }
}

impl sonic::service::Message<Server> for CreateTable {
    type Response = Result<
        (),
        crate::bincode_utils::SerdeCompat<RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>>,
    >;

    async fn handle(self, server: &Server) -> Self::Response {
        match server.raft.client_write(self.into()).await {
            Ok(_) => Ok(()),
            Err(e) => Err(crate::bincode_utils::SerdeCompat(e)),
        }
    }
}

impl sonic::service::Message<Server> for AllTables {
    type Response = Result<
        Vec<Table>,
        crate::bincode_utils::SerdeCompat<RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>>,
    >;

    async fn handle(self, server: &Server) -> Self::Response {
        match server.raft.client_write(self.into()).await {
            Ok(res) => match res.data {
                crate::ampc::dht::Response::AllTables(tables) => tables,
                _ => panic!("unexpected response from raft"),
            },
            Err(e) => Err(crate::bincode_utils::SerdeCompat(e)),
        }
    }
}

impl sonic::service::Message<Server> for CloneTable {
    type Response = Result<
        (),
        crate::bincode_utils::SerdeCompat<RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>>,
    >;

    async fn handle(self, server: &Server) -> Self::Response {
        match server.raft.client_write(self.into()).await {
            Ok(_) => Ok(()),
            Err(e) => Err(crate::bincode_utils::SerdeCompat(e)),
        }
    }
}

impl sonic::service::Message<Server> for RangeGet {
    type Response = Vec<(Key, Value)>;

    async fn handle(self, server: &Server) -> Self::Response {
        server
            .state_machine_store
            .state_machine
            .read()
            .await
            .db
            .range_get(&self.table, self.range, self.limit)
    }
}

impl std::fmt::Debug for RemoteClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteClient")
            .field("self_remote", &self.self_remote.addr())
            .finish()
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct RemoteClient {
    self_remote: sonic::replication::RemoteClient<Server>,
    #[serde(skip)]
    likely_leader: RwLock<Option<sonic::replication::RemoteClient<Server>>>,
}

impl bincode::Encode for RemoteClient {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        self.self_remote.encode(encoder)
    }
}

impl bincode::Decode for RemoteClient {
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let self_remote = sonic::replication::RemoteClient::decode(decoder)?;
        Ok(Self {
            self_remote,
            likely_leader: RwLock::new(None),
        })
    }
}

impl<'de> bincode::BorrowDecode<'de> for RemoteClient {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let self_remote = sonic::replication::RemoteClient::borrow_decode(decoder)?;
        Ok(Self {
            self_remote,
            likely_leader: RwLock::new(None),
        })
    }
}

impl RemoteClient {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            self_remote: sonic::replication::RemoteClient::new(addr),
            likely_leader: RwLock::new(None),
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.self_remote.addr()
    }

    fn retry_strat() -> impl Iterator<Item = std::time::Duration> {
        RandomBackoff::new(
            std::time::Duration::from_millis(200),
            std::time::Duration::from_secs(1),
        )
    }

    pub async fn set(&self, table: Table, key: Key, value: Value) -> Result<()> {
        for backoff in Self::retry_strat() {
            let res = self
                .likely_leader
                .read()
                .await
                .as_ref()
                .unwrap_or(&self.self_remote)
                .send_with_timeout(
                    &Set {
                        table: table.clone(),
                        key: key.clone(),
                        value: value.clone(),
                    },
                    Duration::from_secs(5),
                )
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
                                *likely_leader = Some(sonic::replication::RemoteClient::new(
                                    leader_node
                                        .addr
                                        .parse()
                                        .expect("node addr should always be valid addr"),
                                ));
                            }
                            None => {
                                tokio::time::sleep(backoff).await;
                            }
                        },
                        ClientWriteError::ChangeMembershipError(_) => {
                            unreachable!(".set() should not change membership")
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

        Err(anyhow!("failed to set key"))
    }

    pub async fn batch_set(&self, table: Table, values: Vec<(Key, Value)>) -> Result<()> {
        let values = Arc::new(values);

        for backoff in Self::retry_strat() {
            let res = self
                .likely_leader
                .read()
                .await
                .as_ref()
                .unwrap_or(&self.self_remote)
                .send_with_timeout(
                    &BatchSet {
                        table: table.clone(),
                        values: values.clone(),
                    },
                    Duration::from_secs(5),
                )
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
                                *likely_leader = Some(sonic::replication::RemoteClient::new(
                                    leader_node
                                        .addr
                                        .parse()
                                        .expect("node addr should always be valid addr"),
                                ));
                            }
                            None => {
                                tokio::time::sleep(backoff).await;
                            }
                        },
                        ClientWriteError::ChangeMembershipError(_) => {
                            unreachable!(".batch_set() should not change membership")
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

        Err(anyhow!("failed to batch set values"))
    }

    pub async fn get(&self, table: Table, key: Key) -> Result<Option<Value>> {
        for backoff in Self::retry_strat() {
            match self
                .self_remote
                .send_with_timeout(
                    &Get {
                        table: table.clone(),
                        key: key.clone(),
                    },
                    Duration::from_secs(5),
                )
                .await
            {
                Ok(res) => return Ok(res),
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

        Err(anyhow!("failed to get key"))
    }

    pub async fn batch_get(&self, table: Table, keys: Vec<Key>) -> Result<Vec<(Key, Value)>> {
        let keys = Arc::new(keys);

        for backoff in Self::retry_strat() {
            match self
                .self_remote
                .send_with_timeout(
                    &BatchGet {
                        table: table.clone(),
                        keys: keys.clone(),
                    },
                    Duration::from_secs(5),
                )
                .await
            {
                Ok(res) => return Ok(res),
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

        Err(anyhow!("failed to batch get keys"))
    }

    pub async fn drop_table(&self, table: Table) -> Result<()> {
        for backoff in Self::retry_strat() {
            let res = self
                .likely_leader
                .read()
                .await
                .as_ref()
                .unwrap_or(&self.self_remote)
                .send_with_timeout(
                    &DropTable {
                        table: table.clone(),
                    },
                    Duration::from_secs(5),
                )
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
                                *likely_leader = Some(sonic::replication::RemoteClient::new(
                                    leader_node
                                        .addr
                                        .parse()
                                        .expect("node addr should always be valid addr"),
                                ));
                            }
                            None => {
                                tokio::time::sleep(backoff).await;
                            }
                        },
                        ClientWriteError::ChangeMembershipError(_) => {
                            unreachable!(".drop_table() should not change membership")
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

        Err(anyhow!("failed to drop table"))
    }

    pub async fn create_table(&self, table: Table) -> Result<()> {
        for backoff in Self::retry_strat() {
            let res = self
                .likely_leader
                .read()
                .await
                .as_ref()
                .unwrap_or(&self.self_remote)
                .send_with_timeout(
                    &CreateTable {
                        table: table.clone(),
                    },
                    Duration::from_secs(5),
                )
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
                                *likely_leader = Some(sonic::replication::RemoteClient::new(
                                    leader_node
                                        .addr
                                        .parse()
                                        .expect("node addr should always be valid addr"),
                                ));
                            }
                            None => {
                                tokio::time::sleep(backoff).await;
                            }
                        },
                        ClientWriteError::ChangeMembershipError(_) => {
                            unreachable!(".create_table() should not change membership")
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

        Err(anyhow!("failed to create table"))
    }

    pub async fn all_tables(&self) -> Result<Vec<Table>> {
        for backoff in Self::retry_strat() {
            let res = self
                .likely_leader
                .read()
                .await
                .as_ref()
                .unwrap_or(&self.self_remote)
                .send_with_timeout(&AllTables, Duration::from_secs(5))
                .await;

            match res {
                Ok(res) => match res {
                    Ok(res) => return Ok(res),
                    Err(crate::bincode_utils::SerdeCompat(RaftError::APIError(e))) => match e {
                        ClientWriteError::ForwardToLeader(ForwardToLeader {
                            leader_id: _,
                            leader_node,
                        }) => match leader_node {
                            Some(leader_node) => {
                                let mut likely_leader = self.likely_leader.write().await;
                                *likely_leader = Some(sonic::replication::RemoteClient::new(
                                    leader_node
                                        .addr
                                        .parse()
                                        .expect("node addr should always be valid addr"),
                                ));
                            }
                            None => {
                                tokio::time::sleep(backoff).await;
                            }
                        },
                        ClientWriteError::ChangeMembershipError(_) => {
                            unreachable!(".all_tables() should not change membership")
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

        Err(anyhow!("failed to get tables"))
    }

    pub async fn clone_table(&self, from: Table, to: Table) -> Result<()> {
        for backoff in Self::retry_strat() {
            let res = self
                .likely_leader
                .read()
                .await
                .as_ref()
                .unwrap_or(&self.self_remote)
                .send_with_timeout(
                    &CloneTable {
                        from: from.clone(),
                        to: to.clone(),
                    },
                    Duration::from_secs(5),
                )
                .await;

            match res {
                Ok(res) => match res {
                    Ok(res) => return Ok(res),
                    Err(crate::bincode_utils::SerdeCompat(RaftError::APIError(e))) => match e {
                        ClientWriteError::ForwardToLeader(ForwardToLeader {
                            leader_id: _,
                            leader_node,
                        }) => match leader_node {
                            Some(leader_node) => {
                                let mut likely_leader = self.likely_leader.write().await;
                                *likely_leader = Some(sonic::replication::RemoteClient::new(
                                    leader_node
                                        .addr
                                        .parse()
                                        .expect("node addr should always be valid addr"),
                                ));
                            }
                            None => {
                                tokio::time::sleep(backoff).await;
                            }
                        },
                        ClientWriteError::ChangeMembershipError(_) => {
                            unreachable!(".clone_table() should not change membership")
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

        Err(anyhow!("failed to clone table"))
    }

    pub async fn upsert<F: Into<UpsertEnum>>(
        &self,
        table: Table,
        upsert: F,
        key: Key,
        value: Value,
    ) -> Result<UpsertAction> {
        let upsert = upsert.into();

        for backoff in Self::retry_strat() {
            let res = self
                .likely_leader
                .read()
                .await
                .as_ref()
                .unwrap_or(&self.self_remote)
                .send_with_timeout(
                    &Upsert {
                        table: table.clone(),
                        key: key.clone(),
                        value: value.clone(),
                        upsert_fn: upsert.clone(),
                    },
                    Duration::from_secs(5),
                )
                .await;

            match res {
                Ok(res) => match res {
                    Ok(res) => return Ok(res),
                    Err(crate::bincode_utils::SerdeCompat(RaftError::APIError(e))) => match e {
                        ClientWriteError::ForwardToLeader(ForwardToLeader {
                            leader_id: _,
                            leader_node,
                        }) => match leader_node {
                            Some(leader_node) => {
                                let mut likely_leader = self.likely_leader.write().await;
                                *likely_leader = Some(sonic::replication::RemoteClient::new(
                                    leader_node
                                        .addr
                                        .parse()
                                        .expect("node addr should always be valid addr"),
                                ));
                            }
                            None => {
                                tokio::time::sleep(backoff).await;
                            }
                        },
                        ClientWriteError::ChangeMembershipError(_) => {
                            unreachable!(".upert() should not change membership")
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

        Err(anyhow!("failed to perform upsert"))
    }

    pub async fn batch_upsert<F: Into<UpsertEnum>>(
        &self,
        table: Table,
        upsert: F,
        values: Vec<(Key, Value)>,
    ) -> Result<Vec<(Key, UpsertAction)>> {
        let upsert = upsert.into();
        let values = Arc::new(values);

        for backoff in Self::retry_strat() {
            let res = self
                .likely_leader
                .read()
                .await
                .as_ref()
                .unwrap_or(&self.self_remote)
                .send_with_timeout(
                    &BatchUpsert {
                        table: table.clone(),
                        upsert_fn: upsert.clone(),
                        values: values.clone(),
                    },
                    Duration::from_secs(5),
                )
                .await;

            match res {
                Ok(res) => match res {
                    Ok(res) => return Ok(res),
                    Err(crate::bincode_utils::SerdeCompat(RaftError::APIError(e))) => match e {
                        ClientWriteError::ForwardToLeader(ForwardToLeader {
                            leader_id: _,
                            leader_node,
                        }) => match leader_node {
                            Some(leader_node) => {
                                let mut likely_leader = self.likely_leader.write().await;
                                *likely_leader = Some(sonic::replication::RemoteClient::new(
                                    leader_node
                                        .addr
                                        .parse()
                                        .expect("node addr should always be valid addr"),
                                ));
                            }
                            None => {
                                tokio::time::sleep(backoff).await;
                            }
                        },
                        ClientWriteError::ChangeMembershipError(_) => {
                            unreachable!(".batch_upsert() should not change membership")
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

        Err(anyhow!("failed to batch upsert values"))
    }

    pub async fn range_get(
        &self,
        table: Table,
        range: Range<Bound<Key>>,
        limit: Option<usize>,
    ) -> Result<Vec<(Key, Value)>> {
        for backoff in Self::retry_strat() {
            let res = self
                .self_remote
                .send_with_timeout(
                    &RangeGet {
                        table: table.clone(),
                        range: range.clone(),
                        limit,
                    },
                    Duration::from_secs(5),
                )
                .await;

            tracing::debug!(".range_get() got response: {res:?}");

            match res {
                Ok(res) => return Ok(res),
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

        Err(anyhow!("failed to perform range get"))
    }
}
