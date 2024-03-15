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

use std::{net::SocketAddr, time::Duration};

use crate::{distributed::retry_strategy::RandomBackoff, Result};
use anyhow::anyhow;
use openraft::{
    error::{ClientWriteError, ForwardToLeader, RaftError},
    BasicNode,
};
use tokio::sync::RwLock;

use crate::{distributed::sonic, mapreduce::dht::NodeId};

use super::Server;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Set {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Get {
    pub key: String,
}

impl sonic::service::Message<Server> for Set {
    type Response = Result<(), RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>>;

    async fn handle(self, server: &Server) -> Self::Response {
        tracing::debug!("received set request: {:?}", self);

        match server.raft.client_write(self.into()).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

impl sonic::service::Message<Server> for Get {
    type Response = Option<String>;

    async fn handle(self, server: &Server) -> Self::Response {
        server
            .state_machine_store
            .state_machine
            .read()
            .await
            .data
            .get(&self.key)
            .cloned()
    }
}

pub struct RemoteClient {
    self_remote: sonic::replication::RemoteClient<Server>,
    likely_leader: RwLock<sonic::replication::RemoteClient<Server>>,
}

impl RemoteClient {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            self_remote: sonic::replication::RemoteClient::new(addr),
            likely_leader: RwLock::new(sonic::replication::RemoteClient::new(addr)),
        }
    }

    fn retry_strat() -> impl Iterator<Item = std::time::Duration> {
        RandomBackoff::new(
            std::time::Duration::from_millis(200),
            std::time::Duration::from_secs(1),
        )
    }

    pub async fn set(&self, key: String, value: String) -> Result<()> {
        for backoff in Self::retry_strat() {
            let res = self
                .likely_leader
                .read()
                .await
                .send_with_timeout(
                    &Set {
                        key: key.clone(),
                        value: value.clone(),
                    },
                    Duration::from_secs(5),
                )
                .await;

            tracing::debug!(".set() got response: {res:?}");

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
                            None => {
                                tokio::time::sleep(backoff).await;
                            }
                        },
                        ClientWriteError::ChangeMembershipError(_) => {
                            unreachable!(".set() should not change membership")
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

        Err(anyhow!("failed to set key"))
    }

    pub async fn get(&self, key: String) -> Result<Option<String>> {
        for backoff in Self::retry_strat() {
            match self
                .self_remote
                .send_with_timeout(&Get { key: key.clone() }, Duration::from_secs(5))
                .await
            {
                Ok(res) => return Ok(res),
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

        Err(anyhow!("failed to get key"))
    }
}
