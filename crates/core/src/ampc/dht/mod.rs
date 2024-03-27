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

//! Simple in-memory key-value store with Raft consensus where keys
//! and values are arbitrary bytes. It is intended to be deployed
//! across multiple nodes with multiple shards. Each shard cluster
//! is a Raft cluster, and each key is then routed to the correct
//! cluster based on hash(key) % number_of_shards. The keys
//! are currently *not* rebalanced if the number of shards change, so
//! if an entire shard becomes unavailable or a new shard is added, all
//! keys in the entire DHT is essentially lost as the
//! keys might hash incorrectly.
//!
//! Heavily inspired by https://github.com/datafuselabs/openraft/blob/main/examples/raft-kv-memstore/

mod client;
pub mod log_store;
mod network;
pub mod store;

use network::api::{AllTables, CreateTable, DropTable, Get, Set};

use std::fmt::Debug;
use std::io::Cursor;

use openraft::BasicNode;
use openraft::TokioRuntime;

use self::network::Server;

pub use network::api::RemoteClient as ApiClient;
pub use network::raft::RemoteClient as RaftClient;

pub use client::Client;

pub type NodeId = u64;

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub TypeConfig:
        D = Request,
        R = Response,
        NodeId = NodeId,
        Node = BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = TokioRuntime,
);

#[macro_export]
macro_rules! raft_sonic_request_response {
    ($service:ident, [$($req:ident),*$(,)?]) => {
        #[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
        pub enum Request {
            $(
                $req($req),
            )*
        }

        #[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
        pub enum Response {
            $(
                $req(<$req as $crate::distributed::sonic::service::Message<$service>>::Response),
            )*
            Empty,
        }

        $(
        impl From<$req> for Request {
            fn from(req: $req) -> Self {
                Request::$req(req)
            }
        }
        )*
    };
}

raft_sonic_request_response!(Server, [Get, Set, CreateTable, DropTable, AllTables]);

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};
    use tests::{network::api::RemoteClient, store::Table};
    use tokio::sync::Mutex;
    use tracing_test::traced_test;

    use crate::{distributed::sonic, free_socket_addr};
    use openraft::{error::InitializeError, Config};

    use proptest::prelude::*;
    use proptest_derive::Arbitrary;

    use rand::seq::SliceRandom;

    use super::*;

    async fn server(
        id: u64,
    ) -> anyhow::Result<(
        openraft::Raft<TypeConfig>,
        sonic::service::Server<Server>,
        SocketAddr,
    )> {
        let config = Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        };

        let config = Arc::new(config.validate().unwrap());

        let log_store = log_store::LogStore::<TypeConfig>::default();
        let state_machine_store = Arc::new(store::StateMachineStore::default());

        let network = network::Network;

        let raft = openraft::Raft::new(id, config, network, log_store, state_machine_store.clone())
            .await?;

        let addr = free_socket_addr();

        let server = Server::new(raft.clone(), state_machine_store)
            .bind(addr)
            .await?;

        Ok((raft, server, addr))
    }

    #[tokio::test]
    #[traced_test]
    async fn test_simple_set_get() -> anyhow::Result<()> {
        let (raft1, server1, addr1) = server(1).await?;
        let (raft2, server2, addr2) = server(2).await?;

        let servers = vec![server1, server2];

        for server in servers {
            tokio::spawn(async move {
                loop {
                    server.accept().await.unwrap();
                }
            });
        }

        let members: BTreeMap<u64, _> = vec![(1, addr1), (2, addr2)]
            .into_iter()
            .map(|(id, addr)| (id, BasicNode::new(addr)))
            .collect();

        if let Err(e) = raft1.initialize(members.clone()).await {
            match e {
                openraft::error::RaftError::APIError(e) => match e {
                    InitializeError::NotAllowed(_) => {}
                    InitializeError::NotInMembers(_) => panic!("{:?}", e),
                },
                openraft::error::RaftError::Fatal(_) => panic!("{:?}", e),
            }
        };

        if let Err(e) = raft2.initialize(members.clone()).await {
            match e {
                openraft::error::RaftError::APIError(e) => match e {
                    InitializeError::NotAllowed(_) => {}
                    InitializeError::NotInMembers(_) => panic!("{:?}", e),
                },
                openraft::error::RaftError::Fatal(_) => panic!("{:?}", e),
            }
        };

        let c1 = RemoteClient::new(addr1);
        let c2 = RemoteClient::new(addr2);

        let table = Table::from("test");

        c1.set(
            table.clone(),
            "hello".as_bytes().to_vec().into(),
            "world".as_bytes().to_vec().into(),
        )
        .await?;

        let res = c1.get(table.clone(), "hello".as_bytes().into()).await?;
        assert_eq!(res, Some("world".as_bytes().into()));

        let res = c2.get(table.clone(), "hello".as_bytes().into()).await?;
        assert_eq!(res, Some("world".as_bytes().into()));

        c2.set(
            table.clone(),
            "hello".as_bytes().into(),
            "world2".as_bytes().into(),
        )
        .await?;

        let res = c1.get(table.clone(), "hello".as_bytes().into()).await?;
        assert_eq!(res, Some("world2".as_bytes().into()));

        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_member_join() -> anyhow::Result<()> {
        let (raft1, server1, addr1) = server(1).await?;
        let (raft2, server2, addr2) = server(2).await?;
        let (raft3, server3, addr3) = server(3).await?;

        let servers = vec![server1, server2, server3];

        for server in servers {
            tokio::spawn(async move {
                loop {
                    server.accept().await.unwrap();
                }
            });
        }

        let members: BTreeMap<u64, _> = vec![(1, addr1), (2, addr2)]
            .into_iter()
            .map(|(id, addr)| (id, BasicNode::new(addr)))
            .collect();

        if let Err(e) = raft1.initialize(members.clone()).await {
            match e {
                openraft::error::RaftError::APIError(e) => match e {
                    InitializeError::NotAllowed(_) => {}
                    InitializeError::NotInMembers(_) => panic!("{:?}", e),
                },
                openraft::error::RaftError::Fatal(_) => panic!("{:?}", e),
            }
        };

        if let Err(e) = raft2.initialize(members.clone()).await {
            match e {
                openraft::error::RaftError::APIError(e) => match e {
                    InitializeError::NotAllowed(_) => {}
                    InitializeError::NotInMembers(_) => panic!("{:?}", e),
                },
                openraft::error::RaftError::Fatal(_) => panic!("{:?}", e),
            }
        };

        let c1 = RemoteClient::new(addr1);
        let c2 = RemoteClient::new(addr2);

        let table = Table::from("test");

        c1.set(
            table.clone(),
            "hello".as_bytes().into(),
            "world".as_bytes().into(),
        )
        .await?;

        let res = c2.get(table.clone(), "hello".as_bytes().into()).await?;

        assert_eq!(res, Some("world".as_bytes().into()));

        let members: BTreeMap<u64, _> = vec![(1, addr1), (2, addr2), (3, addr3)]
            .into_iter()
            .map(|(id, addr)| (id, BasicNode::new(addr)))
            .collect();

        raft3.initialize(members.clone()).await?;
        let rc1 = network::raft::RemoteClient::new(1, BasicNode::new(addr1));
        rc1.join(3, addr3, members.clone()).await?;

        let c3 = RemoteClient::new(addr3);
        let res = c3.get(table.clone(), "hello".as_bytes().into()).await?;
        assert_eq!(res, Some("world".as_bytes().into()));

        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn test_node_crash() -> anyhow::Result<()> {
        let (raft1, server1, addr1) = server(1).await?;
        let (raft2, server2, addr2) = server(2).await?;
        let (raft3, server3, addr3) = server(3).await?;

        let servers = vec![server1, server2, server3];
        let mut handles = Vec::new();

        for server in servers {
            handles.push(tokio::spawn(async move {
                loop {
                    server.accept().await.unwrap();
                }
            }));
        }

        let members: BTreeMap<u64, _> = vec![(1, addr1), (2, addr2), (3, addr3)]
            .into_iter()
            .map(|(id, addr)| (id, BasicNode::new(addr)))
            .collect();

        if let Err(e) = raft1.initialize(members.clone()).await {
            match e {
                openraft::error::RaftError::APIError(e) => match e {
                    InitializeError::NotAllowed(_) => {}
                    InitializeError::NotInMembers(_) => panic!("{:?}", e),
                },
                openraft::error::RaftError::Fatal(_) => panic!("{:?}", e),
            }
        };

        if let Err(e) = raft2.initialize(members.clone()).await {
            match e {
                openraft::error::RaftError::APIError(e) => match e {
                    InitializeError::NotAllowed(_) => {}
                    InitializeError::NotInMembers(_) => panic!("{:?}", e),
                },
                openraft::error::RaftError::Fatal(_) => panic!("{:?}", e),
            }
        };

        if let Err(e) = raft3.initialize(members.clone()).await {
            match e {
                openraft::error::RaftError::APIError(e) => match e {
                    InitializeError::NotAllowed(_) => {}
                    InitializeError::NotInMembers(_) => panic!("{:?}", e),
                },
                openraft::error::RaftError::Fatal(_) => panic!("{:?}", e),
            }
        };

        let c1 = RemoteClient::new(addr1);
        let c2 = RemoteClient::new(addr2);

        let rc1 = network::raft::RemoteClient::new(1, BasicNode::new(addr1));

        let table = Table::from("test");

        c1.set(
            table.clone(),
            "hello".as_bytes().into(),
            "world".as_bytes().into(),
        )
        .await?;

        let res = c1.get(table.clone(), "hello".as_bytes().into()).await?;
        assert_eq!(res, Some("world".as_bytes().into()));

        let res = c2.get(table.clone(), "hello".as_bytes().into()).await?;
        assert_eq!(res, Some("world".as_bytes().into()));

        // crash node 2
        handles[1].abort();
        drop(raft2);

        let (raft2, server2, addr2) = server(2).await?;
        handles[1] = tokio::spawn(async move {
            loop {
                server2.accept().await.unwrap();
            }
        });

        rc1.join(2, addr2, members.clone()).await?;

        let c2 = RemoteClient::new(addr2);

        let res = c2.get(table.clone(), "hello".as_bytes().into()).await?;
        assert_eq!(res, Some("world".as_bytes().into()));

        // crash node 2 again
        handles[1].abort();
        drop(raft2);

        c1.set(
            table.clone(),
            "hello".as_bytes().into(),
            "world2".as_bytes().into(),
        )
        .await?;

        let (raft2, server2, addr2) = server(2).await?;
        handles[1] = tokio::spawn(async move {
            loop {
                server2.accept().await.unwrap();
            }
        });
        raft2.initialize(members.clone()).await?;
        rc1.join(2, addr2, members.clone()).await?;

        let c2 = RemoteClient::new(addr2);

        let res = c2.get(table.clone(), "hello".as_bytes().into()).await?;
        assert_eq!(res, Some("world2".as_bytes().into()));

        Ok(())
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Arbitrary)]
    enum Action {
        Set { key: Vec<u8>, value: Vec<u8> },
        // get actions[prev_key % actions.len()]
        // if actions[prev_key % actions.len()] is a get, then get a non-existent key
        Get { prev_key: usize },
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10))]

        #[test]
        #[traced_test]
        fn proptest_chaos(actions: Vec<Action>) {
            let ground_truth = Arc::new(Mutex::new(BTreeMap::<Vec<u8>, Vec<u8>>::new()));

            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let (raft1, server1, addr1) = server(1).await.unwrap();
                    let (raft2, server2, addr2) = server(2).await.unwrap();

                    let servers = vec![server1, server2];

                    let mut handles = Vec::new();
                    for server in servers {
                        handles.push(tokio::spawn(async move {
                            loop {
                                server.accept().await.unwrap();
                            }
                        }));
                    }

                    let members: BTreeMap<u64, _> = vec![(1, addr1), (2, addr2)]
                        .into_iter()
                        .map(|(id, addr)| (id, BasicNode::new(addr)))
                        .collect();

                    if let Err(e) = raft1.initialize(members.clone()).await {
                        match e {
                            openraft::error::RaftError::APIError(e) => match e {
                                InitializeError::NotAllowed(_) => {}
                                InitializeError::NotInMembers(_) => panic!("{:?}", e),
                            },
                            openraft::error::RaftError::Fatal(_) => panic!("{:?}", e),
                        }
                    };

                    if let Err(e) = raft2.initialize(members.clone()).await {
                        match e {
                            openraft::error::RaftError::APIError(e) => match e {
                                InitializeError::NotAllowed(_) => {}
                                InitializeError::NotInMembers(_) => panic!("{:?}", e),
                            },
                            openraft::error::RaftError::Fatal(_) => panic!("{:?}", e),
                        }
                    };

                    let c1 = RemoteClient::new(addr1);
                    let c2 = RemoteClient::new(addr2);

                    let clients = Arc::new(vec![c1, c2]);

                    let shared_actions = Arc::new(actions.clone());
                    let table = Table::from("test");

                    for (i, action) in actions.into_iter().enumerate() {
                        match action {
                            Action::Set { key, value } => {
                                let client = clients.choose(&mut rand::thread_rng()).unwrap();

                                client.set(table.clone(), key.clone().into(), value.clone().into()).await.unwrap();
                                ground_truth.lock().await.insert(key.clone(), value.clone());
                            }
                            Action::Get { prev_key } => {
                                let client = clients.choose(&mut rand::thread_rng()).unwrap();
                                client.set(table.clone(), b"ensure-linearized-read".to_vec().into(), vec![].into()).await.unwrap();

                                let key = if i == 0 {
                                    b"non-existent-key".to_vec()
                                } else {
                                    match shared_actions[prev_key % i] {
                                        Action::Set { ref key, .. } => {
                                            key.clone()
                                        },
                                        Action::Get { .. } => b"non-existent-key".to_vec(),
                                    }
                                };

                                let res = client.get(table.clone(), key.clone().into()).await.unwrap();
                                let expected = ground_truth.lock().await.get(&key).cloned();

                                assert_eq!(res.map(|v| v.as_bytes().to_vec()), expected);
                            }
                        }
                    }
                });
        }
    }
}
