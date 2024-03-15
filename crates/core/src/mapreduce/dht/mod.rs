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

//! Heavily inspired by https://github.com/datafuselabs/openraft/blob/main/examples/raft-kv-memstore/

mod log_store;
mod network;
mod store;

use network::api::{Get, Set};

use std::fmt::Debug;
use std::io::Cursor;

use openraft::BasicNode;
use openraft::TokioRuntime;

use self::network::Server;

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
                $req(<$req as crate::distributed::sonic::service::Message<$service>>::Response),
            )*
        }

        $(
        impl TryFrom<Response> for <$req as crate::distributed::sonic::service::Message<$service>>::Response {
            type Error = crate::distributed::sonic::Error;
            fn try_from(res: Response) -> Result<Self, Self::Error> {
                match res {
                    Response::$req(res) => Ok(res),
                    _ => Err(crate::distributed::sonic::Error::Application(anyhow::anyhow!("Invalid response for request from Raft"))),
                }
            }
        }
        )*

        $(
        impl From<$req> for Request {
            fn from(req: $req) -> Self {
                Request::$req(req)
            }
        }
        )*
    };
}

raft_sonic_request_response!(Server, [Get, Set]);

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};
    use tests::network::api::RemoteClient;
    use tracing_test::traced_test;

    use crate::{distributed::sonic, free_socket_addr};
    use openraft::Config;

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
        let (raft3, server3, addr3) = server(3).await?;

        let servers = vec![server1, server2, server3];

        for server in servers {
            tokio::spawn(async move {
                loop {
                    server.accept().await.unwrap();
                }
            });
        }

        let members: BTreeMap<u64, _> = vec![(1, addr1), (2, addr2), (3, addr3)]
            .into_iter()
            .map(|(id, addr)| (id, BasicNode::new(addr)))
            .collect();

        raft1.initialize(members.clone()).await?;
        raft2.initialize(members.clone()).await?;
        raft3.initialize(members.clone()).await?;

        let c1 = RemoteClient::new(addr1);
        let c2 = RemoteClient::new(addr2);
        let c3 = RemoteClient::new(addr3);

        c1.set("hello".to_string(), "world".to_string()).await?;

        let res = c1.get("hello".to_string()).await?;
        assert_eq!(res, Some("world".to_string()));

        let res = c2.get("hello".to_string()).await?;
        assert_eq!(res, Some("world".to_string()));

        c2.set("hello".to_string(), "world2".to_string()).await?;
        let res = c3.get("hello".to_string()).await?;
        assert_eq!(res, Some("world2".to_string()));

        let res = c1.get("hello".to_string()).await?;
        assert_eq!(res, Some("world2".to_string()));

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

        raft1.initialize(members.clone()).await?;
        raft2.initialize(members.clone()).await?;

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        let c1 = RemoteClient::new(addr1);
        let c2 = RemoteClient::new(addr2);

        c1.set("hello".to_string(), "world".to_string()).await?;

        let res = c2.get("hello".to_string()).await?;

        assert_eq!(res, Some("world".to_string()));

        let members: BTreeMap<u64, _> = vec![(1, addr1), (2, addr2), (3, addr3)]
            .into_iter()
            .map(|(id, addr)| (id, BasicNode::new(addr)))
            .collect();

        raft3.initialize(members.clone()).await?;
        let rc1 = network::raft::RemoteClient::new(1, BasicNode::new(addr1));
        rc1.join(3, addr3, members.clone()).await?;

        let c3 = RemoteClient::new(addr3);
        let res = c3.get("hello".to_string()).await?;
        assert_eq!(res, Some("world".to_string()));

        Ok(())
    }
}
