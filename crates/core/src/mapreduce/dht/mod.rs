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

use self::network::NetworkConnection;

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

raft_sonic_request_response!(NetworkConnection, [Get, Set]);
