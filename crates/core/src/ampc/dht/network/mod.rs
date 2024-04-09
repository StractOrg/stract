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

pub mod api;
pub mod raft;

use api::{
    AllTables, BatchGet, BatchSet, BatchUpsert, CloneTable, CreateTable, DropTable, Get, Set,
    Upsert,
};
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};

use openraft::{BasicNode, Raft, RaftNetworkFactory};

use self::raft::RemoteClient;
use crate::distributed::sonic::service::sonic_service;

use super::{store::StateMachineStore, NodeId, TypeConfig};

#[derive(Clone)]
pub struct Network;

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = RemoteClient;

    async fn new_client(&mut self, _: NodeId, node: &BasicNode) -> Self::Network {
        RemoteClient::new(node.addr.parse().unwrap()).await.unwrap()
    }
}

pub type AppendEntries = openraft::raft::AppendEntriesRequest<TypeConfig>;
pub type AppendEntriesResponse = openraft::raft::AppendEntriesResponse<NodeId>;

pub type InstallSnapshot = openraft::raft::InstallSnapshotRequest<TypeConfig>;
pub type InstallSnapshotResponse = openraft::raft::InstallSnapshotResponse<NodeId>;

pub type Vote = openraft::raft::VoteRequest<NodeId>;
pub type VoteResponse = openraft::raft::VoteResponse<NodeId>;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct AddLearner {
    pub id: NodeId,
    pub addr: SocketAddr,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct AddNodes {
    members: BTreeMap<NodeId, BasicNode>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct Metrics;

sonic_service!(
    Server,
    [
        AppendEntries,
        InstallSnapshot,
        Vote,
        Metrics,
        AddLearner,
        AddNodes,
        Get,
        BatchGet,
        Set,
        BatchSet,
        Upsert,
        BatchUpsert,
        DropTable,
        CreateTable,
        AllTables,
        CloneTable,
    ]
);

pub struct Server {
    raft: Raft<TypeConfig>,
    state_machine_store: Arc<StateMachineStore>,
}

impl Server {
    pub fn new(raft: Raft<TypeConfig>, state_machine_store: Arc<StateMachineStore>) -> Self {
        Self {
            raft,
            state_machine_store,
        }
    }
}
