// Stract is an open source web search engine.
// Copyright (C) 2023 Stract ApS
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
use chitchat::{
    spawn_chitchat, transport::UdpTransport, Chitchat, ChitchatConfig, ChitchatHandle,
    ClusterStateSnapshot, FailureDetectorConfig, NodeId,
};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::Mutex;

use crate::distributed::member::{Member, Service};

const CLUSTER_ID: &str = "stract-cluster";
const GOSSIP_INTERVAL: Duration = Duration::from_secs(1);
const SERVICE_KEY: &str = "service";

type Result<T> = std::result::Result<T, anyhow::Error>;

fn snapshot_members(snapshot: ClusterStateSnapshot) -> Vec<Member> {
    let mut res = Vec::new();
    for (id, state) in snapshot.node_states {
        if let Some(service) = state.get(SERVICE_KEY) {
            if let Ok(service) = serde_json::from_str(service) {
                res.push(Member { service, id });
            }
        }
    }

    res
}

pub struct Cluster {
    self_node: Option<Member>,
    chitchat: Arc<Mutex<Chitchat>>,
    // dropping the handle leaves the cluster
    _chitchat_handle: ChitchatHandle,
}

impl Cluster {
    pub async fn join(
        mut self_node: Member,
        gossip_addr: SocketAddr,
        seed_addrs: Vec<SocketAddr>,
    ) -> Result<Self> {
        let failure_detector_config = FailureDetectorConfig {
            initial_interval: GOSSIP_INTERVAL,
            ..Default::default()
        };

        let uuid = uuid::Uuid::new_v4().to_string();

        let node_id = NodeId {
            id: format!("{}_{}", self_node.id, uuid),
            gossip_public_address: gossip_addr,
        };
        self_node.id = node_id.id.clone();

        let config = ChitchatConfig {
            node_id,
            cluster_id: CLUSTER_ID.to_string(),
            gossip_interval: GOSSIP_INTERVAL,
            listen_addr: gossip_addr,
            seed_nodes: seed_addrs
                .into_iter()
                .map(|addr| addr.to_string())
                .collect(),
            failure_detector_config,
            is_ready_predicate: None,
        };

        Self::join_with_config(
            config,
            vec![(
                SERVICE_KEY.to_string(),
                serde_json::to_string(&self_node.service)?,
            )],
            Some(self_node),
        )
        .await
    }

    pub async fn join_as_spectator(
        cluster_id: String,
        gossip_addr: SocketAddr,
        seed_addrs: Vec<SocketAddr>,
    ) -> Result<Self> {
        let failure_detector_config = FailureDetectorConfig {
            initial_interval: GOSSIP_INTERVAL,
            ..Default::default()
        };

        let uuid = uuid::Uuid::new_v4().to_string();

        let node_id = NodeId {
            id: format!("{}_{}", cluster_id, uuid),
            gossip_public_address: gossip_addr,
        };
        let config = ChitchatConfig {
            node_id,
            cluster_id: CLUSTER_ID.to_string(),
            gossip_interval: GOSSIP_INTERVAL,
            listen_addr: gossip_addr,
            seed_nodes: seed_addrs
                .into_iter()
                .map(|addr| addr.to_string())
                .collect(),
            failure_detector_config,
            is_ready_predicate: None,
        };

        Self::join_with_config(config, vec![], None).await
    }

    async fn join_with_config(
        config: ChitchatConfig,
        key_values: Vec<(String, String)>,
        self_node: Option<Member>,
    ) -> Result<Self> {
        let transport = UdpTransport;

        let chitchat_handle = spawn_chitchat(config, key_values, &transport).await?;
        let chitchat = chitchat_handle.chitchat();

        Ok(Self {
            self_node,
            chitchat,
            _chitchat_handle: chitchat_handle,
        })
    }

    pub async fn members(&self) -> Vec<Member> {
        snapshot_members(self.chitchat.lock().await.state_snapshot())
    }

    pub async fn await_member<P>(&self, pred: P) -> Member
    where
        P: Fn(&Member) -> bool,
    {
        loop {
            let members = self.members().await;
            for member in members {
                if pred(&member) {
                    return member;
                }
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    pub fn self_node(&self) -> Option<&Member> {
        self.self_node.as_ref()
    }

    pub async fn set_service(&self, service: Service) -> Result<()> {
        self.chitchat
            .lock()
            .await
            .self_node_state()
            .set(SERVICE_KEY, serde_json::to_string(&service)?);

        Ok(())
    }

    #[cfg(test)]
    pub async fn remove_service(&self) -> Result<()> {
        self.chitchat
            .lock()
            .await
            .self_node_state()
            .set(SERVICE_KEY, String::new());

        Ok(())
    }
}
