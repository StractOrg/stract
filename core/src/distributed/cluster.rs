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
    spawn_chitchat, transport::UdpTransport, ChitchatConfig, ChitchatHandle, FailureDetectorConfig,
    NodeId,
};
use std::{collections::HashSet, net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tokio_stream::StreamExt;
use tracing::error;

use crate::distributed::member::{Member, Service};

const CLUSTER_ID: &str = "stract-cluster";
const GOSSIP_INTERVAL: Duration = Duration::from_secs(1);
const SERVICE_KEY: &str = "service";

type Result<T> = std::result::Result<T, anyhow::Error>;

pub struct Cluster {
    alive_nodes: Arc<RwLock<HashSet<Member>>>,
    // dropping the handle leaves the cluster
    _chitchat_handle: ChitchatHandle,
}

impl Cluster {
    pub async fn join(
        self_node: Member,
        gossip_addr: SocketAddr,
        seed_addrs: Vec<SocketAddr>,
    ) -> Result<Self> {
        let transport = UdpTransport;
        let failure_detector_config = FailureDetectorConfig {
            initial_interval: GOSSIP_INTERVAL,
            ..Default::default()
        };

        let uuid = uuid::Uuid::new_v4().to_string();

        let node_id = NodeId {
            id: format!("{}_{}", self_node.id, uuid),
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

        let chitchat_handle = spawn_chitchat(
            config,
            vec![(
                SERVICE_KEY.to_string(),
                serde_json::to_string(&self_node.service)?,
            )],
            &transport,
        )
        .await?;
        let chitchat = chitchat_handle.chitchat();

        let alive_nodes = Arc::new(RwLock::new(HashSet::new()));
        let alive_nodes_ref = alive_nodes.clone();

        tokio::spawn(async move {
            let mut node_change_receiver = chitchat.lock().await.ready_nodes_watcher();

            while let Some(members_set) = node_change_receiver.next().await {
                let alive_node_ids: HashSet<_> = alive_nodes_ref
                    .read()
                    .await
                    .iter()
                    .map(|member: &Member| member.id.clone())
                    .collect();

                let member_ids: HashSet<_> =
                    members_set.iter().map(|member| member.id.clone()).collect();

                if alive_node_ids != member_ids {
                    let snapshot = chitchat.lock().await.state_snapshot();
                    let mut new_members = Vec::new();
                    for member in members_set {
                        if let Some(state) = snapshot.node_states.get(&member.id) {
                            if let Some(service) = state.get(SERVICE_KEY) {
                                let service: Service = serde_json::from_str(service).unwrap();
                                new_members.push(Member {
                                    service,
                                    id: member.id,
                                });
                            } else {
                                error!("failed to get service");
                            }
                        } else {
                            error!("no state found for node")
                        }
                    }

                    tracing::info!("new members: {:#?}", new_members);
                    let mut write = alive_nodes_ref.write().await;
                    write.clear();

                    for member in new_members {
                        write.insert(member);
                    }
                }
            }
        });

        Ok(Self {
            alive_nodes,
            _chitchat_handle: chitchat_handle,
        })
    }

    pub async fn members(&self) -> Vec<Member> {
        let lock = self.alive_nodes.read().await;
        let mut res = Vec::with_capacity(lock.len());

        for member in lock.iter() {
            res.push(member.clone());
        }

        res
    }
}
