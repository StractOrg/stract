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

use std::{collections::BTreeMap, sync::Arc};

use anyhow::bail;
use openraft::error::InitializeError;
use tracing::info;

use crate::{ampc::dht, config::DhtConfig, Result};

pub async fn run(config: DhtConfig) -> Result<()> {
    let raft_config = openraft::Config::default();
    let raft_config = Arc::new(raft_config.validate()?);

    let log_store = dht::log_store::LogStore::<dht::TypeConfig>::default();
    let state_machine_store = Arc::new(dht::store::StateMachineStore::default());

    let network = dht::network::Network;

    let raft = openraft::Raft::new(
        config.node_id,
        raft_config,
        network,
        log_store,
        state_machine_store.clone(),
    )
    .await?;

    let server = dht::Server::new(raft.clone(), state_machine_store)
        .bind(config.host)
        .await?;

    match config.seed_node {
        Some(seed) => {
            let client = dht::RaftClient::new(seed).await?;
            client.join(config.node_id, config.host).await?;

            info!("Joined cluster with node_id: {}", config.node_id);
        }
        None => {
            let members: BTreeMap<u64, _> =
                BTreeMap::from([(config.node_id, openraft::BasicNode::new(config.host))]);

            if let Err(e) = raft.initialize(members.clone()).await {
                match e {
                    openraft::error::RaftError::APIError(e) => match e {
                        InitializeError::NotAllowed(_) => {}
                        InitializeError::NotInMembers(_) => bail!(e),
                    },
                    openraft::error::RaftError::Fatal(_) => bail!(e),
                }
            }

            info!("Initialized cluster with node_id: {}", config.node_id);
        }
    }

    loop {
        server.accept().await?;
    }
}

#[cfg(test)]
pub mod tests {
    use std::net::SocketAddr;

    use crate::free_socket_addr;

    use self::dht::ShardId;

    use super::*;

    pub fn setup() -> (ShardId, SocketAddr) {
        let (tx, rx) = crossbeam_channel::unbounded();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let shard = ShardId::new(1);

            rt.block_on(async {
                let addr = free_socket_addr();
                tx.send((shard, addr)).unwrap();

                run(DhtConfig {
                    node_id: 1,
                    host: addr,
                    seed_node: None,
                    shard,
                })
                .await
                .unwrap();
            })
        });

        rx.recv().unwrap()
    }
}
