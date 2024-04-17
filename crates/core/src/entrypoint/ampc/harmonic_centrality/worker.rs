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

use crate::{
    ampc::prelude::*,
    bloom::U64BloomFilter,
    config::HarmonicWorkerConfig,
    distributed::{
        cluster::Cluster,
        member::{Member, Service, ShardId},
    },
    webgraph::{self, Webgraph},
    Result,
};
use std::{
    net::SocketAddr,
    sync::{atomic::AtomicU64, Arc, Mutex},
};

use super::CentralityJob;

pub struct CentralityWorker {
    shard: ShardId,
    graph: Webgraph,
    changed_nodes: Arc<Mutex<U64BloomFilter>>,
    round: AtomicU64,
}

impl CentralityWorker {
    pub fn new(shard: ShardId, graph: Webgraph) -> Self {
        let num_nodes = graph.estimate_num_nodes() as u64;
        let mut changed_nodes = U64BloomFilter::new(num_nodes, 0.05);

        for node in graph.nodes() {
            changed_nodes.insert(node.as_u64());
        }

        Self {
            shard,
            graph,
            changed_nodes: Arc::new(Mutex::new(changed_nodes)),
            round: AtomicU64::new(0),
        }
    }

    pub fn shard(&self) -> ShardId {
        self.shard
    }

    pub fn graph(&self) -> &Webgraph {
        &self.graph
    }

    pub fn changed_nodes(&self) -> &Arc<Mutex<U64BloomFilter>> {
        &self.changed_nodes
    }

    pub fn round(&self) -> u64 {
        self.round.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn inc_round(&self) -> u64 {
        self.round
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct GetShard;

impl Message<CentralityWorker> for GetShard {
    type Response = ShardId;

    fn handle(self, worker: &CentralityWorker) -> Self::Response {
        worker.shard
    }
}
#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct NumNodes;

impl Message<CentralityWorker> for NumNodes {
    type Response = u64;

    fn handle(self, worker: &CentralityWorker) -> Self::Response {
        worker.graph.estimate_num_nodes() as u64
    }
}

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct BatchId2Node(Vec<webgraph::NodeID>);

impl Message<CentralityWorker> for BatchId2Node {
    type Response = Vec<(webgraph::NodeID, webgraph::Node)>;

    fn handle(self, worker: &CentralityWorker) -> Self::Response {
        self.0
            .iter()
            .filter_map(|id| worker.graph.id2node(id).map(|node| (*id, node)))
            .collect()
    }
}

impl_worker!(CentralityJob, RemoteCentralityWorker => CentralityWorker, [GetShard, NumNodes, BatchId2Node]);

#[derive(Clone)]
pub struct RemoteCentralityWorker {
    shard: ShardId,
    addr: SocketAddr,
}

impl RemoteCentralityWorker {
    pub fn new(shard: ShardId, addr: SocketAddr) -> Self {
        Self { shard, addr }
    }

    pub fn shard(&self) -> ShardId {
        self.shard
    }

    pub fn num_nodes(&self) -> u64 {
        self.send(NumNodes)
    }

    pub fn batch_id2node(
        &self,
        id: Vec<webgraph::NodeID>,
    ) -> Vec<(webgraph::NodeID, webgraph::Node)> {
        self.send(BatchId2Node(id))
    }
}

impl RemoteWorker for RemoteCentralityWorker {
    type Job = CentralityJob;

    fn remote_addr(&self) -> SocketAddr {
        self.addr
    }
}

fn start_gossip_cluster_thread(config: HarmonicWorkerConfig) {
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let _cluster = Cluster::join(
                Member {
                    id: config.gossip.cluster_id,
                    service: Service::HarmonicWorker {
                        host: config.host,
                        shard: config.shard,
                    },
                },
                config.gossip.addr,
                config.gossip.seed_nodes.unwrap_or_default(),
            )
            .await;

            // need to keep tokio runtime alive
            // otherwise the spawned task in Cluster::join will be dropped
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });
    });
}

pub fn run(config: HarmonicWorkerConfig) -> Result<()> {
    let tokio_conf = config.clone();

    let graph = Webgraph::builder(config.graph_path).open();
    let worker = CentralityWorker::new(config.shard, graph);
    start_gossip_cluster_thread(tokio_conf);

    worker.run(config.host)?;

    Ok(())
}
