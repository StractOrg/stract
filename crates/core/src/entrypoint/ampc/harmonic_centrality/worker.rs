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
    bloom::BloomFilter,
    config::HarmonicWorkerConfig,
    distributed::{
        cluster::Cluster,
        member::{Member, Service, ShardId},
    },
    webgraph::Webgraph,
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
    changed_nodes: Arc<Mutex<BloomFilter>>,
    round: AtomicU64,
}

impl CentralityWorker {
    pub fn new(shard: ShardId, graph: Webgraph) -> Self {
        let num_nodes = graph.estimate_num_nodes() as u64;
        let mut changed_nodes = BloomFilter::new(num_nodes, 0.05);

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

    pub fn changed_nodes(&self) -> &Arc<Mutex<BloomFilter>> {
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

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct GetShard;

impl Message<CentralityWorker> for GetShard {
    type Response = ShardId;

    fn handle(self, worker: &CentralityWorker) -> Self::Response {
        worker.shard
    }
}

impl_worker!(CentralityJob, RemoteCentralityWorker => CentralityWorker, [GetShard,]);

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
}

impl RemoteWorker for RemoteCentralityWorker {
    type Job = CentralityJob;

    fn remote_addr(&self) -> SocketAddr {
        self.addr
    }
}

async fn gossip_cluster(config: HarmonicWorkerConfig) -> Result<Cluster> {
    Cluster::join(
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
    .await
}

pub fn run(config: HarmonicWorkerConfig) -> Result<()> {
    let tokio_conf = config.clone();

    let graph = Webgraph::builder(config.graph_path).open();
    let worker = CentralityWorker::new(config.shard, graph);

    let _cluster = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(gossip_cluster(tokio_conf))?;

    worker.run(config.host)?;

    Ok(())
}
