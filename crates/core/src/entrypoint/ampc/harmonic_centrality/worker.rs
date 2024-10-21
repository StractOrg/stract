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

use bloom::U64BloomFilter;

use crate::{
    ampc::{prelude::*, JobConn},
    config::HarmonicWorkerConfig,
    distributed::{
        member::{Service, ShardId},
        sonic,
    },
    webgraph::{self, Webgraph},
    Result,
};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Arc, Mutex,
    },
};

use super::CentralityJob;

pub struct CentralityWorker {
    shard: ShardId,
    graph: Webgraph,
    changed_nodes: Arc<Mutex<U64BloomFilter>>,
    round: AtomicU64,
    has_updated_meta_for_round: AtomicBool,
}

impl CentralityWorker {
    pub fn new(shard: ShardId, graph: Webgraph) -> Self {
        let num_nodes = graph.host_nodes().len() as u64;
        let mut changed_nodes = U64BloomFilter::new(num_nodes, 0.05);

        changed_nodes.fill();

        Self {
            shard,
            graph,
            changed_nodes: Arc::new(Mutex::new(changed_nodes)),
            round: AtomicU64::new(0),
            has_updated_meta_for_round: AtomicBool::new(false),
        }
    }

    pub fn setup_changed_nodes(&self, upper_bound_num_nodes: u64) {
        let mut new_changed_nodes = U64BloomFilter::new(upper_bound_num_nodes, 0.05);

        new_changed_nodes.fill();

        *self.changed_nodes.lock().unwrap() = new_changed_nodes;
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

    pub fn has_updated_meta_for_round(&self) -> bool {
        self.has_updated_meta_for_round
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn set_has_updated_meta_for_round(&self, value: bool) {
        self.has_updated_meta_for_round
            .store(value, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn inc_round(&self) -> u64 {
        self.round
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct NumNodes;

impl Message<CentralityWorker> for NumNodes {
    type Response = u64;

    fn handle(self, worker: &CentralityWorker) -> Self::Response {
        worker.graph.host_nodes().len() as u64
    }
}

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct BatchId2Node(Vec<webgraph::NodeID>);

impl Message<CentralityWorker> for BatchId2Node {
    type Response = Vec<(webgraph::NodeID, webgraph::Node)>;

    fn handle(self, worker: &CentralityWorker) -> Self::Response {
        self.0
            .iter()
            .filter_map(|id| {
                worker
                    .graph
                    .search(&webgraph::query::Id2NodeQuery::Host(*id))
                    .ok()
                    .flatten()
                    .map(|node| (*id, node))
            })
            .collect()
    }
}

impl_worker!(CentralityJob, RemoteCentralityWorker => CentralityWorker, [NumNodes, BatchId2Node]);

#[derive(Clone)]
pub struct RemoteCentralityWorker {
    shard: ShardId,
    pool: Arc<sonic::ConnectionPool<JobConn<CentralityJob>>>,
}

impl RemoteCentralityWorker {
    pub fn new(shard: ShardId, addr: SocketAddr) -> Result<Self> {
        Ok(Self {
            shard,
            pool: Arc::new(sonic::ConnectionPool::new(addr)?),
        })
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

    fn pool(&self) -> &crate::distributed::sonic::ConnectionPool<JobConn<Self::Job>> {
        &self.pool
    }
}

pub fn run(config: HarmonicWorkerConfig) -> Result<()> {
    let tokio_conf = config.clone();

    let graph = Webgraph::builder(config.graph_path, config.shard).open()?;
    let worker = CentralityWorker::new(config.shard, graph);
    let service = Service::HarmonicWorker {
        host: tokio_conf.host,
        shard: tokio_conf.shard,
    };
    crate::start_gossip_cluster_thread(tokio_conf.gossip, Some(service));

    worker.run(config.host)?;

    Ok(())
}
