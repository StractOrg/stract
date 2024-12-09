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
    ampc::{JobConn, Worker},
    config::ShortestPathWorkerConfig,
    distributed::{
        member::{Service, ShardId},
        sonic,
    },
    hyperloglog::HyperLogLog,
    webgraph::{self, Webgraph},
    Result,
};
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use super::{impl_worker, updated_nodes::UpdatedNodes, Message, RemoteWorker, ShortestPathJob};

#[derive(Clone)]
pub struct ShortestPathWorker {
    shard: ShardId,
    graph: Arc<Webgraph>,
    changed_nodes: Arc<Mutex<UpdatedNodes>>,
    nodes_sketch: HyperLogLog<4096>,
}

impl ShortestPathWorker {
    pub fn new(graph: Webgraph, shard: ShardId) -> Self {
        let mut nodes_sketch = HyperLogLog::default();

        for node in graph.page_nodes() {
            nodes_sketch.add_u128(node.as_u128());
        }
        let num_nodes = nodes_sketch.size() as u64;

        Self {
            graph: Arc::new(graph),
            shard,
            changed_nodes: Arc::new(Mutex::new(UpdatedNodes::new(num_nodes))),
            nodes_sketch,
        }
    }

    pub fn graph(&self) -> &Webgraph {
        &self.graph
    }

    pub fn shard(&self) -> ShardId {
        self.shard
    }

    pub fn changed_nodes(&self) -> &Arc<Mutex<UpdatedNodes>> {
        &self.changed_nodes
    }

    pub fn nodes_sketch(&self) -> &HyperLogLog<4096> {
        &self.nodes_sketch
    }

    pub fn update_changed_nodes_precision(&self, num_nodes: u64) {
        let mut changed_nodes = self.changed_nodes().lock().unwrap();
        *changed_nodes = UpdatedNodes::new(num_nodes);
    }
}

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct GetNodeSketch;

impl Message<ShortestPathWorker> for GetNodeSketch {
    type Response = HyperLogLog<4096>;

    fn handle(self, worker: &ShortestPathWorker) -> Self::Response {
        worker.nodes_sketch().clone()
    }
}

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct UpdateChangedNodesPrecision(u64);

impl Message<ShortestPathWorker> for UpdateChangedNodesPrecision {
    type Response = ();

    fn handle(self, worker: &ShortestPathWorker) -> Self::Response {
        worker.update_changed_nodes_precision(self.0);
    }
}

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct BatchId2Node(Vec<webgraph::NodeID>);

impl Message<ShortestPathWorker> for BatchId2Node {
    type Response = Vec<(webgraph::NodeID, webgraph::Node)>;

    fn handle(self, worker: &ShortestPathWorker) -> Self::Response {
        self.0
            .iter()
            .filter_map(|id| {
                worker
                    .graph
                    .search(&webgraph::query::Id2NodeQuery::Page(*id))
                    .ok()
                    .flatten()
                    .map(|node| (*id, node))
            })
            .collect()
    }
}

impl_worker!(ShortestPathJob, RemoteShortestPathWorker => ShortestPathWorker, [BatchId2Node, GetNodeSketch, UpdateChangedNodesPrecision]);

#[derive(Clone)]
pub struct RemoteShortestPathWorker {
    shard: ShardId,
    pool: Arc<sonic::ConnectionPool<JobConn<ShortestPathJob>>>,
}

impl RemoteShortestPathWorker {
    pub fn new(shard: ShardId, addr: SocketAddr) -> Result<Self> {
        Ok(Self {
            shard,
            pool: Arc::new(sonic::ConnectionPool::new(addr)?),
        })
    }

    pub fn shard(&self) -> ShardId {
        self.shard
    }

    pub fn addr(&self) -> SocketAddr {
        self.pool.addr()
    }

    pub fn batch_id2node(
        &self,
        ids: Vec<webgraph::NodeID>,
    ) -> Vec<(webgraph::NodeID, webgraph::Node)> {
        self.send(BatchId2Node(ids))
    }

    pub fn get_node_sketch(&self) -> HyperLogLog<4096> {
        self.send(GetNodeSketch)
    }

    pub fn update_changed_nodes_precision(&self, num_nodes: u64) {
        self.send(UpdateChangedNodesPrecision(num_nodes));
    }
}

impl RemoteWorker for RemoteShortestPathWorker {
    type Job = ShortestPathJob;

    fn pool(&self) -> &sonic::ConnectionPool<JobConn<Self::Job>> {
        &self.pool
    }
}

pub fn run(config: ShortestPathWorkerConfig) -> Result<()> {
    tracing::info!("starting worker");
    let tokio_conf = config.clone();

    let graph = Webgraph::builder(config.graph_path, config.shard).open()?;
    let worker = ShortestPathWorker::new(graph, config.shard);
    let service = Service::ShortestPathWorker {
        host: tokio_conf.host,
        shard: tokio_conf.shard,
    };
    crate::start_gossip_cluster_thread(tokio_conf.gossip, Some(service));

    tracing::info!("worker ready");
    worker.run(config.host)?;

    Ok(())
}
