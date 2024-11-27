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
    ampc::JobConn,
    config::ApproxHarmonicWorkerConfig,
    distributed::{
        member::{Service, ShardId},
        sonic,
    },
    webgraph::{self, Webgraph},
    Result,
};
use std::{net::SocketAddr, sync::Arc};

use super::{impl_worker, ApproxCentralityJob, Message, RemoteWorker, Worker as _};

#[derive(Clone)]
pub struct ApproxCentralityWorker {
    shard: ShardId,
    graph: Arc<Webgraph>,
}

impl ApproxCentralityWorker {
    pub fn new(graph: Webgraph, shard: ShardId) -> Self {
        Self {
            graph: Arc::new(graph),
            shard,
        }
    }

    pub fn graph(&self) -> &Webgraph {
        &self.graph
    }

    pub fn shard(&self) -> ShardId {
        self.shard
    }
}

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct NumNodes;

impl Message<ApproxCentralityWorker> for NumNodes {
    type Response = u64;

    fn handle(self, worker: &ApproxCentralityWorker) -> Self::Response {
        worker.graph.page_nodes().len() as u64
    }
}

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct BatchId2Node(Vec<webgraph::NodeID>);

impl Message<ApproxCentralityWorker> for BatchId2Node {
    type Response = Vec<(webgraph::NodeID, webgraph::Node)>;

    fn handle(self, worker: &ApproxCentralityWorker) -> Self::Response {
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

impl_worker!(ApproxCentralityJob, RemoteApproxCentralityWorker => ApproxCentralityWorker, [NumNodes, BatchId2Node]);

#[derive(Clone)]
pub struct RemoteApproxCentralityWorker {
    shard: ShardId,
    pool: Arc<sonic::ConnectionPool<JobConn<ApproxCentralityJob>>>,
}

impl RemoteApproxCentralityWorker {
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

    pub fn num_nodes(&self) -> u64 {
        self.send(NumNodes)
    }

    pub fn batch_id2node(
        &self,
        ids: Vec<webgraph::NodeID>,
    ) -> Vec<(webgraph::NodeID, webgraph::Node)> {
        self.send(BatchId2Node(ids))
    }
}

impl RemoteWorker for RemoteApproxCentralityWorker {
    type Job = ApproxCentralityJob;

    fn pool(&self) -> &sonic::ConnectionPool<JobConn<Self::Job>> {
        &self.pool
    }
}

pub fn run(config: ApproxHarmonicWorkerConfig) -> Result<()> {
    tracing::info!("starting worker");
    let tokio_conf = config.clone();

    let graph = Webgraph::builder(config.graph_path, config.shard).open()?;
    let worker = ApproxCentralityWorker::new(graph, config.shard);
    let service = Service::ApproxHarmonicWorker {
        host: tokio_conf.host,
        shard: tokio_conf.shard,
    };
    crate::start_gossip_cluster_thread(tokio_conf.gossip, Some(service));

    tracing::info!("worker ready");
    worker.run(config.host)?;

    Ok(())
}
