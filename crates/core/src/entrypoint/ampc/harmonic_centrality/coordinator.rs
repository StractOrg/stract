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
    ampc::{dht::ShardId, prelude::*, Coordinator, DhtConn},
    config::HarmonicCoordinatorConfig,
    distributed::member::Member,
    Result,
};
use std::net::SocketAddr;

use crate::{
    ampc::DefaultDhtTable,
    distributed::{cluster::Cluster, member::Service},
};

use super::{CentralityJob, CentralityMapper, CentralityTables, Meta, RemoteCentralityWorker};

pub struct CentralitySetup {
    dht: DhtConn<CentralityTables>,
}

impl CentralitySetup {
    pub async fn new(cluster: &Cluster) -> Self {
        let members: Vec<_> = cluster
            .members()
            .await
            .into_iter()
            .filter_map(|member| {
                if let Service::Dht { host, shard } = member.service {
                    Some((shard, host))
                } else {
                    None
                }
            })
            .collect();

        Self::new_for_dht_members(&members)
    }

    pub fn new_for_dht_members(members: &[(ShardId, SocketAddr)]) -> Self {
        let initial = CentralityTables {
            counters: DefaultDhtTable::new(members, "counters"),
            meta: DefaultDhtTable::new(members, "meta"),
            centrality: DefaultDhtTable::new(members, "centrality"),
        };

        let dht = DhtConn::new(initial);

        Self { dht }
    }
}

impl Setup for CentralitySetup {
    type DhtTables = CentralityTables;

    fn init_dht(&self) -> DhtConn<Self::DhtTables> {
        self.dht.clone()
    }

    fn setup_round(&self, dht: &Self::DhtTables) {
        dht.meta.set(
            (),
            Meta {
                round_had_changes: false,
            },
        );
    }

    fn setup_first_round(&self, dht: &Self::DhtTables) {
        dht.meta.set(
            (),
            Meta {
                round_had_changes: true, // force first round to run
            },
        );
    }
}

pub struct CentralityFinish;

impl Finisher for CentralityFinish {
    type Job = CentralityJob;

    fn is_finished(&self, dht: &CentralityTables) -> bool {
        !dht.meta.get(()).unwrap().round_had_changes
    }
}

pub fn build(
    dht: &[(ShardId, SocketAddr)],
    workers: Vec<RemoteCentralityWorker>,
) -> Coordinator<CentralityJob> {
    let setup = CentralitySetup::new_for_dht_members(dht);

    Coordinator::new(setup, workers)
        .with_mapper(CentralityMapper::SetupCounters)
        .with_mapper(CentralityMapper::Cardinalities)
        .with_mapper(CentralityMapper::Centralities)
}

struct ClusterInfo {
    // dropping the handle will leave the cluster
    _handle: Cluster,
    dht: Vec<(ShardId, SocketAddr)>,
    workers: Vec<RemoteCentralityWorker>,
}

async fn setup_gossip(config: HarmonicCoordinatorConfig) -> Result<ClusterInfo> {
    let handle = Cluster::join(
        Member {
            id: config.gossip.cluster_id,
            service: Service::HarmonicCoordinator { host: config.host },
        },
        config.gossip.addr,
        config.gossip.seed_nodes.unwrap_or_default(),
    )
    .await?;

    let members = handle.members().await;

    let dht = members
        .iter()
        .filter_map(|member| {
            if let Service::Dht { host, shard } = member.service {
                Some((shard, host))
            } else {
                None
            }
        })
        .collect();

    let workers = members
        .iter()
        .filter_map(|member| {
            if let Service::HarmonicWorker { host, shard } = member.service {
                Some(RemoteCentralityWorker::new(shard, host))
            } else {
                None
            }
        })
        .collect();

    Ok(ClusterInfo {
        _handle: handle,
        dht,
        workers,
    })
}

pub fn run(config: HarmonicCoordinatorConfig) -> Result<()> {
    let tokio_conf = config.clone();
    let cluster = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(setup_gossip(tokio_conf))?;

    let jobs = cluster
        .workers
        .iter()
        .map(|worker| CentralityJob {
            shard: worker.shard(),
        })
        .collect();

    let coordinator = build(&cluster.dht, cluster.workers);
    coordinator.run(jobs, CentralityFinish)?;

    Ok(())
}
