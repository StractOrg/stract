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
    webgraph::centrality::{store_csv, store_harmonic, top_nodes, TopNodes},
    Result,
};
use std::{collections::BTreeMap, net::SocketAddr, path::Path};

use crate::{
    ampc::DefaultDhtTable,
    distributed::{cluster::Cluster, member::Service},
};

use super::{CentralityJob, CentralityMapper, CentralityTables, Meta, RemoteCentralityWorker};

pub struct CentralitySetup {
    dht: DhtConn<CentralityTables>,
    workers: Vec<RemoteCentralityWorker>,
}

impl CentralitySetup {
    pub async fn new(cluster: &Cluster) -> Result<Self> {
        let dht_members: Vec<_> = cluster
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

        let workers = cluster
            .members()
            .await
            .into_iter()
            .filter_map(|member| {
                if let Service::HarmonicWorker { host, shard } = member.service {
                    Some(RemoteCentralityWorker::new(shard, host))
                } else {
                    None
                }
            })
            .collect::<Result<Vec<RemoteCentralityWorker>>>()?;

        Ok(Self::new_for_dht_members(&dht_members, workers))
    }

    pub fn new_for_dht_members(
        dht_members: &[(ShardId, SocketAddr)],
        workers: Vec<RemoteCentralityWorker>,
    ) -> Self {
        let initial = CentralityTables {
            counters: DefaultDhtTable::new(dht_members, "counters"),
            meta: DefaultDhtTable::new(dht_members, "meta"),
            centrality: DefaultDhtTable::new(dht_members, "centrality"),
            changed_nodes: DefaultDhtTable::new(dht_members, "changed_nodes"),
        };

        let dht = DhtConn::new(initial);

        Self { dht, workers }
    }
}

impl Setup for CentralitySetup {
    type DhtTables = CentralityTables;

    fn init_dht(&self) -> DhtConn<Self::DhtTables> {
        self.dht.clone()
    }

    fn setup_round(&self, dht: &Self::DhtTables) {
        let mut meta = dht.meta.get(()).unwrap();
        meta.round_had_changes = false;

        dht.meta.set((), meta);
    }

    fn setup_first_round(&self, dht: &Self::DhtTables) {
        let upper_bound_num_nodes = self.workers.iter().map(|w| w.num_nodes()).sum();

        dht.meta.set(
            (),
            Meta {
                round_had_changes: true, // force first round to run
                upper_bound_num_nodes,
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
    let setup = CentralitySetup::new_for_dht_members(dht, workers.clone());

    Coordinator::new(setup, workers)
        .with_mapper(CentralityMapper::SetupCounters)
        .with_mapper(CentralityMapper::SetupBloom)
        .with_mapper(CentralityMapper::Cardinalities)
        .with_mapper(CentralityMapper::SaveBloom)
        .with_mapper(CentralityMapper::UpdateBloom)
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
        Member::new(Service::HarmonicCoordinator { host: config.host }),
        config.gossip.addr,
        config.gossip.seed_nodes.unwrap_or_default(),
    )
    .await?;

    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

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
        .collect::<Result<Vec<RemoteCentralityWorker>>>()?;

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

    let coordinator = build(&cluster.dht, cluster.workers.clone());
    let res = coordinator.run(jobs, CentralityFinish)?;

    let num_nodes = res.counters.num_keys();
    let output_path = Path::new(&config.output_path);

    let store = store_harmonic(
        res.centrality
            .iter()
            .map(|(n, c)| (n, f64::from(c) / (num_nodes - 1) as f64)),
        output_path,
    );

    let top_nodes = top_nodes(&store, TopNodes::Top(1_000_000));

    let ids = top_nodes.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    let id2node: BTreeMap<_, _> = cluster
        .workers
        .iter()
        .flat_map(|w| {
            ids.clone()
                .chunks(10_000)
                .flat_map(move |c| w.batch_id2node(c.to_vec()))
                .collect::<Vec<_>>()
        })
        .collect();

    let top_nodes = top_nodes
        .iter()
        .filter_map(|(id, c)| id2node.get(id).map(|n| (n.clone(), *c)))
        .collect::<Vec<_>>();

    store_csv(top_nodes, output_path.join("harmonic.csv"));

    Ok(())
}
