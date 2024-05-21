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

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;

use super::mapper::ApproxCentralityMapper;
use super::{worker::RemoteApproxCentralityWorker, ApproxCentralityTables};
use super::{ApproxCentralityJob, DhtTable as _, Finisher, Meta, Setup};
use crate::ampc::{Coordinator, DefaultDhtTable, DhtConn};
use crate::config::ApproxHarmonicCoordinatorConfig;
use crate::distributed::cluster::Cluster;
use crate::distributed::member::{Member, Service, ShardId};
use crate::webgraph::centrality::{store_csv, store_harmonic, top_nodes, TopNodes};
use crate::Result;

pub struct ApproxCentralitySetup {
    dht: DhtConn<ApproxCentralityTables>,
    workers: Vec<RemoteApproxCentralityWorker>,
    sample_rate: f64,
}

impl ApproxCentralitySetup {
    pub async fn new(cluster: &Cluster, sample_rate: f64) -> Result<Self> {
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
                if let Service::ApproxHarmonicWorker { host, shard } = member.service {
                    Some(RemoteApproxCentralityWorker::new(shard, host))
                } else {
                    None
                }
            })
            .collect::<Result<Vec<RemoteApproxCentralityWorker>>>()?;

        Ok(Self::new_for_dht_members(
            &dht_members,
            workers,
            sample_rate,
        ))
    }
    pub fn new_for_dht_members(
        dht_members: &[(ShardId, SocketAddr)],
        workers: Vec<RemoteApproxCentralityWorker>,
        sample_rate: f64,
    ) -> Self {
        let initial = ApproxCentralityTables {
            meta: DefaultDhtTable::new(dht_members, "meta"),
            centrality: DefaultDhtTable::new(dht_members, "centrality"),
        };

        let dht = DhtConn::new(initial);

        Self {
            dht,
            workers,
            sample_rate,
        }
    }
}

impl Setup for ApproxCentralitySetup {
    type DhtTables = ApproxCentralityTables;

    fn init_dht(&self) -> DhtConn<Self::DhtTables> {
        self.dht.clone()
    }

    fn setup_round(&self, dht: &Self::DhtTables) {
        let meta = dht.meta.get(()).unwrap();

        dht.meta.set(
            (),
            Meta {
                round: meta.round + 1,
                num_samples_per_worker: meta.num_samples_per_worker,
            },
        );
    }

    fn setup_first_round(&self, dht: &Self::DhtTables) {
        let upper_bound_num_nodes: u64 = self.workers.iter().map(|w| w.num_nodes()).sum();
        let num_samples_per_worker =
            ((upper_bound_num_nodes as f64).log2() / self.sample_rate.powi(2)).ceil() as u64
                / (self.workers.len() as u64);

        dht.meta.set(
            (),
            Meta {
                round: 0,
                num_samples_per_worker,
            },
        );
    }
}

pub struct ApproxCentralityFinish;

impl Finisher for ApproxCentralityFinish {
    type Job = ApproxCentralityJob;

    fn is_finished(&self, dht: &ApproxCentralityTables) -> bool {
        dht.meta.get(()).unwrap().round > 0
    }
}

pub fn build(
    dht: &[(ShardId, SocketAddr)],
    workers: Vec<RemoteApproxCentralityWorker>,
    sample_rate: f64,
) -> Coordinator<ApproxCentralityJob> {
    let setup = ApproxCentralitySetup::new_for_dht_members(dht, workers.clone(), sample_rate);

    let mut coord = Coordinator::new(setup, workers.clone());

    for worker in &workers {
        coord = coord.with_mapper(ApproxCentralityMapper::ApproximateCentrality {
            worker_shard: worker.shard(),
        });
    }

    coord
}

struct ClusterInfo {
    // dropping the handle will leave the cluster
    _handle: Cluster,
    dht: Vec<(ShardId, SocketAddr)>,
    workers: Vec<RemoteApproxCentralityWorker>,
}

async fn setup_gossip(config: ApproxHarmonicCoordinatorConfig) -> Result<ClusterInfo> {
    let handle = Cluster::join(
        Member {
            id: config.gossip.cluster_id,
            service: Service::ApproxHarmonicCoordinator { host: config.host },
        },
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
            if let Service::ApproxHarmonicWorker { host, shard } = member.service {
                Some(RemoteApproxCentralityWorker::new(shard, host))
            } else {
                None
            }
        })
        .collect::<Result<Vec<RemoteApproxCentralityWorker>>>()?;

    Ok(ClusterInfo {
        _handle: handle,
        dht,
        workers,
    })
}

pub fn run(config: ApproxHarmonicCoordinatorConfig) -> Result<()> {
    let tokio_conf = config.clone();
    let cluster = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(setup_gossip(tokio_conf))?;

    let jobs = cluster
        .workers
        .iter()
        .map(|worker| ApproxCentralityJob {
            shard: worker.shard(),
            max_distance: config.max_distance,
            all_workers: cluster
                .workers
                .clone()
                .into_iter()
                .map(|w| (w.shard(), w.addr()))
                .collect(),
        })
        .collect();

    let coordinator = build(&cluster.dht, cluster.workers.clone(), config.sample_rate);
    let res = coordinator.run(jobs, ApproxCentralityFinish)?;

    let num_nodes = res.centrality.num_keys();

    let num_samples =
        res.meta.get(()).unwrap().num_samples_per_worker * cluster.workers.len() as u64;

    let output_path = Path::new(&config.output_path);

    let norm = num_nodes as f64 / (num_samples as f64 * (num_nodes as f64 - 1.0));

    let store = store_harmonic(
        res.centrality.iter().map(|(n, c)| (n, f64::from(c) * norm)),
        output_path,
    );

    let top_nodes = top_nodes(&store, TopNodes::Top(1_000_000));

    let ids = top_nodes.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    let id2node: BTreeMap<_, _> = cluster
        .workers
        .iter()
        .flat_map(|w| w.batch_id2node(ids.clone()))
        .collect();

    let top_nodes = top_nodes
        .iter()
        .filter_map(|(id, c)| id2node.get(id).map(|n| (n.clone(), *c)))
        .collect::<Vec<_>>();

    store_csv(top_nodes, output_path.join("harmonic.csv"));

    Ok(())
}
