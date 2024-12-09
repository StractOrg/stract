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

use itertools::Itertools;
use url::Url;

use super::mapper::ShortestPathMapper;
use super::{worker::RemoteShortestPathWorker, ShortestPathTables};
use super::{DhtTable as _, Finisher, Meta, Setup, ShortestPathJob};
use crate::ampc::{Coordinator, DefaultDhtTable, DhtConn};
use crate::config::ShortestPathCoordinatorConfig;
use crate::distributed::cluster::Cluster;
use crate::distributed::member::{Member, Service, ShardId};
use crate::hyperloglog::HyperLogLog;
use crate::webpage::url_ext::UrlExt;
use crate::{webgraph, Result};

pub struct ShortestPathSetup {
    dht: DhtConn<ShortestPathTables>,
    source: webgraph::NodeID,
}

impl ShortestPathSetup {
    pub async fn new(cluster: &Cluster, source: webgraph::NodeID) -> Result<Self> {
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

        Ok(Self::new_for_dht_members(&dht_members, source))
    }
    pub fn new_for_dht_members(
        dht_members: &[(ShardId, SocketAddr)],
        source: webgraph::NodeID,
    ) -> Self {
        let initial = ShortestPathTables {
            distances: DefaultDhtTable::new(dht_members, "distances"),
            meta: DefaultDhtTable::new(dht_members, "meta"),
            changed_nodes: DefaultDhtTable::new(dht_members, "changed_nodes"),
        };

        let dht = DhtConn::new(initial);

        Self { dht, source }
    }
}

impl Setup for ShortestPathSetup {
    type DhtTables = ShortestPathTables;

    fn init_dht(&self) -> DhtConn<Self::DhtTables> {
        self.dht.clone()
    }

    fn setup_round(&self, dht: &Self::DhtTables) {
        let meta = dht.meta.get(()).unwrap();

        dht.meta.set(
            (),
            Meta {
                round: meta.round + 1,
                round_had_changes: false,
            },
        );
    }

    fn setup_first_round(&self, dht: &Self::DhtTables) {
        dht.distances.set(self.source, 0);
        dht.meta.set(
            (),
            Meta {
                round_had_changes: true,
                round: 0,
            },
        );
    }
}

pub struct ShortestPathFinish {
    max_distance: Option<u64>,
}

impl Finisher for ShortestPathFinish {
    type Job = ShortestPathJob;

    fn is_finished(&self, dht: &ShortestPathTables) -> bool {
        let meta = dht.meta.get(()).unwrap();
        if let Some(max_distance) = self.max_distance {
            if meta.round >= max_distance {
                return true;
            }
        }

        !meta.round_had_changes
    }
}

pub fn build(
    dht: &[(ShardId, SocketAddr)],
    workers: Vec<RemoteShortestPathWorker>,
    source: webgraph::NodeID,
) -> Coordinator<ShortestPathJob> {
    let setup = ShortestPathSetup::new_for_dht_members(dht, source);

    Coordinator::new(setup, workers.clone())
        .with_mapper(ShortestPathMapper::RelaxEdges)
        .with_mapper(ShortestPathMapper::UpdateChangedNodes)
}

struct ClusterInfo {
    // dropping the handle will leave the cluster
    _handle: Cluster,
    dht: Vec<(ShardId, SocketAddr)>,
    workers: Vec<RemoteShortestPathWorker>,
}

async fn setup_gossip(config: ShortestPathCoordinatorConfig) -> Result<ClusterInfo> {
    let handle = Cluster::join(
        Member::new(Service::ShortestPathCoordinator { host: config.host }),
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
            if let Service::ShortestPathWorker { host, shard } = member.service {
                Some(RemoteShortestPathWorker::new(shard, host))
            } else {
                None
            }
        })
        .collect::<Result<Vec<RemoteShortestPathWorker>>>()?;

    Ok(ClusterInfo {
        _handle: handle,
        dht,
        workers,
    })
}

pub fn run(config: ShortestPathCoordinatorConfig) -> Result<()> {
    let source = webgraph::Node::from(Url::robust_parse(&config.source)?).id();
    let tokio_conf = config.clone();
    let cluster = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(setup_gossip(tokio_conf))?;

    let sketch = cluster
        .workers
        .iter()
        .map(|worker| worker.get_node_sketch())
        .fold(HyperLogLog::default(), |mut acc, sketch| {
            acc.merge(&sketch);
            acc
        });

    let num_nodes = sketch.size() as u64;

    for worker in cluster.workers.iter() {
        worker.update_changed_nodes_precision(num_nodes);
    }

    let jobs: Vec<_> = cluster
        .workers
        .iter()
        .map(|worker| ShortestPathJob {
            shard: worker.shard(),
            source,
        })
        .collect();

    tracing::info!("starting {} jobs", jobs.len());

    let coordinator = build(&cluster.dht, cluster.workers.clone(), source);
    let res = coordinator.run(
        jobs,
        ShortestPathFinish {
            max_distance: config.max_distance,
        },
    )?;

    let output_path = Path::new(&config.output_path);

    if !output_path.exists() {
        std::fs::create_dir_all(output_path)?;
    }

    let mut writer = csv::Writer::from_writer(
        std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(output_path.join("distances.csv"))?,
    );
    let mut distances = res.distances.iter().collect::<Vec<_>>();
    distances.sort_by_key(|(_id, distance)| *distance);

    let id2node: BTreeMap<_, _> = cluster
        .workers
        .iter()
        .flat_map(|w| {
            distances
                .iter()
                .chunks(10_000)
                .into_iter()
                .flat_map(move |c| {
                    let ids = c.map(|(id, _)| *id).collect::<Vec<_>>();
                    w.batch_id2node(ids)
                })
                .collect::<Vec<_>>()
        })
        .collect();

    for (id, distance) in distances {
        if let Some(node) = id2node.get(&id) {
            writer.write_record(&[node.as_str().to_string(), distance.to_string()])?;
        }
    }

    Ok(())
}
