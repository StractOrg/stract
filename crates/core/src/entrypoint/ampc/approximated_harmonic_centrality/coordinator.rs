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

use std::net::SocketAddr;
use std::path::Path;

use indicatif::ProgressIterator;
use rustc_hash::FxHashMap;

use crate::ampc::dht::ShardId;
use crate::ampc::{DhtTable as _, DhtTables as _};
use crate::config::ApproxHarmonicCoordinatorConfig;
use crate::distributed::cluster::Cluster;
use crate::distributed::member::Service;
use crate::entrypoint::ampc::shortest_path::coordinator::ShortestPathFinish;
use crate::entrypoint::ampc::shortest_path::{self, RemoteShortestPathWorker, ShortestPathJob};
use crate::hyperloglog::HyperLogLog;
use crate::kahan_sum::KahanSum;
use crate::webgraph::centrality::{store_harmonic, TopNodes};
use crate::{webgraph, Result};

struct ClusterInfo {
    // dropping the handle will leave the cluster
    _handle: Cluster,
    dht: Vec<(ShardId, SocketAddr)>,
    workers: Vec<RemoteShortestPathWorker>,
}

async fn setup_gossip(config: ApproxHarmonicCoordinatorConfig) -> Result<ClusterInfo> {
    let handle = Cluster::join_as_spectator(
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

fn num_samples(num_nodes: u64, sample_rate: f64) -> u64 {
    ((num_nodes as f64).log2() / sample_rate.powi(2)).ceil() as u64
}

pub fn run(config: ApproxHarmonicCoordinatorConfig) -> Result<()> {
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

    let num_samples = num_samples(num_nodes, config.sample_rate);
    tracing::info!("sampling {} nodes", num_samples);

    let norm = 1.0 / ((num_samples - 1) as f64);

    let num_samples_per_worker = num_samples.div_ceil(cluster.workers.len() as u64);

    let sampled_nodes: Vec<_> = cluster
        .workers
        .iter()
        .flat_map(|worker| worker.sample_nodes(num_samples_per_worker))
        .collect();

    let mut centralities: FxHashMap<webgraph::NodeID, KahanSum> = FxHashMap::default();

    for source in sampled_nodes.into_iter().progress() {
        let jobs: Vec<_> = cluster
            .workers
            .iter()
            .map(|worker| ShortestPathJob {
                shard: worker.shard(),
                source,
            })
            .collect();

        let coordinator =
            shortest_path::coordinator::build(&cluster.dht, cluster.workers.clone(), source);

        let res = coordinator.run(
            jobs,
            ShortestPathFinish {
                max_distance: Some(config.max_distance as u64),
            },
        )?;

        for (node, distance) in res.distances.iter() {
            let centrality = KahanSum::from((1.0 / distance as f64) * norm);
            centralities
                .entry(node)
                .and_modify(|sum| *sum += centrality)
                .or_insert(centrality);
        }

        res.drop_tables();
    }

    let output_path = Path::new(&config.output_path);

    let store = store_harmonic(
        centralities.iter().map(|(id, c)| (*id, f64::from(*c))),
        output_path,
    );

    let top_nodes = webgraph::centrality::top_nodes(&store, TopNodes::Top(1_000_000));

    let ids = top_nodes.iter().map(|(id, _)| *id).collect::<Vec<_>>();

    let id2node: FxHashMap<_, _> = cluster
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

    webgraph::centrality::store_csv(top_nodes, output_path.join("harmonic.csv"));

    Ok(())
}
