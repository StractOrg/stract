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

use crate::ampc::{
    dht::{F64Add, UpsertAction},
    prelude::*,
    DhtConn,
};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use crate::{
    ampc::{dht::upsert::HyperLogLog64Upsert, DefaultDhtTable},
    bloom::BloomFilter,
    distributed::{cluster::Cluster, member::Service},
    hyperloglog::HyperLogLog,
    webgraph::{self, Edge, Webgraph},
};

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct Meta {
    round_had_changes: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CentralityTables {
    counters: DefaultDhtTable<webgraph::NodeID, HyperLogLog<64>>,
    meta: DefaultDhtTable<(), Meta>,
    centrality: DefaultDhtTable<webgraph::NodeID, f64>,
}

impl_dht_tables!(CentralityTables, [counters, meta, centrality]);

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct CentralityJob {
    shard: u64,
}

impl Job for CentralityJob {
    type DhtTables = CentralityTables;
    type Worker = CentralityWorker;
    type Mapper = CentralityMapper;

    fn is_schedulable(&self, worker: &RemoteCentralityWorker) -> bool {
        self.shard == worker.shard()
    }
}

struct CentralitySetup {
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

        let initial = CentralityTables {
            counters: DefaultDhtTable::new(&members, "counters"),
            meta: DefaultDhtTable::new(&members, "meta"),
            centrality: DefaultDhtTable::new(&members, "centrality"),
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
}

struct CentralityWorker {
    shard: u64,
    graph: Webgraph,
    changed_nodes: Arc<Mutex<BloomFilter>>,
    round: AtomicU64,
}

impl CentralityWorker {
    fn new(shard: u64, graph: Webgraph) -> Self {
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
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct GetShard;

impl Message<CentralityWorker> for GetShard {
    type Response = u64;

    fn handle(self, worker: &CentralityWorker) -> Self::Response {
        worker.shard
    }
}

impl_worker!(CentralityJob, RemoteCentralityWorker => CentralityWorker, [GetShard,]);

struct RemoteCentralityWorker {
    shard: u64,
    addr: SocketAddr,
}

impl RemoteCentralityWorker {
    fn shard(&self) -> u64 {
        self.shard
    }
}

impl RemoteWorker for RemoteCentralityWorker {
    type Job = CentralityJob;

    fn remote_addr(&self) -> SocketAddr {
        self.addr
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
enum CentralityMapper {
    Cardinalities,
    Centralities,
}

impl CentralityMapper {
    /// get old values from prev dht using edge.from where edge.from in changed_nodes
    fn get_old_counters(
        batch: &[Edge<()>],
        changed_nodes: &Mutex<BloomFilter>,
        dht: &DhtConn<CentralityTables>,
    ) -> BTreeMap<webgraph::NodeID, HyperLogLog<64>> {
        let changed_nodes = changed_nodes.lock().unwrap();

        dht.prev()
            .counters
            .batch_get(
                batch
                    .iter()
                    .filter_map(|edge| {
                        if changed_nodes.contains(edge.from.as_u64()) {
                            Some(edge.from)
                        } else {
                            None
                        }
                    })
                    .collect(),
            )
            .into_iter()
            .collect()
    }

    /// upsert old edge.from `old_counters` into edge.to in dht.next,
    /// thereby updating their hyperloglog counters
    fn update_counters(
        batch: &[Edge<()>],
        changed_nodes: &Mutex<BloomFilter>,
        old_counters: BTreeMap<webgraph::NodeID, HyperLogLog<64>>,
        dht: &DhtConn<CentralityTables>,
    ) -> Vec<(webgraph::NodeID, UpsertAction)> {
        let mut old_counters = old_counters;
        let changed_nodes = changed_nodes.lock().unwrap();

        dht.next().counters.batch_upsert(
            HyperLogLog64Upsert,
            batch
                .iter()
                .filter(|edge| changed_nodes.contains(edge.from.as_u64()))
                .map(|edge| {
                    let mut counter = old_counters
                        .remove(&edge.from)
                        .unwrap_or_else(HyperLogLog::default);
                    counter.add(edge.from.as_u64());
                    (edge.to, counter)
                })
                .collect(),
        )
    }

    /// update new bloom filter with the nodes that changed
    fn update_changed_nodes(
        changes: &[(webgraph::NodeID, UpsertAction)],
        new_changed_nodes: &Mutex<BloomFilter>,
    ) {
        let mut new_changed_nodes = new_changed_nodes.lock().unwrap();

        for (node, upsert_res) in changes {
            if let UpsertAction::Merged = upsert_res {
                new_changed_nodes.insert(node.as_u64());
            }
        }
    }

    fn update_dht(
        batch: &[Edge<()>],
        changed_nodes: &Mutex<BloomFilter>,
        new_changed_nodes: &Mutex<BloomFilter>,
        dht: &DhtConn<CentralityTables>,
    ) {
        let old_counters = Self::get_old_counters(batch, changed_nodes, dht);
        let changes = Self::update_counters(batch, changed_nodes, old_counters, dht);

        Self::update_changed_nodes(&changes, new_changed_nodes);

        // if any nodes changed, indicate in dht that we aren't finished yet
        if changes.iter().any(|(_, upsert_res)| match upsert_res {
            UpsertAction::Merged => true,
            UpsertAction::NoChange => false,
            UpsertAction::Inserted => true,
        }) {
            dht.next().meta.set(
                (),
                Meta {
                    round_had_changes: true,
                },
            )
        }
    }

    fn update_centralities(
        nodes: &[webgraph::NodeID],
        round: u64,
        dht: &DhtConn<CentralityTables>,
    ) {
        let old_counters: BTreeMap<_, _> = dht
            .prev()
            .counters
            .batch_get(nodes.to_vec())
            .into_iter()
            .collect();
        let new_counters: BTreeMap<_, _> = dht
            .next()
            .counters
            .batch_get(nodes.to_vec())
            .into_iter()
            .collect();

        let mut delta = Vec::with_capacity(nodes.len());

        for node in nodes {
            let old_size = old_counters.get(node).map(|s| s.size() as u64).unwrap_or(0);
            let new_size = new_counters.get(node).map(|s| s.size() as u64).unwrap_or(0);

            if let Some(d) = new_size.checked_sub(old_size) {
                delta.push((*node, d as f64 / (round + 1) as f64));
            }
        }

        dht.next().centrality.batch_upsert(F64Add, delta);
    }

    fn map_cardinalities(worker: &CentralityWorker, dht: &DhtConn<CentralityTables>) {
        const BATCH_SIZE: usize = 16_384;
        let new_changed_nodes = Arc::new(Mutex::new(BloomFilter::empty_from(
            &worker.changed_nodes.lock().unwrap(),
        )));

        let pool = rayon::ThreadPoolBuilder::new().build().unwrap();

        pool.scope(|s| {
            let mut batch = Vec::with_capacity(BATCH_SIZE);

            for edge in worker.graph.edges() {
                batch.push(edge);
                if batch.len() >= BATCH_SIZE {
                    let changed_nodes = Arc::clone(&worker.changed_nodes);
                    let new_changed_nodes = Arc::clone(&new_changed_nodes);
                    let update_batch = batch.clone();

                    s.spawn(move |_| {
                        Self::update_dht(&update_batch, &changed_nodes, &new_changed_nodes, dht)
                    });

                    batch.clear();
                }
            }

            if !batch.is_empty() {
                let changed_nodes = Arc::clone(&worker.changed_nodes);
                let new_changed_nodes = Arc::clone(&new_changed_nodes);
                let update_batch = batch.clone();

                s.spawn(move |_| {
                    Self::update_dht(&update_batch, &changed_nodes, &new_changed_nodes, dht)
                });
            }
        });
        *worker.changed_nodes.lock().unwrap() = new_changed_nodes.lock().unwrap().clone();
    }

    fn map_centralities(worker: &CentralityWorker, dht: &DhtConn<CentralityTables>) {
        const BATCH_SIZE: usize = 16_384;
        let pool = rayon::ThreadPoolBuilder::new().build().unwrap();

        let round = worker.round.fetch_add(1, Ordering::Relaxed);

        // count cardinality of hyperloglogs in dht.next and update count after all mappers are done
        pool.scope(|s| {
            let mut batch = Vec::with_capacity(BATCH_SIZE);
            let changed_nodes = worker.changed_nodes.lock().unwrap();
            for node in worker
                .graph
                .nodes()
                .filter(|node| changed_nodes.contains(node.as_u64()))
            {
                batch.push(node);
                if batch.len() >= BATCH_SIZE {
                    let update_batch = batch.clone();
                    s.spawn(move |_| Self::update_centralities(&update_batch, round, dht));

                    batch.clear();
                }
            }

            if !batch.is_empty() {
                s.spawn(move |_| Self::update_centralities(&batch, round, dht));
            }
        });
    }
}

impl Mapper for CentralityMapper {
    type Job = CentralityJob;

    fn map(&self, _: Self::Job, worker: &CentralityWorker, dht: &DhtConn<CentralityTables>) {
        match self {
            CentralityMapper::Cardinalities => Self::map_cardinalities(worker, dht),
            CentralityMapper::Centralities => Self::map_centralities(worker, dht),
        }
    }
}

struct CentralityFinish {}

impl Finisher for CentralityFinish {
    type Job = CentralityJob;

    fn is_finished(&self, dht: &CentralityTables) -> bool {
        dht.meta.get(()).unwrap().round_had_changes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_triangle_graph() {
        todo!()
    }
}
