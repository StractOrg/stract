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

use indicatif::ParallelProgressIterator;
use itertools::Itertools;
use rustc_hash::{FxHashMap, FxHashSet};
use std::{cmp, collections::BTreeMap};

use super::{worker::ApproxCentralityWorker, ApproxCentralityJob, Mapper};
use crate::{
    ampc::dht::upsert,
    distributed::member::ShardId,
    entrypoint::ampc::approximated_harmonic_centrality::{
        worker::RemoteApproxCentralityWorker, DhtTable,
    },
    kahan_sum::KahanSum,
    webgraph,
};
use rayon::prelude::*;

const BATCH_SIZE: usize = 1024;

#[derive(Debug, Clone, bincode::Decode, bincode::Encode)]
pub enum ApproxCentralityMapper {
    ApproximateCentrality { worker_shard: ShardId },
}

struct Workers {
    worker: ApproxCentralityWorker,
    remote_workers: Vec<RemoteApproxCentralityWorker>,
}

impl Workers {
    fn new(
        worker: ApproxCentralityWorker,
        remote_workers: Vec<RemoteApproxCentralityWorker>,
    ) -> Self {
        Self {
            worker,
            remote_workers,
        }
    }

    fn outgoing_nodes(
        &self,
        nodes: &[webgraph::NodeID],
    ) -> FxHashMap<webgraph::NodeID, FxHashSet<webgraph::NodeID>> {
        let mut res: FxHashMap<webgraph::NodeID, FxHashSet<webgraph::NodeID>> =
            FxHashMap::default();

        let limit = webgraph::EdgeLimit::Unlimited;

        for node in nodes {
            for edge in self.worker.graph().raw_outgoing_edges(node, limit) {
                res.entry(*node).or_default().insert(edge.to);
            }
        }

        // this does not retrieve outgoing edges from remote workers asynchronously,
        // but instead waits for each worker to respond before moving on to the next one.
        // this is not ideal, but it will only be a problem when we get way more workers
        // in which case we should probably use an approximation that simply runs the
        // normal harmonic centrality algorithm a fixed number of times.
        for remote_worker in &self.remote_workers {
            let nodes = remote_worker.outgoing_edge_nodes(nodes.to_vec(), limit);

            for (node, outgoing) in nodes {
                res.entry(node).or_default().extend(outgoing);
            }
        }

        res
    }

    fn run_batch(&self, batch: &mut Vec<(u8, webgraph::NodeID)>) -> BTreeMap<webgraph::NodeID, u8> {
        let mut new_distances: BTreeMap<webgraph::NodeID, u8> = BTreeMap::default();

        let nodes: Vec<_> = batch.iter().map(|(_, node)| *node).collect();
        let outgoing_nodes = self.outgoing_nodes(&nodes);

        for (dist, node) in batch.iter().cloned() {
            if let Some(outgoing) = outgoing_nodes.get(&node) {
                for outgoing in outgoing {
                    let d = dist + 1;

                    let current_dist = new_distances.get(outgoing).unwrap_or(&u8::MAX);

                    if d < *current_dist {
                        new_distances.insert(*outgoing, d);
                    }
                }
            }
        }

        batch.clear();

        new_distances
    }

    fn dijkstra(&self, source: webgraph::NodeID, max_dist: u8) -> BTreeMap<webgraph::NodeID, u8> {
        let mut distances: BTreeMap<webgraph::NodeID, u8> = BTreeMap::default();
        let mut queue = std::collections::BinaryHeap::new();
        let mut batch = Vec::with_capacity(BATCH_SIZE);

        queue.push(cmp::Reverse((0u8, source)));
        distances.insert(source, 0u8);

        loop {
            if queue.is_empty() && batch.is_empty() {
                break;
            }

            if let Some(cmp::Reverse((dist, node))) = queue.pop() {
                batch.push((dist, node));
            }

            if batch.len() >= BATCH_SIZE || queue.is_empty() {
                for (node, dist) in self.run_batch(&mut batch) {
                    let cur_dist = distances.get(&node).unwrap_or(&u8::MAX);

                    if dist < *cur_dist {
                        distances.insert(node, dist);

                        if dist < max_dist {
                            queue.push(cmp::Reverse((dist, node)));
                        }
                    }
                }
            }
        }

        distances
    }
}

impl Mapper for ApproxCentralityMapper {
    type Job = ApproxCentralityJob;

    fn map(
        &self,
        job: Self::Job,
        worker: &<<Self as Mapper>::Job as super::Job>::Worker,
        dht: &crate::ampc::DhtConn<<<Self as Mapper>::Job as super::Job>::DhtTables>,
    ) {
        match self {
            ApproxCentralityMapper::ApproximateCentrality { worker_shard } => {
                if worker.shard() != *worker_shard {
                    return;
                }

                let remote_workers: Vec<_> = job
                    .all_workers
                    .iter()
                    .cloned()
                    .filter_map(|(shard, addr)| {
                        if shard == worker.shard() {
                            None
                        } else {
                            Some(RemoteApproxCentralityWorker::new(shard, addr).unwrap())
                        }
                    })
                    .collect();

                let workers = Workers::new(worker.clone(), remote_workers);
                let num_samples = dht.next().meta.get(()).unwrap().num_samples_per_worker;

                let sampled = worker
                    .graph()
                    .random_nodes_with_outgoing(num_samples as usize);

                sampled.into_par_iter().progress().for_each(|node| {
                    for chunk in workers
                        .dijkstra(node, job.max_distance)
                        .into_iter()
                        .filter_map(|(n, d)| {
                            if d == 0 {
                                None
                            } else {
                                Some((n, KahanSum::from((1.0 / d as f64) * job.norm)))
                            }
                        })
                        .chunks(BATCH_SIZE)
                        .into_iter()
                    {
                        let pairs: Vec<_> = chunk.collect();
                        dht.next()
                            .centrality
                            .batch_upsert(upsert::KahanSumAdd, pairs);
                    }
                });
            }
        }
    }
}
