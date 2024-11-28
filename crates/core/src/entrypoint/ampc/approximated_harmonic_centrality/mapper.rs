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

use indicatif::ProgressIterator;
use itertools::Itertools;
use std::{cmp, collections::BTreeMap};

use super::{worker::ApproxCentralityWorker, ApproxCentralityJob, Mapper};
use crate::{
    ampc::dht::upsert,
    entrypoint::ampc::approximated_harmonic_centrality::DhtTable,
    kahan_sum::KahanSum,
    webgraph::{self, EdgeLimit},
    webpage::html::links::RelFlags,
};
use rayon::prelude::*;

const BATCH_SIZE: usize = 1024;
const MAX_OUTGOING_EDGES: usize = 128;

pub static SKIPPED_REL: std::sync::LazyLock<RelFlags> = std::sync::LazyLock::new(|| {
    RelFlags::TAG
        | RelFlags::NOFOLLOW
        | RelFlags::IS_IN_FOOTER
        | RelFlags::IS_IN_NAVIGATION
        | RelFlags::PRIVACY_POLICY
        | RelFlags::TERMS_OF_SERVICE
        | RelFlags::SEARCH
        | RelFlags::LINK_TAG
        | RelFlags::SCRIPT_TAG
        | RelFlags::UGC
});

#[derive(Debug, Clone, bincode::Decode, bincode::Encode)]
pub enum ApproxCentralityMapper {
    InitCentrality,
    ApproximateCentrality,
}

struct Workers {
    worker: ApproxCentralityWorker,
}

impl Workers {
    fn new(worker: ApproxCentralityWorker) -> Self {
        Self { worker }
    }

    fn dijkstra(&self, source: webgraph::NodeID, max_dist: u8) -> BTreeMap<webgraph::NodeID, u8> {
        let mut distances: BTreeMap<webgraph::NodeID, u8> = BTreeMap::default();
        let mut queue = std::collections::BinaryHeap::new();

        queue.push(cmp::Reverse((0u8, source)));
        distances.insert(source, 0u8);

        while let Some(cmp::Reverse((dist, node))) = queue.pop() {
            if dist >= max_dist || dist > *distances.get(&node).unwrap_or(&u8::MAX) {
                continue;
            }

            for outgoing in self
                .worker
                .graph()
                .search(
                    &webgraph::query::ForwardlinksQuery::new(node)
                        .with_limit(EdgeLimit::Limit(MAX_OUTGOING_EDGES)),
                )
                .unwrap_or_default()
                .into_iter()
                .filter(|e| !e.rel_flags.intersects(*SKIPPED_REL))
                .map(|e| e.to)
            {
                let d = dist + 1;

                let current_dist = distances.entry(outgoing).or_insert(u8::MAX);

                if d < *current_dist {
                    *current_dist = d;

                    if d < max_dist {
                        queue.push(cmp::Reverse((d, outgoing)));
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
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(usize::from(std::thread::available_parallelism().unwrap()))
            .stack_size(80_000_000)
            .thread_name(move |num| format!("approx-harmonic-mapper-{num}"))
            .build()
            .unwrap();

        pool.install(|| match self {
            ApproxCentralityMapper::InitCentrality => {
                let num_nodes = worker.num_nodes();
                worker
                    .graph()
                    .page_nodes()
                    .chunks(BATCH_SIZE)
                    .into_iter()
                    .progress_count(num_nodes / BATCH_SIZE as u64)
                    .for_each(|chunk| {
                        let pairs: Vec<_> = chunk
                            .into_iter()
                            .map(|node| (node, KahanSum::from(0.0)))
                            .collect();

                        dht.next().centrality.batch_set(pairs)
                    });
            }
            ApproxCentralityMapper::ApproximateCentrality => {
                let workers = Workers::new(worker.clone());
                let num_samples = dht.next().meta.get(()).unwrap().num_samples_per_worker;

                tracing::info!("Sampling {} nodes", num_samples);

                let sampled = worker
                    .graph()
                    .random_page_nodes_with_outgoing(num_samples as usize);

                let pb = indicatif::ProgressBar::new(sampled.len() as u64);

                sampled.into_par_iter().for_each(|node| {
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

                    pb.inc(1);
                });
            }
        })
    }
}
