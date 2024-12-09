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

use std::sync::{atomic::AtomicBool, Arc, Mutex};

use bloom::U64BloomFilter;
use rustc_hash::FxHashMap;

use super::{DhtTable as _, Mapper, Meta, ShortestPathJob, ShortestPathTables};
use crate::{
    ampc::{
        dht::{U64Min, UpsertAction},
        DhtConn,
    },
    webgraph,
    webpage::html::links::RelFlags,
};

const BATCH_SIZE: usize = 4096;

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
pub enum ShortestPathMapper {
    RelaxEdges,
    UpdateChangedNodes,
}

impl ShortestPathMapper {
    fn get_old_distances(
        batch: Vec<webgraph::NodeID>,
        dht: &DhtConn<ShortestPathTables>,
    ) -> FxHashMap<webgraph::NodeID, u64> {
        dht.prev().distances.batch_get(batch).into_iter().collect()
    }

    fn update_distances(
        batch: &[webgraph::SmallEdge],
        dht: &DhtConn<ShortestPathTables>,
    ) -> Vec<(webgraph::NodeID, UpsertAction)> {
        let old_distances = Self::get_old_distances(batch.iter().map(|e| e.from).collect(), dht);

        let mut new_distances = FxHashMap::default();

        for edge in batch {
            if let Some(old_distance) = old_distances.get(&edge.from) {
                let new_distance = old_distance + 1;
                let old_new = new_distances.entry(edge.to).or_insert(new_distance);

                if new_distance < *old_new {
                    *old_new = new_distance;
                }
            }
        }

        dht.next()
            .distances
            .batch_upsert(U64Min, new_distances.into_iter().collect())
    }

    fn map_batch(
        batch: &[webgraph::SmallEdge],
        new_changed_nodes: &Mutex<U64BloomFilter>,
        round_had_changes: &AtomicBool,
        dht: &DhtConn<ShortestPathTables>,
    ) {
        let updates = Self::update_distances(batch, dht);
        let mut new_changed_nodes = new_changed_nodes.lock().unwrap();

        for (node, action) in updates {
            if action.is_changed() {
                new_changed_nodes.insert_u128(node.as_u128());
                round_had_changes.store(true, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
}

impl Mapper for ShortestPathMapper {
    type Job = ShortestPathJob;

    fn map(
        &self,
        job: Self::Job,
        worker: &<<Self as Mapper>::Job as super::Job>::Worker,
        dht: &crate::ampc::DhtConn<<<Self as Mapper>::Job as super::Job>::DhtTables>,
    ) {
        match self {
            ShortestPathMapper::RelaxEdges => {
                let round_had_changes = Arc::new(AtomicBool::new(false));
                let pool = rayon::ThreadPoolBuilder::new().build().unwrap();

                let new_changed_nodes = Arc::new(Mutex::new(U64BloomFilter::empty_from(
                    &worker.changed_nodes().lock().unwrap(),
                )));

                pool.scope(|s| {
                    let mut changed_nodes = worker.changed_nodes().lock().unwrap();
                    changed_nodes.insert_u128(job.source.as_u128());

                    let mut batch = Vec::with_capacity(BATCH_SIZE);

                    for edge in worker.graph().page_edges() {
                        if edge.rel_flags.intersects(*SKIPPED_REL) {
                            continue;
                        }

                        if changed_nodes.contains_u128(edge.from.as_u128()) {
                            batch.push(edge);
                        }

                        if batch.len() >= BATCH_SIZE {
                            let update_batch = batch.clone();
                            let update_new_changed_nodes = new_changed_nodes.clone();
                            let update_round_had_changes = round_had_changes.clone();
                            s.spawn(move |_| {
                                Self::map_batch(
                                    &update_batch,
                                    &update_new_changed_nodes,
                                    &update_round_had_changes,
                                    dht,
                                )
                            });
                            batch.clear();
                        }
                    }

                    if !batch.is_empty() {
                        Self::map_batch(&batch, &new_changed_nodes, &round_had_changes, dht);
                    }
                });

                dht.next()
                    .changed_nodes
                    .set(worker.shard(), new_changed_nodes.lock().unwrap().clone());
                dht.next().meta.set(
                    (),
                    Meta {
                        round_had_changes: round_had_changes
                            .load(std::sync::atomic::Ordering::Relaxed),
                    },
                );
            }
            ShortestPathMapper::UpdateChangedNodes => {
                let all_changed_nodes: Vec<_> =
                    dht.next().changed_nodes.iter().map(|(_, v)| v).collect();
                let mut changed_nodes =
                    U64BloomFilter::empty_from(&worker.changed_nodes().lock().unwrap());

                for bloom in all_changed_nodes {
                    changed_nodes.union(bloom.clone());
                }

                *worker.changed_nodes().lock().unwrap() = changed_nodes;
            }
        }
    }
}
