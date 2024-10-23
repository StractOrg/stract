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

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use bloom::U64BloomFilter;

use crate::{
    ampc::{
        dht::{HyperLogLog64Upsert, UpsertAction},
        prelude::*,
        DhtConn,
    },
    hyperloglog::HyperLogLog,
    webgraph::{self, centrality::harmonic::SKIPPED_REL},
};

use super::{CentralityJob, CentralityTables, CentralityWorker};

const OPS_BATCH_PER_SHARD: u64 = 4096;

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
pub enum CentralityMapper {
    SetupCounters,
    SetupBloom,
    Cardinalities,
    Centralities,
    SaveBloom,
    UpdateBloom,
}

impl CentralityMapper {
    /// get old values from prev dht using edge.from where edge.from in changed_nodes
    fn get_old_counters(
        batch: &[webgraph::SmallEdge],
        dht: &DhtConn<CentralityTables>,
    ) -> BTreeMap<webgraph::NodeID, HyperLogLog<64>> {
        let nodes: Vec<_> = batch.iter().map(|edge| edge.from).collect();

        if nodes.is_empty() {
            return BTreeMap::new();
        }

        dht.prev().counters.batch_get(nodes).into_iter().collect()
    }

    fn setup_counters(nodes: &[webgraph::NodeID], dht: &DhtConn<CentralityTables>) {
        dht.prev().counters.batch_set(
            nodes
                .iter()
                .map(|node| {
                    let mut hll = HyperLogLog::default();
                    hll.add(node.as_u64());
                    (*node, hll)
                })
                .collect(),
        );

        dht.next().counters.batch_set(
            nodes
                .iter()
                .map(|node| {
                    let mut hll = HyperLogLog::default();
                    hll.add(node.as_u64());
                    (*node, hll)
                })
                .collect(),
        );
    }

    /// upsert old edge.from `old_counters` into edge.to in dht.next,
    /// thereby updating their hyperloglog counters
    fn update_counters(
        batch: &[webgraph::SmallEdge],
        dht: &DhtConn<CentralityTables>,
    ) -> Vec<(webgraph::NodeID, UpsertAction)> {
        let old_counters = Self::get_old_counters(batch, dht);

        let updates: Vec<_> = batch
            .iter()
            .map(|edge| {
                let mut counter = old_counters.get(&edge.from).cloned().unwrap_or_default();
                counter.add(edge.from.as_u64());
                (edge.to, counter)
            })
            .collect();

        if updates.is_empty() {
            return vec![];
        }

        dht.next()
            .counters
            .batch_upsert(HyperLogLog64Upsert, updates)
    }

    /// update new bloom filter with the nodes that changed
    fn update_changed_nodes(
        changes: &[(webgraph::NodeID, UpsertAction)],
        new_changed_nodes: &Mutex<U64BloomFilter>,
    ) {
        let mut new_changed_nodes = new_changed_nodes.lock().unwrap();

        for (node, upsert_res) in changes {
            if let UpsertAction::Merged = upsert_res {
                new_changed_nodes.insert(node.as_u64());
            }
        }
    }

    fn update_dht(
        worker: &CentralityWorker,
        batch: &[webgraph::SmallEdge],
        new_changed_nodes: &Mutex<U64BloomFilter>,
        dht: &DhtConn<CentralityTables>,
    ) {
        if batch.is_empty() {
            return;
        }

        let changes = Self::update_counters(batch, dht);
        Self::update_changed_nodes(&changes, new_changed_nodes);

        // if any nodes changed, indicate in dht that we aren't finished yet
        if !worker.has_updated_meta_for_round()
            && changes.iter().any(|(_, upsert_res)| match upsert_res {
                UpsertAction::Merged => true,
                UpsertAction::NoChange => false,
                UpsertAction::Inserted => true,
            })
        {
            // it's okay that this is not atomic as it will just cause an extra meta update
            worker.set_has_updated_meta_for_round(true);

            let mut meta = dht.next().meta.get(()).unwrap();
            meta.round_had_changes = true;
            dht.next().meta.set((), meta)
        }
    }

    fn update_centralities(
        nodes: &[webgraph::NodeID],
        round: u64,
        dht: &DhtConn<CentralityTables>,
    ) {
        if nodes.is_empty() {
            return;
        }

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

        let mut new_values = BTreeMap::new();
        let old_values: BTreeMap<_, _> = dht
            .prev()
            .centrality
            .batch_get(nodes.to_vec())
            .into_iter()
            .collect();

        for node in nodes {
            if let (Some(old_size), Some(new_size)) = (
                old_counters.get(node).map(|s| s.size() as u64),
                new_counters.get(node).map(|s| s.size() as u64),
            ) {
                let d = new_size.saturating_sub(old_size);

                if d == 0 {
                    continue;
                }

                new_values.insert(
                    *node,
                    old_values.get(node).copied().unwrap_or(0.0.into())
                        + (d as f64 / (round + 1) as f64),
                );
            }
        }

        dht.next()
            .centrality
            .batch_set(new_values.into_iter().collect());
    }

    fn map_setup_counters(worker: &CentralityWorker, dht: &DhtConn<CentralityTables>) {
        if worker.round() != 0 {
            return;
        }

        // shards are the same for both prev and next
        let num_shards = dht.prev().num_shards();
        let batch_size = (num_shards * OPS_BATCH_PER_SHARD) as usize;

        let pool = rayon::ThreadPoolBuilder::new().build().unwrap();

        pool.scope(|s| {
            let mut batch = Vec::with_capacity(batch_size);
            let mut changed_nodes = worker.changed_nodes().lock().unwrap();

            for node in worker.graph().host_nodes() {
                changed_nodes.insert(node.as_u64());
                batch.push(node);
                if batch.len() >= batch_size {
                    let update_batch = batch.clone();
                    s.spawn(move |_| Self::setup_counters(&update_batch, dht));

                    batch.clear();
                }
            }

            if !batch.is_empty() {
                let update_batch = batch.clone();
                s.spawn(move |_| Self::setup_counters(&update_batch, dht));
            }
        });
    }

    fn map_setup_bloom(worker: &CentralityWorker, dht: &DhtConn<CentralityTables>) {
        if worker.round() != 0 {
            return;
        }

        let upper_bound_num_nodes = dht.prev().meta.get(()).unwrap().upper_bound_num_nodes;
        worker.setup_changed_nodes(upper_bound_num_nodes);
    }

    fn map_cardinalities(worker: &CentralityWorker, dht: &DhtConn<CentralityTables>) {
        worker.set_has_updated_meta_for_round(false);
        // shards are the same for both prev and next
        let num_shards = dht.prev().num_shards();
        let batch_size = (num_shards * OPS_BATCH_PER_SHARD) as usize;

        let new_changed_nodes = Arc::new(Mutex::new(U64BloomFilter::empty_from(
            &worker.changed_nodes().lock().unwrap(),
        )));

        let pool = rayon::ThreadPoolBuilder::new().build().unwrap();

        pool.scope(|s| {
            let mut batch = Vec::with_capacity(batch_size);
            let changed_nodes = worker.changed_nodes().lock().unwrap();

            for edge in worker
                .graph()
                .host_edges()
                .filter(|e| !e.rel_flags.intersects(*SKIPPED_REL))
                .filter(|e| changed_nodes.contains(e.from.as_u64()))
            {
                batch.push(edge);
                if batch.len() >= batch_size {
                    let new_changed_nodes = Arc::clone(&new_changed_nodes);
                    let update_batch = batch.clone();

                    s.spawn(move |_| {
                        Self::update_dht(worker, &update_batch, &new_changed_nodes, dht)
                    });

                    batch.clear();
                }
            }

            if !batch.is_empty() {
                let new_changed_nodes = Arc::clone(&new_changed_nodes);
                let update_batch = batch.clone();

                s.spawn(move |_| Self::update_dht(worker, &update_batch, &new_changed_nodes, dht));
            }
        });
        *worker.changed_nodes().lock().unwrap() = new_changed_nodes.lock().unwrap().clone();
    }

    fn map_centralities(worker: &CentralityWorker, dht: &DhtConn<CentralityTables>) {
        if !dht.prev().meta.get(()).unwrap().round_had_changes {
            return;
        }

        // shards are the same for both prev and next
        let num_shards = dht.prev().num_shards();
        let batch_size = (num_shards * OPS_BATCH_PER_SHARD) as usize;

        let pool = rayon::ThreadPoolBuilder::new().build().unwrap();
        let round = worker.inc_round();

        // count cardinality of hyperloglogs in dht.next and update count after all mappers are done
        pool.scope(|s| {
            let mut batch = Vec::with_capacity(batch_size);
            let changed_nodes = worker.changed_nodes().lock().unwrap();
            for node in worker
                .graph()
                .host_nodes()
                .into_iter()
                .filter(|n| changed_nodes.contains(n.as_u64()))
            {
                batch.push(node);
                if batch.len() >= batch_size {
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

    fn map_save_bloom(worker: &CentralityWorker, dht: &DhtConn<CentralityTables>) {
        let changed_nodes = worker.changed_nodes().lock().unwrap();
        dht.next()
            .changed_nodes
            .set(worker.shard(), changed_nodes.clone());
    }

    fn map_update_bloom(worker: &CentralityWorker, dht: &DhtConn<CentralityTables>) {
        let changed_nodes: Vec<_> = dht.next().changed_nodes.iter().map(|(_, v)| v).collect();

        let mut new_changed_nodes =
            U64BloomFilter::empty_from(&worker.changed_nodes().lock().unwrap());

        for bloom in changed_nodes {
            new_changed_nodes.union(bloom.clone());
        }

        *worker.changed_nodes().lock().unwrap() = new_changed_nodes;
    }
}

impl Mapper for CentralityMapper {
    type Job = CentralityJob;

    fn map(&self, _: Self::Job, worker: &CentralityWorker, dht: &DhtConn<CentralityTables>) {
        match self {
            CentralityMapper::SetupBloom => Self::map_setup_bloom(worker, dht),
            CentralityMapper::SetupCounters => Self::map_setup_counters(worker, dht),
            CentralityMapper::Cardinalities => Self::map_cardinalities(worker, dht),
            CentralityMapper::SaveBloom => Self::map_save_bloom(worker, dht),
            CentralityMapper::UpdateBloom => Self::map_update_bloom(worker, dht),
            CentralityMapper::Centralities => Self::map_centralities(worker, dht),
        }
    }
}
