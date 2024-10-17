// Stract is an open source web search engine.
// Copyright (C) 2023 Stract ApS
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
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

//! This is a centrality measure that is based on the harmonic centrality.
//! The idea is to use the harmonic centrality from the domain graph to
//! derive a centrality measure for the page graph.

use anyhow::Result;
use bloom::U64BloomFilter;
use std::{collections::BTreeMap, path::Path, sync::Mutex};

use crate::webgraph::{EdgeLimit, NodeID, Webgraph};

struct BloomMap {
    map: Vec<Mutex<U64BloomFilter>>,
}

impl BloomMap {
    fn new(num_blooms: usize, estimated_items: u64, fp: f64) -> Self {
        let mut map = Vec::new();

        for _ in 0..num_blooms {
            map.push(Mutex::new(U64BloomFilter::new(estimated_items, fp)));
        }

        Self { map }
    }

    fn insert(&self, item: &NodeID) {
        let h = item.as_u64();
        self.map[(h as usize) % self.map.len()]
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(h);
    }

    fn finalize(mut self) -> U64BloomFilter {
        let mut bf = self.map.pop().unwrap().into_inner().unwrap();

        for m in self.map {
            bf.union(m.into_inner().unwrap());
        }

        bf
    }
}

pub struct DerivedCentrality {
    inner: speedy_kv::Db<NodeID, f64>,
}

impl DerivedCentrality {
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        let inner = speedy_kv::Db::open_or_create(path).unwrap();
        Self { inner }
    }

    pub fn build<P: AsRef<Path>>(
        host_harmonic: &speedy_kv::Db<NodeID, f64>,
        page_graph: &Webgraph,
        output: P,
    ) -> Result<Self> {
        if output.as_ref().exists() {
            return Err(anyhow::anyhow!("output path already exists"));
        }

        let num_nodes = page_graph.nodes().count();

        let has_outgoing = BloomMap::new(8, num_nodes as u64, 0.01);

        page_graph.edges().for_each(|edge| {
            has_outgoing.insert(&edge.from);
        });

        let has_outgoing = has_outgoing.finalize();

        let mut non_normalized =
            speedy_kv::Db::open_or_create(output.as_ref().join("non_normalized")).unwrap();

        let norms: Mutex<BTreeMap<NodeID, f64>> = Mutex::new(BTreeMap::new());
        let pb = indicatif::ProgressBar::new(num_nodes as u64);

        page_graph.node_ids().for_each(|(node, id)| {
            pb.inc(1);
            if has_outgoing.contains(id.as_u64()) {
                let host_node = node.clone().into_host().id();

                if let Some(harmonic) = host_harmonic.get(&host_node).unwrap() {
                    let mut ingoing: Vec<_> = page_graph
                        .raw_ingoing_edges(&id, EdgeLimit::Limit(128))
                        .into_iter()
                        .filter_map(|e| page_graph.id2node(&e.from).unwrap())
                        .map(|n| n.into_host())
                        .collect();
                    ingoing.sort();
                    ingoing.dedup();

                    let votes = ingoing
                        .into_iter()
                        .filter_map(|n| host_harmonic.get(&n.id()).unwrap())
                        .sum::<f64>();
                    let page_score = harmonic * votes;

                    non_normalized.insert(id, page_score).unwrap();

                    if non_normalized.uncommitted_inserts() > 1_000_000 {
                        non_normalized.commit().unwrap();
                    }

                    let mut l = norms.lock().unwrap_or_else(|e| e.into_inner());
                    let norm = l.entry(host_node).or_insert(0.0);
                    *norm = (*norm).max(votes);
                }
            }
        });

        non_normalized.commit().unwrap();
        non_normalized.merge_all_segments().unwrap();

        pb.finish_and_clear();

        let norms = norms.into_inner().unwrap();

        let mut db = speedy_kv::Db::open_or_create(output.as_ref()).unwrap();
        for (id, score) in non_normalized.iter() {
            let node = page_graph.id2node(&id).unwrap().unwrap().into_host().id();
            let norm = norms.get(&node).unwrap();
            let normalized = score / *norm;
            db.insert(id, normalized).unwrap();

            if db.uncommitted_inserts() > 1_000_000 {
                db.commit().unwrap();
            }
        }
        db.commit().unwrap();
        db.merge_all_segments().unwrap();

        drop(non_normalized);
        std::fs::remove_dir_all(output.as_ref().join("non_normalized"))?;

        Ok(Self { inner: db })
    }

    pub fn get(&self, node: &NodeID) -> Option<f64> {
        self.inner.get(node).unwrap()
    }

    pub fn iter(&self) -> impl Iterator<Item = (NodeID, f64)> + '_ {
        self.inner.iter()
    }
}
