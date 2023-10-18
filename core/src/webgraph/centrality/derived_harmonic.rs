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
use std::{collections::BTreeMap, path::Path};

use crate::{
    kv::{rocksdb_store::RocksDbStore, Kv},
    webgraph::{NodeID, Webgraph},
};
pub struct DerivedCentrality {
    inner: RocksDbStore<NodeID, f64>,
}

impl DerivedCentrality {
    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        let inner = RocksDbStore::open(path);
        Self { inner }
    }

    pub fn build<P: AsRef<Path>>(
        host_harmonic: &RocksDbStore<NodeID, f64>,
        page_graph: &Webgraph,
        output: P,
    ) -> Result<Self> {
        if output.as_ref().exists() {
            return Err(anyhow::anyhow!("output path already exists"));
        }

        let non_normalized = RocksDbStore::open(output.as_ref().join("non_normalized"));

        let mut norms: BTreeMap<NodeID, usize> = BTreeMap::new();

        for (node, id) in page_graph.node_ids() {
            let host_node = node.clone().into_host().id();

            if let Some(harmonic) = host_harmonic.get(&host_node) {
                let mut ingoing: Vec<_> = page_graph
                    .raw_ingoing_edges(&id)
                    .into_iter()
                    .filter_map(|e| page_graph.id2node(&e.from))
                    .map(|n| n.into_host())
                    .collect();
                ingoing.sort();
                ingoing.dedup();

                let count = ingoing.len();
                let page_score = harmonic * count as f64;

                non_normalized.insert(id, page_score);

                let norm = norms.entry(host_node).or_insert(0);
                *norm = (*norm).max(count);
            }
        }

        let db = RocksDbStore::open(output.as_ref());
        for (id, score) in non_normalized.iter() {
            let node = page_graph.id2node(&id).unwrap().into_host().id();
            let norm = norms.get(&node).unwrap();
            let normalized = score / (*norm as f64);
            db.insert(id, normalized);
        }
        db.flush();

        drop(non_normalized);
        std::fs::remove_dir_all(output.as_ref().join("non_normalized"))?;

        Ok(Self { inner: db })
    }

    pub fn get(&self, node: &NodeID) -> Option<f64> {
        self.inner.get(node)
    }

    pub fn iter(&self) -> impl Iterator<Item = (NodeID, f64)> + '_ {
        self.inner.iter()
    }
}
