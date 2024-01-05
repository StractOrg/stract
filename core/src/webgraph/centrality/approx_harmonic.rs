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

use std::{path::Path, sync::Mutex};

use indicatif::ParallelProgressIterator;
use rayon::prelude::*;

use crate::{
    kv::{rocksdb_store::RocksDbStore, Kv},
    webgraph::{NodeID, ShortestPaths, Webgraph},
};

const EPSILON: f64 = 0.05;

// Approximate harmonic centrality by sampling O(log n / epsilon^2) nodes and
// computing single-source shortest paths from each of them.
//
// Epsilong is set to 0.05.
pub struct ApproxHarmonic {
    inner: RocksDbStore<NodeID, f64>,
}

impl ApproxHarmonic {
    pub fn build<P: AsRef<Path>>(graph: &Webgraph, output: P) -> Self {
        let num_nodes = graph.nodes().count();

        let num_samples = ((num_nodes as f64).log2() / EPSILON.powi(2)).ceil() as usize;
        let sampled = graph.random_nodes(num_samples);

        let res = Mutex::new(Self {
            inner: RocksDbStore::open(output),
        });

        let norm = num_nodes as f64 / (num_samples as f64 * (num_nodes as f64 - 1.0));

        sampled.into_par_iter().progress().for_each(|source| {
            let dists = graph.raw_distances_with_max(source, 5);

            let res = res.lock().unwrap();
            for (target, dist) in dists {
                if dist == 0 {
                    continue;
                }

                let dist = dist as f64;

                let old = res.inner.get(&target).unwrap_or(0.0);
                res.inner.insert(target, old + ((1.0 / dist) * norm));
            }
        });

        res.into_inner().unwrap()
    }

    pub fn get(&self, node: &NodeID) -> Option<f64> {
        self.inner.get(node)
    }

    pub fn iter(&self) -> impl Iterator<Item = (NodeID, f64)> + '_ {
        self.inner.iter()
    }
}
