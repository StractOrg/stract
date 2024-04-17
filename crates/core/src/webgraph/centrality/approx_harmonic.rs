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

use std::path::Path;

use dashmap::DashMap;
use indicatif::ParallelProgressIterator;
use rayon::prelude::*;

use crate::{
    speedy_kv,
    webgraph::{NodeID, ShortestPaths, Webgraph},
};

const EPSILON: f64 = 0.05;

// Approximate harmonic centrality by sampling O(log n / epsilon^2) nodes and
// computing single-source shortest paths from each of them.
//
// Epsilong is set to 0.05.
pub struct ApproxHarmonic {
    inner: speedy_kv::Db<NodeID, f64>,
}

impl ApproxHarmonic {
    pub fn build<P: AsRef<Path>>(graph: &Webgraph, output: P) -> Self {
        let num_nodes = graph.estimate_num_nodes();

        tracing::info!("found approximately {} nodes in graph", num_nodes);

        let num_samples = ((num_nodes as f64).log2() / EPSILON.powi(2)).ceil() as usize;

        tracing::info!("sampling {} nodes", num_samples);

        let sampled = graph.random_nodes_with_outgoing(num_samples);

        let centralities: DashMap<NodeID, f32> = DashMap::new();

        let norm = num_nodes as f32 / (num_samples as f32 * (num_nodes as f32 - 1.0));

        sampled.into_par_iter().progress().for_each(|source| {
            let dists = graph.raw_distances_with_max(source, 5);

            for (target, dist) in dists {
                if dist == 0 {
                    continue;
                }

                let dist = dist as f32;

                *centralities.entry(target).or_default() += (1.0 / dist) * norm;
            }
        });

        let mut res = Self {
            inner: speedy_kv::Db::open_or_create(output).unwrap(),
        };

        for (node, centrality) in centralities {
            res.inner.insert(node, centrality as f64).unwrap();

            if res.inner.uncommitted_inserts() >= 1_000_000 {
                res.inner.commit().unwrap();
            }
        }

        res.inner.commit().unwrap();
        res.inner.merge_all_segments().unwrap();

        res
    }

    pub fn get(&self, node: &NodeID) -> Option<f64> {
        self.inner.get(node).unwrap()
    }

    pub fn iter(&self) -> impl Iterator<Item = (NodeID, f64)> + '_ {
        self.inner.iter()
    }
}
