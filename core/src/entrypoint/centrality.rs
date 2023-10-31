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

use anyhow::Result;
use std::{cmp::Reverse, collections::BinaryHeap, fs::File, path::Path};

use crate::{
    kv::{rocksdb_store::RocksDbStore, Kv},
    ranking::inbound_similarity::InboundSimilarity,
    webgraph::{
        centrality::{derived_harmonic::DerivedCentrality, harmonic::HarmonicCentrality},
        Node, WebgraphBuilder,
    },
};

fn store_csv(data: Vec<(Node, f64)>, output: &Path) {
    let csv_file = File::options()
        .write(true)
        .create(true)
        .truncate(true)
        .open(output)
        .unwrap();

    let mut data = data;
    data.sort_by(|(_, a), (_, b)| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
    let mut wtr = csv::Writer::from_writer(csv_file);
    for (node, centrality) in data {
        wtr.write_record(&[node.name, centrality.to_string()])
            .unwrap();
    }
    wtr.flush().unwrap();
}

struct SortableFloat(f64);

impl PartialEq for SortableFloat {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for SortableFloat {}

impl PartialOrd for SortableFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortableFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

pub struct Centrality;

impl Centrality {
    pub fn build_harmonic(webgraph_path: &Path, base_output: &Path) {
        tracing::info!("Building harmonic centrality");
        let graph = WebgraphBuilder::new(webgraph_path).open();
        let harmonic_centrality = HarmonicCentrality::calculate(&graph);
        let store = RocksDbStore::open(&base_output.join("harmonic"));

        for (node_id, centrality) in harmonic_centrality.iter() {
            store.insert(*node_id, centrality);
        }
        store.flush();

        let mut top_harmonics = BinaryHeap::new();

        for (node_id, centrality) in harmonic_centrality.iter() {
            if top_harmonics.len() < 1_000_000 {
                top_harmonics.push((Reverse(SortableFloat(centrality)), *node_id));
            } else {
                let mut min = top_harmonics.peek_mut().unwrap();
                if min.0 .0 < SortableFloat(centrality) {
                    *min = (Reverse(SortableFloat(centrality)), *node_id);
                }
            }
        }

        let harmonics: Vec<_> = top_harmonics
            .into_iter()
            .map(|(score, id)| (graph.id2node(&id).unwrap(), score.0 .0))
            .collect();

        store_csv(harmonics, &base_output.join("harmonic.csv"));
    }

    pub fn build_similarity(webgraph_path: &Path, base_output: &Path) {
        tracing::info!("Building inbound similarity");
        let graph = WebgraphBuilder::new(webgraph_path).open();

        let sim = InboundSimilarity::build(&graph);

        sim.save(&base_output.join("inbound_similarity")).unwrap();
    }

    pub fn build_derived_harmonic(
        webgraph_path: &Path,
        host_centrality_path: &Path,
        base_output: &Path,
    ) -> Result<()> {
        tracing::info!("Building derived harmonic centrality");
        let graph = WebgraphBuilder::new(webgraph_path).single_threaded().open();
        let host_centrality = RocksDbStore::open(&host_centrality_path.join("harmonic"));

        let derived = DerivedCentrality::build(
            &host_centrality,
            &graph,
            &base_output.join("derived_harmonic"),
        )?;

        let mut top_nodes = BinaryHeap::new();

        for (node_id, centrality) in derived.iter() {
            if top_nodes.len() < 1_000_000 {
                top_nodes.push((Reverse(SortableFloat(centrality)), node_id));
            } else {
                let mut min = top_nodes.peek_mut().unwrap();
                if min.0 .0 < SortableFloat(centrality) {
                    *min = (Reverse(SortableFloat(centrality)), node_id);
                }
            }
        }

        let derived: Vec<_> = top_nodes
            .into_iter()
            .map(|(score, id)| (graph.id2node(&id).unwrap(), score.0 .0))
            .collect();

        store_csv(derived, &base_output.join("derived_centrality.csv"));

        Ok(())
    }
}
