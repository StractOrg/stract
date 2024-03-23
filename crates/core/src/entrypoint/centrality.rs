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
use std::{cmp::Reverse, fs::File, path::Path};

use crate::{
    external_sort::ExternalSorter,
    kv::{rocksdb_store::RocksDbStore, Kv},
    ranking::inbound_similarity::InboundSimilarity,
    webgraph::{
        centrality::{approx_harmonic::ApproxHarmonic, harmonic::HarmonicCentrality},
        Node, WebgraphBuilder,
    },
    SortableFloat,
};

fn store_csv<P: AsRef<Path>>(data: Vec<(Node, f64)>, output: P) {
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
        wtr.write_record(&[node.as_str().to_string(), centrality.to_string()])
            .unwrap();
    }
    wtr.flush().unwrap();
}

pub struct Centrality;

impl Centrality {
    pub fn build_harmonic<P: AsRef<Path>>(webgraph_path: P, base_output: P) {
        tracing::info!(
            "Building harmonic centrality for {}",
            webgraph_path.as_ref().to_str().unwrap()
        );
        let graph = WebgraphBuilder::new(webgraph_path).single_threaded().open();
        let harmonic_centrality = HarmonicCentrality::calculate(&graph);
        let store = RocksDbStore::open(base_output.as_ref().join("harmonic"));

        for (node_id, centrality) in harmonic_centrality.iter() {
            store.insert(*node_id, centrality);
        }
        store.flush();

        let rank_store: RocksDbStore<crate::webgraph::NodeID, u64> =
            RocksDbStore::open(base_output.as_ref().join("harmonic_rank"));
        let mut top_harmonics = Vec::new();
        for (rank, node, centrality) in ExternalSorter::new()
            .with_chunk_size(100_000_000)
            .sort(
                harmonic_centrality
                    .iter()
                    .map(|(node_id, centrality)| (Reverse(SortableFloat(centrality)), *node_id)),
            )
            .unwrap()
            .enumerate()
            .map(|(rank, (Reverse(SortableFloat(centrality)), node_id))| {
                (rank, node_id, centrality)
            })
        {
            rank_store.insert(node, rank as u64);

            if top_harmonics.len() < 1_000_000 {
                top_harmonics.push((graph.id2node(&node).unwrap(), centrality));
            }
        }

        store_csv(top_harmonics, base_output.as_ref().join("harmonic.csv"));
    }

    pub fn build_similarity<P: AsRef<Path>>(webgraph_path: P, base_output: P) {
        tracing::info!(
            "Building inbound similarity for {}",
            webgraph_path.as_ref().to_str().unwrap()
        );
        let graph = WebgraphBuilder::new(webgraph_path).single_threaded().open();

        let sim = InboundSimilarity::build(&graph);

        sim.save(base_output.as_ref().join("inbound_similarity"))
            .unwrap();
    }

    pub fn build_approx_harmonic<P: AsRef<Path>>(webgraph_path: P, base_output: P) -> Result<()> {
        tracing::info!(
            "Building approximated harmonic centrality for {}",
            webgraph_path.as_ref().to_str().unwrap()
        );

        let graph = WebgraphBuilder::new(webgraph_path).single_threaded().open();

        let approx = ApproxHarmonic::build(&graph, base_output.as_ref().join("approx_harmonic"));
        let approx_rank: RocksDbStore<crate::webgraph::NodeID, u64> =
            RocksDbStore::open(base_output.as_ref().join("approx_harmonic_rank"));

        let mut top_nodes = Vec::new();

        for (rank, node, centrality) in ExternalSorter::new()
            .with_chunk_size(100_000_000)
            .sort(
                approx
                    .iter()
                    .map(|(node_id, centrality)| (Reverse(SortableFloat(centrality)), node_id)),
            )?
            .enumerate()
            .map(|(rank, (Reverse(SortableFloat(centrality)), node_id))| {
                (rank, node_id, centrality)
            })
        {
            approx_rank.insert(node, rank as u64);
            if top_nodes.len() < 1_000_000 {
                top_nodes.push((graph.id2node(&node).unwrap(), centrality));
            }
        }

        store_csv(top_nodes, base_output.as_ref().join("approx_harmonic.csv"));

        Ok(())
    }
}
