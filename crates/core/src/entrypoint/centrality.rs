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
use std::{cmp::Reverse, path::Path};

use crate::{
    external_sort::ExternalSorter,
    webgraph::{
        centrality::{
            approx_harmonic::ApproxHarmonic, harmonic::HarmonicCentrality, store_csv,
            store_harmonic, TopNodes,
        },
        WebgraphBuilder,
    },
    SortableFloat,
};

pub struct Centrality;

impl Centrality {
    pub fn build_harmonic<P: AsRef<Path>>(webgraph_path: P, base_output: P) {
        tracing::info!(
            "Building harmonic centrality for {}",
            webgraph_path.as_ref().to_str().unwrap()
        );
        let graph = WebgraphBuilder::new(webgraph_path).single_threaded().open();
        let harmonic_centrality = HarmonicCentrality::calculate(&graph);
        let store = store_harmonic(
            harmonic_centrality.iter().map(|(n, c)| (*n, c)),
            base_output.as_ref(),
        );

        let top_harmonics =
            crate::webgraph::centrality::top_nodes(&store, TopNodes::Top(1_000_000))
                .into_iter()
                .map(|(n, c)| (graph.id2node(&n).unwrap(), c))
                .collect();

        store_csv(top_harmonics, base_output.as_ref().join("harmonic.csv"));
    }

    pub fn build_approx_harmonic<P: AsRef<Path>>(webgraph_path: P, base_output: P) -> Result<()> {
        tracing::info!(
            "Building approximated harmonic centrality for {}",
            webgraph_path.as_ref().to_str().unwrap()
        );

        let graph = WebgraphBuilder::new(webgraph_path).single_threaded().open();

        let approx = ApproxHarmonic::build(&graph, base_output.as_ref().join("approx_harmonic"));
        let mut approx_rank: speedy_kv::Db<crate::webgraph::NodeID, u64> =
            speedy_kv::Db::open_or_create(base_output.as_ref().join("approx_harmonic_rank"))
                .unwrap();

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
            approx_rank.insert(node, rank as u64).unwrap();

            if approx_rank.uncommitted_inserts() > 100_000_000 {
                approx_rank.commit().unwrap();
            }

            if top_nodes.len() < 1_000_000 {
                top_nodes.push((graph.id2node(&node).unwrap(), centrality));
            }
        }

        approx_rank.commit().unwrap();
        approx_rank.merge_all_segments().unwrap();

        store_csv(top_nodes, base_output.as_ref().join("approx_harmonic.csv"));

        Ok(())
    }
}
