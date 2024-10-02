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
use futures::StreamExt;
use itertools::Itertools;
use std::{cmp::Reverse, path::Path, sync::Arc};

use crate::{
    config,
    distributed::cluster::Cluster,
    external_sort::ExternalSorter,
    webgraph::{
        centrality::{
            approx_harmonic::ApproxHarmonic, harmonic::HarmonicCentrality, store_csv,
            store_harmonic, TopNodes,
        },
        remote::{Page, RemoteWebgraph},
        EdgeLimit, NodeID, WebgraphBuilder,
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
        tracing::info!("done");
    }

    pub fn build_approx_harmonic<P: AsRef<Path>>(webgraph_path: P, base_output: P) -> Result<()> {
        tracing::info!(
            "Building approximated harmonic centrality for {}",
            webgraph_path.as_ref().to_str().unwrap()
        );

        let graph = WebgraphBuilder::new(webgraph_path).single_threaded().open();

        let approx = ApproxHarmonic::build(&graph, base_output.as_ref().join("harmonic"));
        let mut approx_rank: speedy_kv::Db<crate::webgraph::NodeID, u64> =
            speedy_kv::Db::open_or_create(base_output.as_ref().join("harmonic_rank")).unwrap();

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

        store_csv(top_nodes, base_output.as_ref().join("harmonic.csv"));

        tracing::info!("done");

        Ok(())
    }

    pub async fn harmonic_nearest_seed(config: config::HarmonicNearestSeedConfig) -> Result<()> {
        let cluster = Arc::new(
            Cluster::join_as_spectator(
                config.gossip.addr,
                config.gossip.seed_nodes.unwrap_or_default(),
            )
            .await?,
        );
        let graph: RemoteWebgraph<Page> = RemoteWebgraph::new(cluster).await;

        let mut harmonic: speedy_kv::Db<NodeID, f64> =
            speedy_kv::Db::open_or_create(config.output_path.join("harmonic"))?;
        let original_centrality: speedy_kv::Db<NodeID, f64> =
            speedy_kv::Db::open_or_create(&config.original_centrality_path)?;

        let mut node_ids = graph.stream_node_ids().await;

        while let Some(node_id) = node_ids.next().await {
            match original_centrality.get(&node_id)? {
                Some(original_centrality) => {
                    harmonic.insert(node_id, original_centrality)?;
                }
                None => {
                    if let Some(seed) = graph
                        .raw_ingoing_edges(node_id, EdgeLimit::Limit(1))
                        .await?
                        .pop()
                        .map(|edge| edge.from.node())
                    {
                        if let Some(seed_centrality) = original_centrality.get(&seed)? {
                            harmonic.insert(node_id, seed_centrality * config.discount_factor)?;
                        }
                    }
                }
            }

            if harmonic.uncommitted_inserts() > 100_000_000 {
                harmonic.commit()?;
            }
        }

        harmonic.commit()?;
        harmonic.merge_all_segments()?;

        let mut top_nodes = Vec::with_capacity(1_000_000);

        for chunk in ExternalSorter::new()
            .with_chunk_size(100_000_000)
            .sort(
                harmonic
                    .iter()
                    .map(|(node_id, centrality)| (Reverse(SortableFloat(centrality)), node_id)),
            )?
            .take(1_000_000)
            .map(|(Reverse(SortableFloat(centrality)), node_id)| (node_id, centrality))
            .chunks(10_000)
            .into_iter()
        {
            let (ids, centrality): (Vec<_>, Vec<_>) = chunk.into_iter().unzip();

            let nodes = graph.batch_get_node(&ids).await?;

            for (node, centrality) in nodes.into_iter().zip(centrality) {
                if let Some(node) = node {
                    top_nodes.push((node, centrality));
                }
            }
        }

        store_csv(top_nodes, config.output_path.join("harmonic.csv"));

        Ok(())
    }
}
