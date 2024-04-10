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

use std::{cmp::Reverse, collections::BinaryHeap, fs::File, path::Path};

use crate::{
    external_sort::ExternalSorter,
    kv::{rocksdb_store::RocksDbStore, Kv},
    SortableFloat,
};

use super::{Node, NodeID};

pub mod approx_harmonic;
pub mod betweenness;
pub mod derived_harmonic;
pub mod harmonic;

#[derive(Debug, Clone, Copy)]
pub enum TopNodes {
    Top(usize),
    Fraction(f64),
}

pub fn top_nodes(host_centrality: &RocksDbStore<NodeID, f64>, top: TopNodes) -> Vec<(NodeID, f64)> {
    let num_hosts = match top {
        TopNodes::Top(abs) => abs,
        TopNodes::Fraction(frac) => (host_centrality.approx_len() as f64 * frac) as usize,
    };

    let mut top: BinaryHeap<Reverse<(SortableFloat, NodeID)>> = BinaryHeap::new();

    for (id, centrality) in host_centrality.iter() {
        if top.len() >= num_hosts {
            let mut min = top.peek_mut().unwrap();

            if centrality > min.0 .0 .0 {
                min.0 .1 = id;
                min.0 .0 .0 = centrality;
            }
        } else {
            top.push(Reverse((SortableFloat(centrality), id)));
        }
    }

    top.into_sorted_vec()
        .into_iter()
        .map(|Reverse((SortableFloat(c), id))| (id, c))
        .rev()
        .collect()
}

pub fn store_csv<P: AsRef<Path>>(data: Vec<(Node, f64)>, output: P) {
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

pub fn store_harmonic<I, P>(centralities: I, output: P) -> RocksDbStore<NodeID, f64>
where
    I: Iterator<Item = (NodeID, f64)>,
    P: AsRef<Path>,
{
    let store = RocksDbStore::open(output.as_ref().join("harmonic"));

    for (node_id, centrality) in centralities {
        store.insert(node_id, centrality);
    }
    store.flush();

    let rank_store: RocksDbStore<crate::webgraph::NodeID, u64> =
        RocksDbStore::open(output.as_ref().join("harmonic_rank"));
    for (rank, node_id) in ExternalSorter::new()
        .with_chunk_size(100_000_000)
        .sort(
            store
                .iter()
                .map(|(node_id, centrality)| (Reverse(SortableFloat(centrality)), node_id)),
        )
        .unwrap()
        .enumerate()
        .map(|(rank, (_, node_id))| (rank, node_id))
    {
        rank_store.insert(node_id, rank as u64);
    }

    store
}
