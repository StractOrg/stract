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

use std::{cmp::Reverse, fs::File, path::Path};

use crate::{external_sort::ExternalSorter, SortableFloat};

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

pub fn top_nodes(
    host_centrality: &speedy_kv::Db<NodeID, f64>,
    top: TopNodes,
) -> Vec<(NodeID, f64)> {
    let num_hosts = match top {
        TopNodes::Top(abs) => abs,
        TopNodes::Fraction(frac) => (host_centrality.len() as f64 * frac) as usize,
    };

    crate::sorted_k(
        host_centrality
            .iter()
            .map(|(id, centrality)| (SortableFloat(centrality), id))
            .map(Reverse),
        num_hosts,
    )
    .into_iter()
    .map(|Reverse((SortableFloat(c), id))| (id, c))
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

pub fn store_harmonic<I, P>(centralities: I, output: P) -> speedy_kv::Db<NodeID, f64>
where
    I: Iterator<Item = (NodeID, f64)>,
    P: AsRef<Path>,
{
    let mut store = speedy_kv::Db::open_or_create(output.as_ref().join("harmonic")).unwrap();

    for (node_id, centrality) in centralities {
        store.insert(node_id, centrality).unwrap();

        if store.uncommitted_inserts() >= 100_000_000 {
            store.commit().unwrap();
        }
    }
    store.commit().unwrap();
    store.merge_all_segments().unwrap();

    let mut rank_store: speedy_kv::Db<crate::webgraph::NodeID, u64> =
        speedy_kv::Db::open_or_create(output.as_ref().join("harmonic_rank")).unwrap();

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
        rank_store.insert(node_id, rank as u64).unwrap();

        if rank_store.uncommitted_inserts() >= 100_000_000 {
            rank_store.commit().unwrap();
        }
    }

    rank_store.commit().unwrap();
    rank_store.merge_all_segments().unwrap();

    store
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_top_k() {
        let hits = [
            (SortableFloat(0.0), NodeID::from(0_u64)),
            (SortableFloat(1.0), NodeID::from(1_u64)),
            (SortableFloat(2.0), NodeID::from(2_u64)),
            (SortableFloat(3.0), NodeID::from(3_u64)),
            (SortableFloat(4.0), NodeID::from(4_u64)),
            (SortableFloat(5.0), NodeID::from(5_u64)),
            (SortableFloat(6.0), NodeID::from(6_u64)),
            (SortableFloat(7.0), NodeID::from(7_u64)),
            (SortableFloat(8.0), NodeID::from(8_u64)),
            (SortableFloat(9.0), NodeID::from(9_u64)),
        ];

        let top_5 = crate::sorted_k(hits.iter().copied(), 5);
        assert_eq!(
            top_5,
            vec![
                (SortableFloat(0.0), NodeID::from(0_u64)),
                (SortableFloat(1.0), NodeID::from(1_u64)),
                (SortableFloat(2.0), NodeID::from(2_u64)),
                (SortableFloat(3.0), NodeID::from(3_u64)),
                (SortableFloat(4.0), NodeID::from(4_u64))
            ]
        );

        let top_3 = crate::sorted_k(hits.iter().copied(), 3);
        assert_eq!(
            top_3,
            vec![
                (SortableFloat(0.0), NodeID::from(0_u64)),
                (SortableFloat(1.0), NodeID::from(1_u64)),
                (SortableFloat(2.0), NodeID::from(2_u64))
            ]
        );

        let top_0 = crate::sorted_k(hits.iter().copied(), 0);
        assert_eq!(top_0, Vec::<(SortableFloat, NodeID)>::new());
    }

    #[test]
    fn test_top_k_reversed() {
        let hits = [
            (SortableFloat(9.0), NodeID::from(9_u64)),
            (SortableFloat(8.0), NodeID::from(8_u64)),
            (SortableFloat(7.0), NodeID::from(7_u64)),
            (SortableFloat(6.0), NodeID::from(6_u64)),
            (SortableFloat(5.0), NodeID::from(5_u64)),
            (SortableFloat(4.0), NodeID::from(4_u64)),
            (SortableFloat(3.0), NodeID::from(3_u64)),
            (SortableFloat(2.0), NodeID::from(2_u64)),
            (SortableFloat(1.0), NodeID::from(1_u64)),
            (SortableFloat(0.0), NodeID::from(0_u64)),
        ];

        let top_5 = crate::sorted_k(hits.iter().copied().map(Reverse), 5)
            .into_iter()
            .map(|Reverse(x)| x)
            .collect::<Vec<_>>();

        assert_eq!(
            top_5,
            vec![
                (SortableFloat(9.0), NodeID::from(9_u64)),
                (SortableFloat(8.0), NodeID::from(8_u64)),
                (SortableFloat(7.0), NodeID::from(7_u64)),
                (SortableFloat(6.0), NodeID::from(6_u64)),
                (SortableFloat(5.0), NodeID::from(5_u64))
            ]
        );

        let top_3 = crate::sorted_k(hits.iter().copied().map(Reverse), 3)
            .into_iter()
            .map(|Reverse(x)| x)
            .collect::<Vec<_>>();

        assert_eq!(
            top_3,
            vec![
                (SortableFloat(9.0), NodeID::from(9_u64)),
                (SortableFloat(8.0), NodeID::from(8_u64)),
                (SortableFloat(7.0), NodeID::from(7_u64))
            ]
        );
    }
}
