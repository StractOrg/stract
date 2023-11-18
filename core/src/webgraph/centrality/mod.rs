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

use crate::kv::{rocksdb_store::RocksDbStore, Kv};

use super::NodeID;

pub mod betweenness;
pub mod derived_harmonic;
pub mod harmonic;

#[derive(Debug, Clone, Copy)]
pub enum TopHosts {
    Top(usize),
    Fraction(f64),
}

pub fn top_hosts(host_centrality: &RocksDbStore<NodeID, f64>, top: TopHosts) -> Vec<NodeID> {
    let mut hosts = host_centrality
        .iter()
        .map(|(id, centrality)| {
            if !centrality.is_finite() {
                (id, 0.0)
            } else {
                (id, centrality)
            }
        })
        .collect::<Vec<_>>();

    hosts.sort_by(|(_, a), (_, b)| b.total_cmp(a));

    let num_hosts = match top {
        TopHosts::Top(abs) => abs,
        TopHosts::Fraction(frac) => (hosts.len() as f64 * frac) as usize,
    };

    hosts
        .into_iter()
        .take(num_hosts)
        .map(|(id, _)| id)
        .collect()
}
