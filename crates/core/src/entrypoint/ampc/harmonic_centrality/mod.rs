// Stract is an open source web search engine.
// Copyright (C) 2024 Stract ApS
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
// along with this program.  If not, see <https://www.gnu.org/licenses/>

use crate::hyperloglog::HyperLogLog;
use crate::{ampc::prelude::*, kahan_sum::KahanSum};

use crate::distributed::member::ShardId;
use crate::{ampc::DefaultDhtTable, webgraph};

pub mod coordinator;
mod mapper;
pub mod worker;

use bloom::U64BloomFilter;
pub use coordinator::{CentralityFinish, CentralitySetup};
pub use mapper::CentralityMapper;
pub use worker::{CentralityWorker, RemoteCentralityWorker};

#[derive(
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    Debug,
    Clone,
    PartialEq,
    Eq,
)]
pub struct Meta {
    round_had_changes: bool,
    upper_bound_num_nodes: u64,
}

#[derive(bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct CentralityTables {
    counters: DefaultDhtTable<webgraph::NodeID, HyperLogLog<64>>,
    meta: DefaultDhtTable<(), Meta>,
    centrality: DefaultDhtTable<webgraph::NodeID, KahanSum>,
    changed_nodes: DefaultDhtTable<ShardId, U64BloomFilter>,
}

impl CentralityTables {
    pub fn num_shards(&self) -> u64 {
        self.counters.shards().len() as u64
    }
}

impl_dht_tables!(
    CentralityTables,
    [counters, meta, centrality, changed_nodes]
);

#[derive(bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct CentralityJob {
    shard: ShardId,
}

impl Job for CentralityJob {
    type DhtTables = CentralityTables;
    type Worker = CentralityWorker;
    type Mapper = CentralityMapper;

    fn is_schedulable(&self, worker: &RemoteCentralityWorker) -> bool {
        self.shard == worker.shard()
    }
}

#[cfg(test)]
mod tests {
    use tracing_test::traced_test;
    use webgraph::{Edge, Webgraph};

    use crate::{
        executor::Executor, free_socket_addr, webgraph::centrality::harmonic::HarmonicCentrality,
        webpage::html::links::RelFlags,
    };

    use super::*;

    #[test]
    #[traced_test]
    fn test_simple_graph() {
        let temp_dir = crate::gen_temp_dir().unwrap();
        let mut combined = Webgraph::builder(temp_dir.as_ref().join("combined"))
            .open()
            .unwrap();
        let mut a = Webgraph::builder(temp_dir.as_ref().join("a"))
            .open()
            .unwrap();
        let mut b = Webgraph::builder(temp_dir.as_ref().join("b"))
            .open()
            .unwrap();

        let edges = crate::webgraph::tests::test_edges();

        for (i, (from, to, label)) in edges.into_iter().enumerate() {
            let e = Edge {
                from: from.clone(),
                to: to.clone(),
                label: label.clone(),
                rel_flags: RelFlags::default(),
                combined_centrality: 0.0,
            };
            combined.insert(e.clone());

            if i % 2 == 0 {
                a.insert(e);
            } else {
                b.insert(e);
            }
        }

        combined.commit().unwrap();
        a.commit().unwrap();
        b.commit().unwrap();

        let expected = HarmonicCentrality::calculate(&combined);
        let num_nodes = combined.nodes().count();
        let worker = CentralityWorker::new(1.into(), a);

        let worker_addr = free_socket_addr();

        std::thread::spawn(move || {
            worker.run(worker_addr).unwrap();
        });

        std::thread::sleep(std::time::Duration::from_secs(2)); // Wait for worker to start
        let a = RemoteCentralityWorker::new(1.into(), worker_addr).unwrap();

        let worker = CentralityWorker::new(2.into(), b);
        let worker_addr = free_socket_addr();
        std::thread::spawn(move || {
            worker.run(worker_addr).unwrap();
        });

        std::thread::sleep(std::time::Duration::from_secs(2)); // Wait for worker to start

        let b = RemoteCentralityWorker::new(2.into(), worker_addr).unwrap();

        let (dht_shard, dht_addr) = crate::entrypoint::ampc::dht::tests::setup();
        let res = coordinator::build(&[(dht_shard, dht_addr)], vec![a, b])
            .run(
                vec![
                    CentralityJob { shard: 1.into() },
                    CentralityJob { shard: 2.into() },
                ],
                CentralityFinish,
            )
            .unwrap();

        let mut actual = res
            .centrality
            .iter()
            .map(|(n, s)| (n, f64::from(s) / ((num_nodes - 1) as f64)))
            .collect::<Vec<_>>();
        let mut expected = expected.iter().map(|(n, c)| (*n, c)).collect::<Vec<_>>();

        actual.sort_by(|a, b| a.0.cmp(&b.0));
        expected.sort_by(|a, b| a.0.cmp(&b.0));

        for (expected, actual) in expected
            .iter()
            .map(|(_, c)| c)
            .zip(actual.iter().map(|(_, c)| c))
        {
            assert!((expected - actual).abs() < 0.0001);
        }
    }
}
