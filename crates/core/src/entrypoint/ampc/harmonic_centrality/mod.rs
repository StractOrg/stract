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

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct Meta {
    round_had_changes: bool,
    upper_bound_num_nodes: u64,
}

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
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

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
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

    use crate::{
        executor::Executor,
        free_socket_addr,
        webgraph::{centrality::harmonic::HarmonicCentrality, Compression, WebgraphWriter},
    };

    use super::*;

    #[test]
    #[traced_test]
    fn test_simple_graph() {
        let mut combined = WebgraphWriter::new(
            crate::gen_temp_path(),
            Executor::single_thread(),
            Compression::default(),
            None,
        );
        let mut a = WebgraphWriter::new(
            crate::gen_temp_path(),
            Executor::single_thread(),
            Compression::default(),
            None,
        );
        let mut b = WebgraphWriter::new(
            crate::gen_temp_path(),
            Executor::single_thread(),
            Compression::default(),
            None,
        );

        let edges = crate::webgraph::tests::test_edges();

        for (i, (from, to, label)) in edges.into_iter().enumerate() {
            combined.insert(from.clone(), to.clone(), label.clone());

            if i % 2 == 0 {
                a.insert(from, to, label);
            } else {
                b.insert(from, to, label);
            }
        }

        combined.commit();
        a.commit();
        b.commit();

        let combined = combined.finalize();
        let a = a.finalize();
        let b = b.finalize();

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

        // assert_eq!(a.num_nodes() + b.num_nodes(), num_nodes as u64);

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
