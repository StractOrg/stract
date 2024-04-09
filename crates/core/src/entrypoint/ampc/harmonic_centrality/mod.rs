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

use crate::{ampc::DefaultDhtTable, webgraph};

pub mod coordinator;
mod mapper;
pub mod worker;

pub use coordinator::{CentralityFinish, CentralitySetup};
pub use mapper::CentralityMapper;
pub use worker::{CentralityWorker, RemoteCentralityWorker};

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct Meta {
    round_had_changes: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct CentralityTables {
    counters: DefaultDhtTable<webgraph::NodeID, HyperLogLog<64>>,
    meta: DefaultDhtTable<(), Meta>,
    centrality: DefaultDhtTable<webgraph::NodeID, KahanSum>,
}

impl_dht_tables!(CentralityTables, [counters, meta, centrality]);

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct CentralityJob {
    shard: u64,
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

    use crate::{free_socket_addr, webgraph::centrality::harmonic::HarmonicCentrality};

    use super::*;

    #[test]
    #[traced_test]
    fn test_simple_graph() {
        let graph = crate::webgraph::tests::test_graph();
        let expected = HarmonicCentrality::calculate(&graph);
        let num_nodes = graph.nodes().count();
        let (dht_shard, dht_addr) = crate::entrypoint::ampc::dht::tests::setup();
        let setup = CentralitySetup::new_for_members(&[(dht_shard, dht_addr)]);
        let worker = CentralityWorker::new(1, graph);

        let worker_addr = free_socket_addr();

        std::thread::spawn(move || {
            worker.run(worker_addr).unwrap();
        });

        let remote_worker = RemoteCentralityWorker::new(1, worker_addr);

        let res = coordinator::build(setup, vec![remote_worker])
            .run(vec![CentralityJob { shard: 1 }], CentralityFinish)
            .unwrap();

        let mut actual = Vec::new();
        let mut expected = expected.iter().map(|(n, c)| (*n, c)).collect::<Vec<_>>();

        for (node, _) in &expected {
            actual.push((
                *node,
                f64::from(res.centrality.get(*node).unwrap_or_default()) / (num_nodes - 1) as f64,
            ));
        }

        actual.sort_by(|a, b| a.0.cmp(&b.0));
        expected.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(expected, actual);

        for (expected, actual) in expected
            .iter()
            .map(|(_, c)| c)
            .zip(actual.iter().map(|(_, c)| c))
        {
            assert!((expected - actual).abs() < 0.0001);
        }
    }
}
