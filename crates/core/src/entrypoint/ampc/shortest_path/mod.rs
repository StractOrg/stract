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

//! Single-source shortest path.

pub mod coordinator;
mod mapper;
mod updated_nodes;
pub mod worker;

pub use updated_nodes::UpdatedNodes;

use crate::distributed::member::ShardId;
use crate::{
    ampc::{prelude::*, DefaultDhtTable},
    webgraph,
};

pub use self::mapper::ShortestPathMapper;
pub use self::worker::{RemoteShortestPathWorker, ShortestPathWorker};

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
    round: u64,
}

#[derive(bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct ShortestPathTables {
    pub distances: DefaultDhtTable<webgraph::NodeID, u64>,
    pub meta: DefaultDhtTable<(), Meta>,
    pub changed_nodes: DefaultDhtTable<ShardId, UpdatedNodes>,
}

impl_dht_tables!(ShortestPathTables, [distances, meta, changed_nodes]);

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct ShortestPathJob {
    pub shard: ShardId,
    pub source: webgraph::NodeID,
}

impl Job for ShortestPathJob {
    type DhtTables = ShortestPathTables;
    type Worker = ShortestPathWorker;
    type Mapper = ShortestPathMapper;

    fn is_schedulable(&self, worker: &RemoteShortestPathWorker) -> bool {
        self.shard == worker.shard()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use tracing_test::traced_test;
    use webgraph::{Edge, ShortestPaths, Webgraph};

    use crate::{config::WebgraphGranularity, free_socket_addr};

    use super::*;

    #[test]
    #[traced_test]
    fn test_simple_graph() {
        let temp_dir = crate::gen_temp_dir().unwrap();
        let mut combined = Webgraph::builder(temp_dir.as_ref().join("combined"), 0u64.into())
            .open()
            .unwrap();
        let mut a = Webgraph::builder(temp_dir.as_ref().join("a"), 0u64.into())
            .open()
            .unwrap();
        let mut b = Webgraph::builder(temp_dir.as_ref().join("b"), 0u64.into())
            .open()
            .unwrap();

        let edges = crate::webgraph::tests::test_edges();

        for (i, (from, to)) in edges.into_iter().enumerate() {
            let e = Edge::new_test(from.clone(), to.clone());
            combined.insert(e.clone()).unwrap();

            if i % 2 == 0 {
                a.insert(e).unwrap();
            } else {
                b.insert(e).unwrap();
            }
        }

        combined.commit().unwrap();
        a.commit().unwrap();
        b.commit().unwrap();

        let a = Arc::new(a);
        let b = Arc::new(b);

        let node = webgraph::Node::from("C");

        let expected = combined
            .raw_distances(node.id(), WebgraphGranularity::Page)
            .into_iter()
            .map(|(node, dist)| (node, dist as u64))
            .collect::<BTreeMap<_, _>>();

        let worker = ShortestPathWorker::new(a, 1.into());

        let worker_addr = free_socket_addr();

        std::thread::spawn(move || {
            worker.run(worker_addr).unwrap();
        });

        std::thread::sleep(std::time::Duration::from_secs(2)); // Wait for worker to start
        let a = RemoteShortestPathWorker::new(1.into(), worker_addr).unwrap();

        let worker = ShortestPathWorker::new(b, 2.into());
        let worker_addr = free_socket_addr();
        std::thread::spawn(move || {
            worker.run(worker_addr).unwrap();
        });

        std::thread::sleep(std::time::Duration::from_secs(2)); // Wait for worker to start

        let b = RemoteShortestPathWorker::new(2.into(), worker_addr).unwrap();

        let (dht_shard, dht_addr) = crate::entrypoint::ampc::dht::tests::setup();

        let res = coordinator::build(
            &[(dht_shard, dht_addr)],
            vec![a.clone(), b.clone()],
            node.id(),
        )
        .run(
            vec![
                ShortestPathJob {
                    shard: a.shard(),
                    source: node.id(),
                },
                ShortestPathJob {
                    shard: b.shard(),
                    source: node.id(),
                },
            ],
            coordinator::ShortestPathFinish {
                max_distance: Some(128),
            },
        )
        .unwrap();

        let actual = res.distances.iter().collect::<BTreeMap<_, _>>();

        assert_eq!(expected, actual);
    }
}
