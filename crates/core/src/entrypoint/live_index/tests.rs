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

use crate::{live_index::LiveIndex, Result};
use std::{net::SocketAddr, sync::Arc};

use tokio::task::JoinHandle;

use crate::{
    ampc::dht::ShardId,
    distributed::{
        cluster::Cluster,
        member::{LiveIndexState, Service},
        sonic,
    },
    entrypoint::{indexer::IndexableWebpage, live_index::IndexWebpages},
    free_socket_addr,
};

use super::LiveIndexService;

struct RemoteIndex {
    host: SocketAddr,
    shard: ShardId,
    gossip_addr: SocketAddr,
    underlying_index: Arc<LiveIndex>,
    handle: JoinHandle<()>,
}

impl RemoteIndex {
    async fn conn(&self) -> Result<sonic::service::Connection<LiveIndexService>> {
        Ok(sonic::service::Connection::create(self.host).await?)
    }

    async fn index_pages(
        &self,
        pages: Vec<IndexableWebpage>,
        consistency_fraction: Option<f64>,
    ) -> Result<()> {
        self.conn()
            .await?
            .send(IndexWebpages {
                pages,
                consistency_fraction,
            })
            .await??;

        Ok(())
    }

    async fn await_ready(&self, cluster: &Cluster) {
        cluster
            .await_member(|member| {
                if let Service::LiveIndex {
                    host: _,
                    shard,
                    state,
                } = member.service.clone()
                {
                    self.shard == shard && matches!(state, LiveIndexState::Ready)
                } else {
                    false
                }
            })
            .await;
    }

    async fn commit_underlying(&self) -> Result<()> {
        let index = Arc::clone(&self.underlying_index);
        tokio::task::spawn_blocking(move || index.commit()).await?;

        Ok(())
    }
}

const CLUSTER_ID: &str = "test-cluster";

async fn start_index(shard: ShardId, gossip: Vec<SocketAddr>) -> Result<RemoteIndex> {
    todo!()
}

#[tokio::test]
async fn test_shard_without_replica() -> Result<()> {
    let shard1 = start_index(ShardId::new(1), vec![]).await?;
    let shard2 = start_index(ShardId::new(2), vec![shard1.gossip_addr]).await?;

    let cluster = Cluster::join_as_spectator(
        CLUSTER_ID.to_string(),
        free_socket_addr(),
        vec![shard1.gossip_addr],
    )
    .await?;

    shard1.await_ready(&cluster).await;
    shard2.await_ready(&cluster).await;

    shard1
        .index_pages(
            vec![IndexableWebpage {
                url: "https://a.com/".to_string(),
                body: "
                <title>test page</title>
                Example webpage
                "
                .to_string(),
                fetch_time_ms: 100,
            }],
            None,
        )
        .await?;
    shard2
        .index_pages(
            vec![IndexableWebpage {
                url: "https://b.com/".to_string(),
                body: "
                <title>test page</title>
                Example webpage
                "
                .to_string(),
                fetch_time_ms: 100,
            }],
            None,
        )
        .await?;

    shard1.commit_underlying().await?;
    shard2.commit_underlying().await?;

    todo!("test searches");
}

#[tokio::test]
async fn test_replica_no_fails() -> Result<()> {
    todo!("start cluster with shard1_rep1 and shard1_rep2, insert some pages and check they are all indexed");
}

#[tokio::test]
async fn test_replica_setup_after_inserts() -> Result<()> {
    todo!("start cluser with shard1_rep1, insert page, start shard1_rep2, check that shard1_rep2 has page");
}

#[tokio::test]
async fn test_replica_recovery() -> Result<()> {
    todo!("start cluser with shard1_rep1 shard1_rep2, insert page, kill shard1_rep2, insert another page, start shard1_rep2, check that shard1_rep2 has both pages");
}
