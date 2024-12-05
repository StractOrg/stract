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

use std::sync::Arc;

use futures::{stream::FuturesUnordered, StreamExt};
use itertools::Itertools;
use tokio::sync::Mutex;
use url::Url;

use crate::{
    ampc::dht::ShardId,
    distributed::{
        cluster::Cluster,
        member::Service,
        sonic::{
            self,
            replication::{
                AllShardsSelector, RandomReplicaSelector, RemoteClient, ReplicatedClient,
            },
        },
        streaming_response::StreamingResponse,
    },
    entrypoint::webgraph_server::{self, GetPageNodeIDs, Query, RetrieveReq, WebGraphService},
    webgraph::Collector,
    Result,
};

use super::{query::BacklinksQuery, EdgeLimit, Node, NodeID};
use crate::webgraph;

impl sonic::replication::ShardIdentifier for ShardId {}

struct WebgraphClientManager;

impl sonic::replication::ReusableClientManager for WebgraphClientManager {
    const CLIENT_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

    type Service = WebGraphService;
    type ShardId = ShardId;

    async fn new_client(
        cluster: &Cluster,
    ) -> sonic::replication::ShardedClient<Self::Service, Self::ShardId> {
        let shards = cluster
            .members()
            .await
            .into_iter()
            .filter_map(|member| {
                if let Service::Webgraph { host, shard } = member.service {
                    Some((shard, RemoteClient::<WebGraphService>::new(host)))
                } else {
                    None
                }
            })
            .into_group_map();

        let shards: Vec<_> = shards
            .into_iter()
            .map(|(shard, clients)| {
                let replica = ReplicatedClient::new(clients);
                sonic::replication::Shard::new(shard, replica)
            })
            .collect();

        sonic::replication::ShardedClient::new(shards)
    }
}

#[derive(Clone)]
pub struct RemoteWebgraph {
    client: Arc<Mutex<sonic::replication::ReusableShardedClient<WebgraphClientManager>>>,
    cluster: Arc<Cluster>,
}

impl RemoteWebgraph {
    pub async fn new(cluster: Arc<Cluster>) -> Self {
        Self {
            client: Arc::new(Mutex::new(
                sonic::replication::ReusableShardedClient::new(cluster.clone()).await,
            )),
            cluster,
        }
    }

    pub async fn await_ready(&self) {
        tracing::info!("waiting for webgraph to come online...");
        self.cluster
            .await_member(|member| matches!(member.service, Service::Webgraph { .. }))
            .await;
    }

    async fn conn(&self) -> Arc<sonic::replication::ShardedClient<WebGraphService, ShardId>> {
        self.client.lock().await.conn().await
    }

    pub async fn search_initial<Q>(
        &self,
        query: &Q,
    ) -> Result<<Q::Collector as webgraph::Collector>::Fruit>
    where
        Q: Query,
        Result<
            <Q::Collector as webgraph::Collector>::Fruit,
            webgraph_server::EncodedError,
        >: From<<Q as sonic::service::Message<WebGraphService>>::Response>,
        <<Q::Collector as webgraph::Collector>::Child as tantivy::collector::SegmentCollector>::Fruit:
            From<<Q::Collector as webgraph::Collector>::Fruit>,
    {
        let collector = query.coordinator_collector();

        let res = self
            .conn()
            .await
            .send(query.clone(), &AllShardsSelector, &RandomReplicaSelector)
            .await?;

        let fruits: Vec<<<Q::Collector as webgraph::Collector>::Child as tantivy::collector::SegmentCollector>::Fruit> = res
            .into_iter()
            .flatten()
            .flat_map(|(_, reps)| reps)
            .filter_map(|(_, rep)| {
                Result::<
                    <Q::Collector as webgraph::Collector>::Fruit,
                    webgraph_server::EncodedError,
                >::from(rep)
                .ok()
            })
            .map(|fruit| {
                <<Q::Collector as webgraph::Collector>::Child as tantivy::collector::SegmentCollector>::Fruit::from(fruit)
            })
            .collect();

        collector
            .merge_fruits(fruits)
            .map_err(|_| anyhow::anyhow!("failed to merge fruits"))
    }

    pub async fn retrieve<Q>(
        &self,
        query: Q,
        fruit: <Q::Collector as webgraph::Collector>::Fruit,
    ) -> Result<Vec<Q::IntermediateOutput>>
    where
        Q: Query,
        <Q::Collector as webgraph::Collector>::Fruit: Clone,
        <Q as webgraph_server::Query>::RetrieveReq: sonic::service::Wrapper<WebGraphService>,
        Result<Q::IntermediateOutput, webgraph_server::EncodedError>: From<
            <<Q as webgraph_server::Query>::RetrieveReq as sonic::service::Message<
                WebGraphService,
            >>::Response,
        >,
    {
        let conn = self.conn().await;
        let mut results = FuturesUnordered::new();
        for shard in conn.shards() {
            let fruit = query.filter_fruit_shards(*shard.id(), fruit.clone());
            let req = Q::RetrieveReq::new(query.clone(), fruit);
            results.push(shard.replicas().send(req, &RandomReplicaSelector));
        }
        let mut res = Vec::new();

        while let Some(shard_res) = results.next().await {
            if let Ok(shard_res) = shard_res {
                res.push(shard_res);
            }
        }

        Ok(res
            .into_iter()
            .flatten()
            .filter_map(|(_, res)| {
                Result::<Q::IntermediateOutput, webgraph_server::EncodedError>::from(res).ok()
            })
            .collect())
    }

    pub async fn search<Q>(&self, query: Q) -> Result<Q::Output>
    where
        Q: Query,
        <Q::Collector as webgraph::Collector>::Fruit: Clone,
        Result<
            <Q::Collector as webgraph::Collector>::Fruit,
            webgraph_server::EncodedError,
        >: From<<Q as sonic::service::Message<WebGraphService>>::Response>,
        <<Q::Collector as webgraph::Collector>::Child as tantivy::collector::SegmentCollector>::Fruit:
            From<<Q::Collector as webgraph::Collector>::Fruit>,
        <Q as webgraph_server::Query>::RetrieveReq: sonic::service::Wrapper<WebGraphService>,
        Result<Q::IntermediateOutput, webgraph_server::EncodedError>: From<
            <<Q as webgraph_server::Query>::RetrieveReq as sonic::service::Message<
                WebGraphService,
            >>::Response,
        >,
    {
        let fruit = self.search_initial(&query).await?;
        let res = self.retrieve(query, fruit).await?;
        let output = Q::merge_results(res);
        Ok(output)
    }

    pub async fn batch_search_initial<Q>(
        &self,
        queries: &[Q],
    ) -> Result<Vec<<Q::Collector as webgraph::Collector>::Fruit>>
    where
        Q: Query,
        <Q::Collector as webgraph::Collector>::Fruit: Clone,
        Result<<<Q::Collector as webgraph::Collector>::Child as tantivy::collector::SegmentCollector>::Fruit, webgraph_server::EncodedError>:
            From<<Q as sonic::service::Message<WebGraphService>>::Response>,
    {
        let res = self
            .conn()
            .await
            .batch_send(queries, &AllShardsSelector, &RandomReplicaSelector)
            .await?;

        let mut fruits = Vec::with_capacity(queries.len());

        for _ in 0..queries.len() {
            fruits.push(Vec::new());
        }

        for (_, replica_results) in res.into_iter() {
            debug_assert_eq!(replica_results.len(), 1);

            for (_, shard_results) in replica_results.into_iter() {
                for (i, shard_result) in shard_results.into_iter().enumerate() {
                    if let Ok(shard_result) =
                        Result::<_, webgraph_server::EncodedError>::from(shard_result)
                    {
                        fruits[i].push(shard_result);
                    }
                }
            }
        }

        queries
            .iter()
            .zip_eq(fruits.into_iter())
            .map(|(query, shard_fruits)| query.coordinator_collector().merge_fruits(shard_fruits))
            .collect::<Result<Vec<_>, _>>()
    }

    pub async fn batch_retrieve<Q>(
        &self,
        queries: Vec<(Q, <Q::Collector as webgraph::Collector>::Fruit)>,
    ) -> Result<Vec<Vec<Q::IntermediateOutput>>>
    where
        Q: Query,
        <Q as webgraph_server::Query>::RetrieveReq: sonic::service::Wrapper<WebGraphService>,
        Result<Q::IntermediateOutput, webgraph_server::EncodedError>: From<
            <<Q as webgraph_server::Query>::RetrieveReq as sonic::service::Message<
                WebGraphService,
            >>::Response,
        >,
        <Q::Collector as webgraph::Collector>::Fruit: Clone,
    {
        let conn = self.conn().await;
        let mut results = FuturesUnordered::new();

        for shard in conn.shards() {
            let retrieve_requests: Vec<_> = queries
                .iter()
                .map(|(query, fruit)| {
                    let fruit = query.filter_fruit_shards(*shard.id(), fruit.clone());
                    Q::RetrieveReq::new(query.clone(), fruit)
                })
                .collect();

            results.push(async move {
                let retrieve_requests = retrieve_requests; // move lifetime
                shard
                    .replicas()
                    .batch_send(&retrieve_requests, &RandomReplicaSelector)
                    .await
            });
        }

        let mut res = Vec::new();

        for _ in 0..queries.len() {
            res.push(Vec::new());
        }

        while let Some(shard_res) = results.next().await {
            for (_, shard_res) in shard_res? {
                assert_eq!(shard_res.len(), queries.len());

                for (i, query_res) in shard_res.into_iter().enumerate() {
                    res[i].push(
                        <Result<Q::IntermediateOutput, webgraph_server::EncodedError>>::from(
                            query_res,
                        )
                        .map_err(|e| anyhow::anyhow!("{e}"))?,
                    );
                }
            }
        }

        Ok(res)
    }

    pub async fn batch_search<Q>(&self, queries: Vec<Q>) -> Result<Vec<Q::Output>>
    where
        Q: Query,
        <Q::Collector as webgraph::Collector>::Fruit: Clone,
        Result<<<Q::Collector as webgraph::Collector>::Child as tantivy::collector::SegmentCollector>::Fruit, webgraph_server::EncodedError>:
            From<<Q as sonic::service::Message<WebGraphService>>::Response>,
            <Q as webgraph_server::Query>::RetrieveReq: sonic::service::Wrapper<WebGraphService>,
        Result<Q::IntermediateOutput, webgraph_server::EncodedError>: From<
            <<Q as webgraph_server::Query>::RetrieveReq as sonic::service::Message<
                WebGraphService,
            >>::Response,
        >,
    {
        let res = self.batch_search_initial(&queries).await?;
        let res = self
            .batch_retrieve(queries.into_iter().zip(res).collect())
            .await?;
        Ok(res.into_iter().map(|v| Q::merge_results(v)).collect())
    }

    pub async fn knows(&self, mut host: String) -> Result<Option<Node>> {
        if let Some(suf) = host.strip_prefix("http://") {
            host = suf.to_string();
        }
        if let Some(suf) = host.strip_prefix("https://") {
            host = suf.to_string();
        }

        let url = Url::parse(&("http://".to_string() + host.as_str()))?;
        let node = Node::from(url).into_host();
        let id = node.id();
        let edges = self
            .search(BacklinksQuery::new(id).with_limit(EdgeLimit::Limit(1)))
            .await?;

        if !edges.is_empty() {
            Ok(Some(node))
        } else {
            Ok(None)
        }
    }

    pub async fn stream_page_node_ids(&self) -> impl futures::Stream<Item = NodeID> {
        StreamNodeIDs::new(self.conn().await).stream()
    }
}

pub struct StreamNodeIDs {
    offset: u64,
    limit: u64,
    conn: Arc<sonic::replication::ShardedClient<WebGraphService, ShardId>>,
}

impl StreamNodeIDs {
    pub fn new(conn: Arc<sonic::replication::ShardedClient<WebGraphService, ShardId>>) -> Self {
        Self {
            offset: 0,
            limit: 2048,
            conn,
        }
    }
}

impl StreamingResponse for StreamNodeIDs {
    type Item = NodeID;

    async fn next_batch(&mut self) -> Result<Vec<Self::Item>> {
        let req = GetPageNodeIDs {
            offset: self.offset,
            limit: self.limit,
        };

        let res = self
            .conn
            .send(req, &AllShardsSelector, &RandomReplicaSelector)
            .await?;
        self.offset += self.limit;

        Ok(res
            .into_iter()
            .flatten()
            .flat_map(|(_, v)| v.into_iter().flat_map(|(_, v)| v))
            .collect())
    }
}
