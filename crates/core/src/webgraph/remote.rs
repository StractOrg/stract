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
use ring::rand::Random;
use tokio::sync::Mutex;
use url::Url;

use crate::{
    config,
    distributed::{
        cluster::Cluster,
        member::{Service, ShardId},
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

use super::{
    query::{
        id2node::Id2NodeQuery, BacklinksQuery, BacklinksWithLabelsQuery, ForwardlinksQuery,
        FullBacklinksQuery, FullForwardlinksQuery,
    },
    Edge, EdgeLimit, Node, NodeID, SmallEdge, SmallEdgeWithLabel,
};
use crate::webgraph;

struct WebgraphClientManager<G: WebgraphGranularity>(std::marker::PhantomData<G>);

pub trait WebgraphGranularity: Clone {
    fn granularity() -> config::WebgraphGranularity;
}

#[derive(Clone)]
pub struct Page;

impl WebgraphGranularity for Page {
    fn granularity() -> config::WebgraphGranularity {
        config::WebgraphGranularity::Page
    }
}

#[derive(Clone)]
pub struct Host;

impl WebgraphGranularity for Host {
    fn granularity() -> config::WebgraphGranularity {
        config::WebgraphGranularity::Host
    }
}

impl<G> sonic::replication::ReusableClientManager for WebgraphClientManager<G>
where
    G: WebgraphGranularity,
{
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
                if let Service::Webgraph {
                    host,
                    shard,
                    granularity,
                } = member.service
                {
                    if granularity == G::granularity() {
                        Some((shard, RemoteClient::<WebGraphService>::new(host)))
                    } else {
                        None
                    }
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
pub struct RemoteWebgraph<G: WebgraphGranularity> {
    client: Arc<Mutex<sonic::replication::ReusableShardedClient<WebgraphClientManager<G>>>>,
    cluster: Arc<Cluster>,
}

impl<G: WebgraphGranularity> RemoteWebgraph<G> {
    pub async fn new(cluster: Arc<Cluster>) -> Self {
        Self {
            client: Arc::new(Mutex::new(
                sonic::replication::ReusableShardedClient::new(cluster.clone()).await,
            )),
            cluster,
        }
    }

    pub async fn await_ready(&self) {
        let granularity = G::granularity();
        tracing::info!("waiting for {granularity} webgraph to come online...");
        self.cluster
            .await_member(|member| {
                if let Service::Webgraph {
                    host: _,
                    shard: _,
                    granularity: remote_granularity,
                } = member.service
                {
                    granularity == remote_granularity
                } else {
                    false
                }
            })
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
        let collector = query.remote_collector();

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

        for (query, (_, shard_results)) in queries.iter().zip(res.into_iter()) {
            let merged_fruit = query.remote_collector().merge_fruits(
                shard_results
                    .into_iter()
                    .flat_map(|(_, reps)| reps)
                    .filter_map(|res| Result::<_, webgraph_server::EncodedError>::from(res).ok())
                    .collect(),
            )?;

            fruits.push(merged_fruit);
        }

        Ok(fruits)
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
        let edges = self.raw_ingoing_edges(id, EdgeLimit::Limit(1)).await?;

        if !edges.is_empty() {
            Ok(Some(node))
        } else {
            Ok(None)
        }
    }

    pub async fn get_page_node(&self, id: NodeID) -> Result<Option<Node>> {
        self.search(Id2NodeQuery::Page(id)).await
    }

    pub async fn batch_get_page_node(&self, ids: &[NodeID]) -> Result<Vec<Option<Node>>> {
        self.batch_search(ids.iter().map(|id| Id2NodeQuery::Page(*id)).collect())
            .await
    }

    pub async fn get_host_node(&self, id: NodeID) -> Result<Option<Node>> {
        self.search(Id2NodeQuery::Host(id)).await
    }

    pub async fn batch_get_host_node(&self, ids: &[NodeID]) -> Result<Vec<Option<Node>>> {
        self.batch_search(ids.iter().map(|id| Id2NodeQuery::Host(*id)).collect())
            .await
    }

    pub async fn ingoing_edges(&self, node: Node, limit: EdgeLimit) -> Result<Vec<Edge>> {
        self.search(FullBacklinksQuery::new(node).with_limit(limit))
            .await
    }

    pub async fn raw_ingoing_edges(&self, id: NodeID, limit: EdgeLimit) -> Result<Vec<SmallEdge>> {
        self.search(BacklinksQuery::new(id).with_limit(limit)).await
    }

    pub async fn raw_ingoing_edges_with_labels(
        &self,
        id: NodeID,
        limit: EdgeLimit,
    ) -> Result<Vec<SmallEdgeWithLabel>> {
        self.search(BacklinksWithLabelsQuery::new(id).with_limit(limit))
            .await
    }

    pub async fn batch_raw_ingoing_edges_with_labels(
        &self,
        ids: &[NodeID],
        limit: EdgeLimit,
    ) -> Result<Vec<Vec<SmallEdgeWithLabel>>> {
        self.batch_search(
            ids.iter()
                .map(|id| BacklinksWithLabelsQuery::new(*id).with_limit(limit))
                .collect(),
        )
        .await
    }

    pub async fn batch_raw_ingoing_edges(
        &self,
        ids: &[NodeID],
        limit: EdgeLimit,
    ) -> Result<Vec<Vec<SmallEdge>>> {
        self.batch_search(
            ids.iter()
                .map(|id| BacklinksQuery::new(*id).with_limit(limit))
                .collect(),
        )
        .await
    }

    pub async fn outgoing_edges(&self, node: Node, limit: EdgeLimit) -> Result<Vec<Edge>> {
        self.search(FullForwardlinksQuery::new(node).with_limit(limit))
            .await
    }

    pub async fn raw_outgoing_edges(&self, id: NodeID, limit: EdgeLimit) -> Result<Vec<SmallEdge>> {
        self.search(ForwardlinksQuery::new(id).with_limit(limit))
            .await
    }

    pub async fn batch_raw_outgoing_edges(
        &self,
        ids: &[NodeID],
        limit: EdgeLimit,
    ) -> Result<Vec<Vec<SmallEdge>>> {
        self.batch_search(
            ids.iter()
                .map(|id| ForwardlinksQuery::new(*id).with_limit(limit))
                .collect(),
        )
        .await
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
