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

use itertools::Itertools;
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
    entrypoint::webgraph_server::{
        GetNode, GetNodeIDs, IngoingEdges, OutgoingEdges, RawIngoingEdges,
        RawIngoingEdgesWithLabels, RawOutgoingEdges, RawOutgoingEdgesWithLabels, WebGraphService,
    },
    Result,
};

use super::{Edge, EdgeLimit, FullEdge, Node, NodeID};

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

    pub async fn get_node(&self, id: NodeID) -> Result<Option<Node>> {
        let res = self
            .conn()
            .await
            .send(
                GetNode { node: id },
                &AllShardsSelector,
                &RandomReplicaSelector,
            )
            .await?;

        Ok(res
            .into_iter()
            .flatten()
            .flat_map(|(_, res)| res.into_iter().map(|(_, v)| v))
            .find(|n| n.is_some())
            .flatten()
            .clone())
    }

    pub async fn batch_get_node(&self, ids: &[NodeID]) -> Result<Vec<Option<Node>>> {
        let reqs = ids.iter().map(|&id| GetNode { node: id }).collect_vec();

        let res = self
            .conn()
            .await
            .batch_send(&reqs, &AllShardsSelector, &RandomReplicaSelector)
            .await?;

        let mut nodes = vec![None; ids.len()];

        for (_, rep) in res {
            debug_assert!(rep.len() <= 1);

            for (_, rep_nodes) in rep {
                for (i, node) in rep_nodes.into_iter().enumerate() {
                    if let Some(node) = node {
                        nodes[i] = Some(node);
                    }
                }
            }
        }

        Ok(nodes)
    }

    pub async fn ingoing_edges(&self, node: Node, limit: EdgeLimit) -> Result<Vec<FullEdge>> {
        let res = self
            .conn()
            .await
            .send(
                IngoingEdges { node, limit },
                &AllShardsSelector,
                &RandomReplicaSelector,
            )
            .await?;

        Ok(res
            .into_iter()
            .flatten()
            .flat_map(|(_, reps)| {
                debug_assert!(reps.len() <= 1);
                reps.into_iter().flat_map(|(_, rep)| rep)
            })
            .collect())
    }

    pub async fn raw_ingoing_edges(&self, id: NodeID, limit: EdgeLimit) -> Result<Vec<Edge<()>>> {
        let res = self
            .conn()
            .await
            .send(
                RawIngoingEdges { node: id, limit },
                &AllShardsSelector,
                &RandomReplicaSelector,
            )
            .await?;

        Ok(res
            .into_iter()
            .flatten()
            .flat_map(|(_, reps)| {
                debug_assert!(reps.len() <= 1);
                reps.into_iter().flat_map(|(_, rep)| rep)
            })
            .collect())
    }

    pub async fn raw_ingoing_edges_with_labels(
        &self,
        id: NodeID,
        limit: EdgeLimit,
    ) -> Result<Vec<Edge<String>>> {
        let res = self
            .conn()
            .await
            .send(
                RawIngoingEdgesWithLabels { node: id, limit },
                &AllShardsSelector,
                &RandomReplicaSelector,
            )
            .await?;

        Ok(res
            .into_iter()
            .flatten()
            .flat_map(|(_, reps)| {
                debug_assert!(reps.len() <= 1);
                reps.into_iter().flat_map(|(_, rep)| rep)
            })
            .collect())
    }

    pub async fn batch_raw_ingoing_edges_with_labels(
        &self,
        ids: &[NodeID],
        limit: EdgeLimit,
    ) -> Result<Vec<Vec<Edge<String>>>> {
        let reqs: Vec<_> = ids
            .iter()
            .map(|id| RawIngoingEdgesWithLabels { node: *id, limit })
            .collect();

        let res = self
            .conn()
            .await
            .batch_send(&reqs, &AllShardsSelector, &RandomReplicaSelector)
            .await?;

        let mut edges = vec![vec![]; ids.len()];

        for (_, res) in res {
            debug_assert!(res.len() <= 1);

            for (_, res) in res {
                for (i, rep) in res.into_iter().enumerate() {
                    edges[i].extend(rep);
                }
            }
        }

        Ok(edges)
    }

    pub async fn batch_raw_ingoing_edges(
        &self,
        ids: &[NodeID],
        limit: EdgeLimit,
    ) -> Result<Vec<Vec<Edge<()>>>> {
        let reqs: Vec<_> = ids
            .iter()
            .map(|id| RawIngoingEdges { node: *id, limit })
            .collect();

        let res = self
            .conn()
            .await
            .batch_send(&reqs, &AllShardsSelector, &RandomReplicaSelector)
            .await?;

        let mut edges = vec![vec![]; ids.len()];

        for (_, res) in res {
            debug_assert!(res.len() <= 1);

            for (_, res) in res {
                for (i, rep) in res.into_iter().enumerate() {
                    edges[i].extend(rep);
                }
            }
        }

        Ok(edges)
    }

    pub async fn outgoing_edges(&self, node: Node, limit: EdgeLimit) -> Result<Vec<FullEdge>> {
        let res = self
            .conn()
            .await
            .send(
                OutgoingEdges { node, limit },
                &AllShardsSelector,
                &RandomReplicaSelector,
            )
            .await?;

        Ok(res
            .into_iter()
            .flatten()
            .flat_map(|(_, reps)| {
                debug_assert!(reps.len() <= 1);
                reps.into_iter().flat_map(|(_, rep)| rep)
            })
            .collect())
    }

    pub async fn raw_outgoing_edges(&self, id: NodeID, limit: EdgeLimit) -> Result<Vec<Edge<()>>> {
        let res = self
            .conn()
            .await
            .send(
                RawOutgoingEdges { node: id, limit },
                &AllShardsSelector,
                &RandomReplicaSelector,
            )
            .await?;

        Ok(res
            .into_iter()
            .flatten()
            .flat_map(|(_, reps)| {
                debug_assert!(reps.len() <= 1);
                reps.into_iter().flat_map(|(_, rep)| rep)
            })
            .collect())
    }

    pub async fn raw_outgoing_edges_with_labels(
        &self,
        id: NodeID,
        limit: EdgeLimit,
    ) -> Result<Vec<Edge<String>>> {
        let res = self
            .conn()
            .await
            .send(
                RawOutgoingEdgesWithLabels { node: id, limit },
                &AllShardsSelector,
                &RandomReplicaSelector,
            )
            .await?;

        Ok(res
            .into_iter()
            .flatten()
            .flat_map(|(_, reps)| {
                debug_assert!(reps.len() <= 1);
                reps.into_iter().flat_map(|(_, rep)| rep)
            })
            .collect())
    }

    pub async fn batch_raw_outgoing_edges(
        &self,
        ids: &[NodeID],
        limit: EdgeLimit,
    ) -> Result<Vec<Vec<Edge<()>>>> {
        let reqs: Vec<_> = ids
            .iter()
            .map(|id| RawOutgoingEdges { node: *id, limit })
            .collect();

        let res = self
            .conn()
            .await
            .batch_send(&reqs, &AllShardsSelector, &RandomReplicaSelector)
            .await?;

        let mut edges = vec![vec![]; ids.len()];

        for (_, res) in res {
            debug_assert!(res.len() <= 1);

            for (_, res) in res {
                for (i, rep) in res.into_iter().enumerate() {
                    edges[i].extend(rep);
                }
            }
        }

        Ok(edges)
    }

    pub async fn stream_node_ids(&self) -> impl futures::Stream<Item = NodeID> {
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
        let req = GetNodeIDs {
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
