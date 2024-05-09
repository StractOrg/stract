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
    config::WebgraphGranularity,
    distributed::{
        cluster::Cluster,
        member::{Service, ShardId},
        sonic::{
            self,
            replication::{
                AllShardsSelector, RandomReplicaSelector, RemoteClient, ReplicatedClient,
            },
        },
    },
    entrypoint::webgraph_server::{
        GetNode, IngoingEdges, OutgoingEdges, RawIngoingEdges, RawIngoingEdgesWithLabels,
        RawOutgoingEdges, RawOutgoingEdgesWithLabels, WebGraphService,
    },
    Result,
};

use super::{Edge, EdgeLimit, FullEdge, Node, NodeID};

struct WebgraphClientManager {
    granularity: WebgraphGranularity,
}

impl sonic::replication::ReusableClientManager for WebgraphClientManager {
    const CLIENT_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

    type Service = WebGraphService;
    type ShardId = ShardId;

    async fn new_client(
        &self,
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
                    if granularity == self.granularity {
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

pub struct RemoteWebgraph {
    client: Mutex<sonic::replication::ReusableShardedClient<WebgraphClientManager>>,
}

impl RemoteWebgraph {
    pub async fn new(cluster: Arc<Cluster>, granularity: WebgraphGranularity) -> Self {
        let manager = WebgraphClientManager { granularity };

        Self {
            client: Mutex::new(
                sonic::replication::ReusableShardedClient::new(cluster, manager).await,
            ),
        }
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
}
