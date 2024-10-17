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

use std::net::SocketAddr;
use std::sync::Arc;

use tracing::info;
use utoipa::ToSchema;

use crate::config;
use crate::distributed::cluster::Cluster;
use crate::distributed::member::Member;
use crate::distributed::member::Service;
use crate::distributed::sonic::service::sonic_service;
use crate::distributed::sonic::service::Message;
use crate::webgraph::Edge;
use crate::webgraph::EdgeLimit;
use crate::webgraph::Node;
use crate::webgraph::NodeID;
use crate::webgraph::SmallEdge;
use crate::webgraph::SmallEdgeWithLabel;
use crate::webgraph::Webgraph;
use crate::webgraph::WebgraphBuilder;
use crate::Result;

#[derive(serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScoredHost {
    pub host: String,
    pub score: f64,
    pub description: Option<String>,
}

pub struct WebGraphService {
    graph: Arc<Webgraph>,
}

sonic_service!(
    WebGraphService,
    [
        GetNode,
        IngoingEdges,
        OutgoingEdges,
        RawIngoingEdges,
        RawOutgoingEdges,
        RawIngoingEdgesWithLabels,
        GetNodeIDs
    ]
);

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct GetNode {
    pub node: NodeID,
}

impl Message<WebGraphService> for GetNode {
    type Response = Option<Node>;

    async fn handle(self, server: &WebGraphService) -> Self::Response {
        server.graph.id2node(&self.node).ok().flatten()
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct IngoingEdges {
    pub node: Node,
    pub limit: EdgeLimit,
}

impl Message<WebGraphService> for IngoingEdges {
    type Response = Vec<Edge>;

    async fn handle(self, server: &WebGraphService) -> Self::Response {
        server.graph.ingoing_edges(self.node, self.limit)
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct OutgoingEdges {
    pub node: Node,
    pub limit: EdgeLimit,
}

impl Message<WebGraphService> for OutgoingEdges {
    type Response = Vec<Edge>;

    async fn handle(self, server: &WebGraphService) -> Self::Response {
        server.graph.outgoing_edges(self.node, self.limit)
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct RawIngoingEdges {
    pub node: NodeID,
    pub limit: EdgeLimit,
}

impl Message<WebGraphService> for RawIngoingEdges {
    type Response = Vec<SmallEdge>;

    async fn handle(self, server: &WebGraphService) -> Self::Response {
        server.graph.raw_ingoing_edges(&self.node, self.limit)
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct RawOutgoingEdges {
    pub node: NodeID,
    pub limit: EdgeLimit,
}

impl Message<WebGraphService> for RawOutgoingEdges {
    type Response = Vec<SmallEdge>;

    async fn handle(self, server: &WebGraphService) -> Self::Response {
        server.graph.raw_outgoing_edges(&self.node, self.limit)
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct RawIngoingEdgesWithLabels {
    pub node: NodeID,
    pub limit: EdgeLimit,
}

impl Message<WebGraphService> for RawIngoingEdgesWithLabels {
    type Response = Vec<SmallEdgeWithLabel>;

    async fn handle(self, server: &WebGraphService) -> Self::Response {
        server
            .graph
            .raw_ingoing_edges_with_labels(&self.node, self.limit)
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct InDegreeUpperBound {
    pub node: NodeID,
}

impl Message<WebGraphService> for InDegreeUpperBound {
    type Response = u64;

    async fn handle(self, server: &WebGraphService) -> Self::Response {
        server.graph.in_degree_upper_bound(&self.node)
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct OutDegreeUpperBound {
    pub node: NodeID,
}

impl Message<WebGraphService> for OutDegreeUpperBound {
    type Response = u64;

    async fn handle(self, server: &WebGraphService) -> Self::Response {
        server.graph.out_degree_upper_bound(&self.node)
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct GetNodeIDs {
    pub offset: u64,
    pub limit: u64,
}

impl Message<WebGraphService> for GetNodeIDs {
    type Response = Vec<NodeID>;

    async fn handle(self, server: &WebGraphService) -> Self::Response {
        server
            .graph
            .iter_nodes_with_offset(self.offset)
            .take(self.limit as usize)
            .map(|(id, _)| id)
            .collect()
    }
}

pub async fn run(config: config::WebgraphServerConfig) -> Result<()> {
    let addr: SocketAddr = config.host;

    let graph = Arc::new(WebgraphBuilder::new(config.graph_path).open()?);

    let server = WebGraphService { graph }.bind(addr).await.unwrap();

    // dropping the handle leaves the cluster
    let _cluster = Arc::new(
        Cluster::join(
            Member::new(Service::Webgraph {
                host: addr,
                granularity: config.granularity,
                shard: config.shard,
            }),
            config.gossip_addr,
            config.gossip_seed_nodes.unwrap_or_default(),
        )
        .await?,
    );

    info!("webgraph server is ready to accept requests on {}", addr);

    loop {
        if let Err(e) = server.accept().await {
            tracing::error!("{:?}", e);
        }
    }
}
