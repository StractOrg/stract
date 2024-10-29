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
use crate::distributed::sonic;
use crate::distributed::sonic::service::sonic_service;
use crate::distributed::sonic::service::Message;
use crate::webgraph;
use crate::webgraph::query::BacklinksQuery;
use crate::webgraph::query::BacklinksWithLabelsQuery;
use crate::webgraph::query::ForwardlinksQuery;
use crate::webgraph::query::FullBacklinksQuery;
use crate::webgraph::query::FullForwardlinksQuery;
use crate::webgraph::query::FullHostBacklinksQuery;
use crate::webgraph::query::FullHostForwardlinksQuery;
use crate::webgraph::query::HostBacklinksQuery;
use crate::webgraph::query::HostForwardlinksQuery;
use crate::webgraph::query::HostGroupSketchQuery;
use crate::webgraph::query::Id2NodeQuery;
use crate::webgraph::NodeID;
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

pub trait RetrieveReq: bincode::Encode + bincode::Decode + Clone {
    type Query: webgraph::Query + bincode::Encode + bincode::Decode;
    fn new(
        query: Self::Query,
        fruit: <<Self::Query as webgraph::Query>::Collector as webgraph::Collector>::Fruit,
    ) -> Self;
}

pub trait Query
where
    Self: webgraph::Query
        + bincode::Encode
        + bincode::Decode
        + sonic::service::Wrapper<WebGraphService>,
{
    type RetrieveReq: RetrieveReq<Query = Self>;
}

#[derive(bincode::Encode, bincode::Decode, Clone)]
pub struct EncodedError {
    pub msg: String,
}

impl std::fmt::Display for EncodedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

macro_rules! impl_search {
    ([$($q:ident),*$(,)?]) => {
        $(
            impl Message<WebGraphService> for $q {
                type Response = Result<<<$q as webgraph::Query>::Collector as webgraph::Collector>::Fruit, EncodedError>;

                async fn handle(self, server: &WebGraphService) -> Self::Response {
                    server.graph.search_initial(&self).map_err(|e| EncodedError { msg: e.to_string() })
                }
            }

            paste::item! {
                #[derive(bincode::Encode, bincode::Decode, Clone)]
                pub struct [<$q Retrieve>] {
                    pub query: $q,
                    #[bincode(with_serde)]
                    pub fruit: <<$q as webgraph::Query>::Collector as webgraph::Collector>::Fruit,
                }

                impl Message<WebGraphService> for [<$q Retrieve>] {
                    type Response = Result<<$q as webgraph::Query>::IntermediateOutput, EncodedError>;
                    async fn handle(self, server: &WebGraphService) -> Self::Response {
                        server.graph.retrieve(&self.query, self.fruit).map_err(|e| EncodedError { msg: e.to_string() })
                    }
                }

                impl Query for $q {
                    type RetrieveReq = [<$q Retrieve>];
                }

                impl RetrieveReq for [<$q Retrieve>] {
                    type Query = $q;
                    fn new(query: Self::Query, fruit: <<Self::Query as webgraph::Query>::Collector as webgraph::Collector>::Fruit) -> Self {
                        Self { query, fruit }
                    }
                }
            }
        )*

        paste::item! {
            sonic_service!(WebGraphService, [GetPageNodeIDs, $(
                $q,
                [<$q Retrieve>],
            )*]);
        }

    }
}

impl_search!([
    BacklinksQuery,
    HostBacklinksQuery,
    FullBacklinksQuery,
    FullHostBacklinksQuery,
    BacklinksWithLabelsQuery,
    Id2NodeQuery,
    ForwardlinksQuery,
    HostForwardlinksQuery,
    FullForwardlinksQuery,
    FullHostForwardlinksQuery,
    HostGroupSketchQuery,
]);

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct GetPageNodeIDs {
    pub offset: u64,
    pub limit: u64,
}

impl Message<WebGraphService> for GetPageNodeIDs {
    type Response = Vec<NodeID>;

    async fn handle(self, server: &WebGraphService) -> Self::Response {
        server
            .graph
            .page_node_ids_with_offset(self.offset, self.limit)
    }
}

pub async fn run(config: config::WebgraphServerConfig) -> Result<()> {
    let addr: SocketAddr = config.host;

    let graph = Arc::new(WebgraphBuilder::new(config.graph_path, config.shard).open()?);

    let server = WebGraphService { graph }.bind(addr).await.unwrap();

    // dropping the handle leaves the cluster
    let _cluster = Arc::new(
        Cluster::join(
            Member::new(Service::Webgraph {
                host: addr,
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
