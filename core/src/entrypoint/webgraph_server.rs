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

use serde::Deserialize;
use serde::Serialize;
use tracing::info;

use crate::distributed::cluster::Cluster;
use crate::distributed::member::Member;
use crate::distributed::member::Service;
use crate::distributed::sonic;
use crate::ranking::inbound_similarity::InboundSimilarity;
use crate::similar_sites::SimilarSitesFinder;
use crate::webgraph::Node;
use crate::webgraph::WebgraphBuilder;
use crate::Result;
use crate::WebgraphServerConfig;

#[derive(Serialize, Deserialize)]
pub enum Request {
    SimilarSites { sites: Vec<String>, top_n: usize },
    Knows { site: String },
}

pub async fn run(config: WebgraphServerConfig) -> Result<()> {
    let addr: SocketAddr = config.host;

    // dropping the handle leaves the cluster
    let _cluster_handle = Cluster::join(
        Member {
            id: config.cluster_id,
            service: Service::Webgraph { host: addr },
        },
        config.gossip_addr,
        config.gossip_seed_nodes.unwrap_or_default(),
    )
    .await?;

    let graph = Arc::new(WebgraphBuilder::new(config.graph_path).open());
    let inbound_similarity = InboundSimilarity::open(config.inbound_similarity_path)?;

    let similar_sites_finder = SimilarSitesFinder::new(Arc::clone(&graph), inbound_similarity);

    let server = sonic::Server::bind(addr).await.unwrap();

    info!("webgraph server is ready to accept requests on {}", addr);

    loop {
        if let Ok(req) = server.accept::<Request>().await {
            match &req.body {
                Request::SimilarSites { sites, top_n } => {
                    let similar_sites = similar_sites_finder.find_similar_sites(sites, *top_n);
                    req.respond(sonic::Response::Content(similar_sites))
                        .await
                        .ok();
                }
                Request::Knows { site } => {
                    let node = Node::from(site.to_string()).into_host();

                    if similar_sites_finder.knows_about(&node) {
                        req.respond(sonic::Response::Content(Some(node))).await.ok();
                    } else {
                        req.respond::<Option<Node>>(sonic::Response::Content(None))
                            .await
                            .ok();
                    }
                }
            }
        }
    }
}
