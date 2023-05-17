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

use itertools::Itertools;
use serde::Deserialize;
use serde::Serialize;
use tracing::info;

use crate::distributed::cluster::Cluster;
use crate::distributed::member::Member;
use crate::distributed::member::Service;
use crate::distributed::sonic;
use crate::ranking::inbound_similarity::InboundSimilarity;
use crate::searcher::DistributedSearcher;
use crate::similar_sites::SimilarSitesFinder;
use crate::webgraph::Node;
use crate::webgraph::WebgraphBuilder;
use crate::webpage::Url;
use crate::Result;
use crate::WebgraphServerConfig;

#[derive(Serialize, Deserialize)]
pub enum Request {
    SimilarSites { sites: Vec<String>, top_n: usize },
    Knows { site: String },
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ScoredSite {
    pub site: String,
    pub score: f64,
    pub description: Option<String>,
}

const MAX_SITES: usize = 20;

pub async fn run(config: WebgraphServerConfig) -> Result<()> {
    let addr: SocketAddr = config.host;

    // dropping the handle leaves the cluster
    let cluster = Arc::new(
        Cluster::join(
            Member {
                id: config.cluster_id,
                service: Service::Webgraph { host: addr },
            },
            config.gossip_addr,
            config.gossip_seed_nodes.unwrap_or_default(),
        )
        .await?,
    );
    let searcher = DistributedSearcher::new(cluster);

    let graph = Arc::new(WebgraphBuilder::new(config.graph_path).open());
    let inbound_similarity = InboundSimilarity::open(config.inbound_similarity_path)?;

    let similar_sites_finder = SimilarSitesFinder::new(Arc::clone(&graph), inbound_similarity);

    let server = sonic::Server::bind(addr).await.unwrap();

    info!("webgraph server is ready to accept requests on {}", addr);

    loop {
        if let Ok(req) = server.accept::<Request>().await {
            match &req.body {
                Request::SimilarSites { sites, top_n } => {
                    let sites = &sites[..std::cmp::min(sites.len(), MAX_SITES)];
                    let similar_sites = similar_sites_finder.find_similar_sites(sites, *top_n);

                    let urls = similar_sites
                        .iter()
                        .map(|s| s.node.name.clone())
                        .collect_vec();

                    let descriptions = searcher.get_homepage_descriptions(&urls).await;

                    let similar_sites = similar_sites
                        .into_iter()
                        .map(|site| {
                            let description = descriptions
                                .get(&Url::from(site.node.name.clone()))
                                .cloned();

                            ScoredSite {
                                site: site.node.name,
                                score: site.score,
                                description,
                            }
                        })
                        .collect_vec();

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
