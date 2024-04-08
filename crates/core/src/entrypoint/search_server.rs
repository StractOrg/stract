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

use std::{collections::HashMap, path::Path};

use serde::{Deserialize, Serialize};
use tracing::info;
use url::Url;

use crate::{
    config,
    distributed::sonic::service::sonic_service,
    distributed::{
        cluster::Cluster,
        member::{Member, Service},
        sonic,
    },
    index::Index,
    inverted_index::{self, RetrievedWebpage},
    models::dual_encoder::DualEncoder,
    ranking::{
        inbound_similarity::InboundSimilarity,
        models::{lambdamart::LambdaMART, linear::LinearRegression},
    },
    searcher::{InitialWebsiteResult, LocalSearcher, SearchQuery},
    Result,
};

sonic_service!(
    SearchService,
    [
        RetrieveWebsites,
        Search,
        GetWebpage,
        GetHomepageDescriptions,
    ]
);

pub struct SearchService {
    local_searcher: LocalSearcher<Index>,
    // dropping the handle leaves the cluster
    #[allow(unused)]
    cluster_handle: Cluster,
}

impl SearchService {
    async fn new(config: config::SearchServerConfig) -> Result<Self> {
        let centrality_store = config
            .host_centrality_store_path
            .map(|p| InboundSimilarity::open(Path::new(&p).join("inbound_similarity")).unwrap());
        let search_index = Index::open(config.index_path)?;

        let mut local_searcher = LocalSearcher::new(search_index);

        if let Some(centrality_store) = centrality_store {
            local_searcher.set_inbound_similarity(centrality_store);
        }

        if let Some(model_path) = config.linear_model_path {
            local_searcher.set_linear_model(LinearRegression::open(model_path)?);
        }

        if let Some(model_path) = config.lambda_model_path {
            local_searcher.set_lambda_model(LambdaMART::open(model_path)?);
        }

        if let Some(model_path) = config.dual_encoder_model_path {
            local_searcher.set_dual_encoder(DualEncoder::open(model_path)?);
        }

        local_searcher.set_collector_config(config.collector);
        local_searcher.set_snippet_config(config.snippet);

        let cluster_handle = Cluster::join(
            Member {
                id: config.cluster_id,
                service: Service::Searcher {
                    host: config.host,
                    shard: config.shard_id,
                },
            },
            config.gossip_addr,
            config.gossip_seed_nodes.unwrap_or_default(),
        )
        .await?;

        Ok(SearchService {
            local_searcher,
            cluster_handle,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetrieveWebsites {
    pub websites: Vec<inverted_index::WebpagePointer>,
    pub query: String,
}
impl sonic::service::Message<SearchService> for RetrieveWebsites {
    type Response = Option<Vec<inverted_index::RetrievedWebpage>>;
    async fn handle(self, server: &SearchService) -> Self::Response {
        server
            .local_searcher
            .retrieve_websites(&self.websites, &self.query)
            .ok()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Search {
    pub query: SearchQuery,
}
impl sonic::service::Message<SearchService> for Search {
    type Response = Option<InitialWebsiteResult>;
    async fn handle(self, server: &SearchService) -> Self::Response {
        server.local_searcher.search_initial(&self.query, true).ok()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetWebpage {
    pub url: String,
}
impl sonic::service::Message<SearchService> for GetWebpage {
    type Response = Option<RetrievedWebpage>;
    async fn handle(self, server: &SearchService) -> Self::Response {
        server.local_searcher.get_webpage(&self.url)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetHomepageDescriptions {
    pub urls: Vec<Url>,
}
impl sonic::service::Message<SearchService> for GetHomepageDescriptions {
    type Response = HashMap<Url, String>;
    async fn handle(self, server: &SearchService) -> Self::Response {
        let mut result = HashMap::with_capacity(self.urls.len());

        for url in &self.urls {
            if let Some(homepage) = server.local_searcher.get_homepage(url) {
                if let Some(desc) = homepage.description() {
                    result.insert(url.clone(), desc.clone());
                }
            }
        }

        result
    }
}

pub async fn run(config: config::SearchServerConfig) -> Result<()> {
    let addr = config.host;
    let server = SearchService::new(config).await?.bind(addr).await.unwrap();

    info!("search server is ready to accept requests on {}", addr);

    loop {
        if let Err(e) = server.accept().await {
            tracing::error!("{:?}", e);
        }
    }
}
