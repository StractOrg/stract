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
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::{collections::HashMap, sync::Arc};

use tracing::info;
use url::Url;

use crate::{
    config,
    distributed::{
        cluster::Cluster,
        member::{Member, Service},
        sonic::{self, service::sonic_service},
    },
    generic_query,
    index::Index,
    inverted_index::{self, KeyPhrase, RetrievedWebpage},
    models::dual_encoder::DualEncoder,
    ranking::models::linear::LinearRegression,
    searcher::{InitialWebsiteResult, LocalSearcher, SearchQuery},
    Result,
};

use super::api::{Size, SizeResponse};

pub trait RetrieveReq: bincode::Encode + bincode::Decode + Clone {
    type Query: generic_query::GenericQuery + bincode::Encode + bincode::Decode;
    fn new(
        query: Self::Query,
        fruit: <<Self::Query as generic_query::GenericQuery>::Collector as generic_query::Collector>::Fruit,
    ) -> Self;
}

pub trait Query
where
    Self: generic_query::GenericQuery
        + bincode::Encode
        + bincode::Decode
        + sonic::service::Wrapper<SearchService>,
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
            impl Message<SearchService> for $q {
                type Response = Result<<<$q as generic_query::GenericQuery>::Collector as generic_query::Collector>::Fruit, EncodedError>;

                async fn handle(self, server: &SearchService) -> Self::Response {
                    server.local_searcher.search_initial_generic(self).await.map_err(|e| EncodedError { msg: e.to_string() })
                }
            }

            paste::item! {
                #[derive(bincode::Encode, bincode::Decode, Clone)]
                pub struct [<$q Retrieve>] {
                    pub query: $q,
                    #[bincode(with_serde)]
                    pub fruit: <<$q as generic_query::GenericQuery>::Collector as generic_query::Collector>::Fruit,
                }

                impl Message<SearchService> for [<$q Retrieve>] {
                    type Response = Result<<$q as generic_query::GenericQuery>::IntermediateOutput, EncodedError>;
                    async fn handle(self, server: &SearchService) -> Self::Response {
                        server
                            .local_searcher
                            .retrieve_generic(self.query, self.fruit)
                            .await
                            .map_err(|e| EncodedError { msg: e.to_string() })
                    }
                }

                impl Query for $q {
                    type RetrieveReq = [<$q Retrieve>];
                }

                impl RetrieveReq for [<$q Retrieve>] {
                    type Query = $q;
                    fn new(query: Self::Query, fruit: <<Self::Query as generic_query::GenericQuery>::Collector as generic_query::Collector>::Fruit) -> Self {
                        Self { query, fruit }
                    }
                }
            }
        )*

        paste::item! {
            sonic_service!(SearchService, [
                RetrieveWebsites,
                Search,
                GetWebpage,
                GetHomepageDescriptions,
                TopKeyPhrases,
                Size,
                GetSiteUrls,
                $(
                    $q,
                    [<$q Retrieve>],
                )*
            ]);
        }

    }
}

impl_search!([]);

pub struct SearchService {
    local_searcher: LocalSearcher<Arc<Index>>,
    // dropping the handle leaves the cluster
    #[allow(unused)]
    cluster_handle: Cluster,
}

impl SearchService {
    async fn new(config: config::SearchServerConfig) -> Result<Self> {
        let mut search_index = Index::open(config.index_path)?;
        search_index
            .inverted_index
            .set_snippet_config(config.snippet);
        search_index.inverted_index.set_shard_id(config.shard);

        let mut local_searcher = LocalSearcher::builder(Arc::new(search_index));

        if let Some(model_path) = config.linear_model_path {
            local_searcher = local_searcher.set_linear_model(LinearRegression::open(model_path)?);
        }

        if let Some(model_path) = config.dual_encoder_model_path {
            local_searcher = local_searcher.set_dual_encoder(DualEncoder::open(model_path)?);
        }

        local_searcher = local_searcher.set_collector_config(config.collector);

        let cluster_handle = Cluster::join(
            Member::new(Service::Searcher {
                host: config.host,
                shard: config.shard,
            }),
            config.gossip_addr,
            config.gossip_seed_nodes.unwrap_or_default(),
        )
        .await?;

        Ok(SearchService {
            local_searcher: local_searcher.build(),
            cluster_handle,
        })
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
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
            .await
            .ok()
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct Search {
    pub query: SearchQuery,
}
impl sonic::service::Message<SearchService> for Search {
    type Response = Option<InitialWebsiteResult>;
    async fn handle(self, server: &SearchService) -> Self::Response {
        server
            .local_searcher
            .search_initial(&self.query, true)
            .await
            .ok()
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct GetWebpage {
    pub url: String,
}
impl sonic::service::Message<SearchService> for GetWebpage {
    type Response = Option<RetrievedWebpage>;
    async fn handle(self, server: &SearchService) -> Self::Response {
        server.local_searcher.get_webpage(&self.url).await
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct GetHomepageDescriptions {
    #[bincode(with_serde)]
    pub urls: Vec<Url>,
}
impl sonic::service::Message<SearchService> for GetHomepageDescriptions {
    type Response = crate::bincode_utils::SerdeCompat<HashMap<Url, String>>;
    async fn handle(self, server: &SearchService) -> Self::Response {
        let mut result = HashMap::with_capacity(self.urls.len());

        for url in &self.urls {
            if let Some(homepage) = server.local_searcher.get_homepage(url).await {
                if let Some(desc) = homepage.description() {
                    result.insert(url.clone(), desc.clone());
                }
            }
        }

        crate::bincode_utils::SerdeCompat(result)
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct TopKeyPhrases {
    pub top_n: usize,
}
impl sonic::service::Message<SearchService> for TopKeyPhrases {
    type Response = Vec<KeyPhrase>;
    async fn handle(self, server: &SearchService) -> Self::Response {
        server.local_searcher.top_key_phrases(self.top_n).await
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

impl sonic::service::Message<SearchService> for Size {
    type Response = SizeResponse;
    async fn handle(self, server: &SearchService) -> Self::Response {
        SizeResponse {
            pages: server.local_searcher.num_documents().await,
        }
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct GetSiteUrls {
    pub site: String,
    pub offset: u64,
    pub limit: u64,
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct SiteUrls {
    #[bincode(with_serde)]
    pub urls: Vec<Url>,
}

impl sonic::service::Message<SearchService> for GetSiteUrls {
    type Response = SiteUrls;
    async fn handle(self, server: &SearchService) -> Self::Response {
        let urls = server
            .local_searcher
            .get_site_urls(&self.site, self.offset as usize, self.limit as usize)
            .await;

        SiteUrls { urls }
    }
}
