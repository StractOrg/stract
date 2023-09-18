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

pub mod defaults;

use super::Result;
use crate::searcher::ShardId;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{self, BufRead};
use std::net::SocketAddr;

#[derive(Debug, Deserialize, Clone)]
pub struct IndexingLocalConfig {
    pub limit_warc_files: Option<usize>,
    pub skip_warc_files: Option<usize>,
    pub warc_source: WarcSource,
    pub batch_size: Option<usize>,
    pub page_webgraph_path: Option<String>,
    pub output_path: Option<String>,
    pub host_centrality_threshold: Option<f64>,
    pub topics_path: Option<String>,
    pub host_centrality_store_path: String,
    pub page_centrality_store_path: Option<String>,
    pub safety_classifier_path: Option<String>,
    pub minimum_clean_words: Option<usize>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct WebgraphConstructConfig {
    pub host_graph_base_path: String,
    pub page_graph_base_path: String,
    pub warc_source: WarcSource,
    pub redirect_db_path: Option<String>,
    pub limit_warc_files: Option<usize>,
    pub batch_size: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum WarcSource {
    HTTP(HttpConfig),
    Local(LocalConfig),
    S3(S3Config),
}

impl WarcSource {
    pub fn paths(&self) -> Result<Vec<String>> {
        let mut warc_paths = Vec::new();
        match &self {
            WarcSource::HTTP(config) => {
                let file = File::open(&config.warc_paths_file)?;
                for line in io::BufReader::new(file).lines() {
                    warc_paths.push(line?);
                }
            }
            WarcSource::Local(config) => {
                warc_paths = config.names.clone();
            }
            WarcSource::S3(config) => {
                let bucket = s3::Bucket::new(
                    &config.bucket,
                    s3::Region::Custom {
                        region: "".to_string(),
                        endpoint: config.endpoint.clone(),
                    },
                    s3::creds::Credentials {
                        access_key: Some(config.access_key.clone()),
                        secret_key: Some(config.secret_key.clone()),
                        security_token: None,
                        session_token: None,
                        expiration: None,
                    },
                )?
                .with_path_style();

                let mut folder = config.folder.clone();

                if !folder.ends_with('/') {
                    folder.push('/');
                }

                let pages = bucket.list_blocking(folder, Some("/".to_string()))?;

                let objects = pages
                    .into_iter()
                    .flat_map(|p| p.contents.into_iter())
                    .collect::<Vec<_>>();

                for p in objects.into_iter().filter_map(|o| {
                    if o.key.ends_with("warc.gz") {
                        Some(o.key)
                    } else {
                        None
                    }
                }) {
                    warc_paths.push(p);
                }
            }
        }

        Ok(warc_paths)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LocalConfig {
    pub folder: String,
    pub names: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HttpConfig {
    pub base_url: String,
    pub warc_paths_file: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub folder: String,
    pub access_key: String,
    pub secret_key: String,
    pub endpoint: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CollectorConfig {
    #[serde(default = "defaults::Collector::site_penalty")]
    pub site_penalty: f64,

    #[serde(default = "defaults::Collector::title_penalty")]
    pub title_penalty: f64,

    #[serde(default = "defaults::Collector::url_penalty")]
    pub url_penalty: f64,

    #[serde(default = "defaults::Collector::max_docs_considered")]
    pub max_docs_considered: usize,
}

impl Default for CollectorConfig {
    fn default() -> Self {
        Self {
            site_penalty: defaults::Collector::site_penalty(),
            title_penalty: defaults::Collector::title_penalty(),
            url_penalty: defaults::Collector::url_penalty(),
            max_docs_considered: defaults::Collector::max_docs_considered(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ApiThresholds {
    #[serde(default = "defaults::Api::stackoverflow")]
    pub stackoverflow: f64,

    #[serde(default = "defaults::Api::entity_sidebar")]
    pub entity_sidebar: f64,

    #[serde(default = "defaults::Api::discussions_widget")]
    pub discussions_widget: f64,
}

impl Default for ApiThresholds {
    fn default() -> Self {
        Self {
            stackoverflow: defaults::Api::stackoverflow(),
            entity_sidebar: defaults::Api::entity_sidebar(),
            discussions_widget: defaults::Api::discussions_widget(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ApiConfig {
    pub queries_csv_path: String,
    pub host: SocketAddr,
    pub prometheus_host: SocketAddr,
    pub crossencoder_model_path: Option<String>,
    pub lambda_model_path: Option<String>,
    pub qa_model_path: Option<String>,
    pub bangs_path: String,
    pub summarizer_path: String,
    pub fact_check_model_path: String,
    pub query_store_db_host: Option<String>,
    pub cluster_id: String,
    pub gossip_seed_nodes: Option<Vec<SocketAddr>>,
    pub gossip_addr: SocketAddr,

    #[serde(default)]
    pub collector: CollectorConfig,

    #[serde(default)]
    pub thresholds: ApiThresholds,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SnippetConfig {
    #[serde(default = "defaults::Snippet::desired_num_chars")]
    pub desired_num_chars: usize,

    #[serde(default = "defaults::Snippet::delta_num_chars")]
    pub delta_num_chars: usize,

    #[serde(default = "defaults::Snippet::min_passage_width")]
    pub min_passage_width: usize,

    pub max_considered_words: Option<usize>,
    pub num_words_for_lang_detection: Option<usize>,
}

impl Default for SnippetConfig {
    fn default() -> Self {
        Self {
            desired_num_chars: defaults::Snippet::desired_num_chars(),
            delta_num_chars: defaults::Snippet::delta_num_chars(),
            min_passage_width: defaults::Snippet::min_passage_width(),
            max_considered_words: None,
            num_words_for_lang_detection: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SearchServerConfig {
    pub cluster_id: String,
    pub gossip_seed_nodes: Option<Vec<SocketAddr>>,
    pub gossip_addr: SocketAddr,
    pub shard_id: ShardId,
    pub index_path: String,
    pub entity_index_path: Option<String>,
    pub host_centrality_store_path: Option<String>,
    pub linear_model_path: Option<String>,
    pub lambda_model_path: Option<String>,
    pub host: SocketAddr,

    #[serde(default)]
    pub collector: CollectorConfig,

    #[serde(default)]
    pub snippet: SnippetConfig,

    #[serde(default = "defaults::SearchServer::build_spell_dictionary")]
    pub build_spell_dictionary: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CrawlCoordinatorConfig {
    pub crawldb_folder: String,
    pub host: SocketAddr,
    pub num_urls_to_crawl: u64,
    pub seed_urls: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UserAgent {
    pub full: String,
    pub token: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CrawlerConfig {
    pub num_worker_threads: usize,
    pub user_agent: UserAgent,
    pub num_jobs_per_fetch: usize,

    #[serde(default = "defaults::Crawler::robots_txt_cache_sec")]
    pub robots_txt_cache_sec: u64,

    #[serde(default = "defaults::Crawler::politeness_factor")]
    pub politeness_factor: f32,

    #[serde(default = "defaults::Crawler::min_crawl_delay_ms")]
    pub min_crawl_delay_ms: u64,

    #[serde(default = "defaults::Crawler::max_crawl_delay_ms")]
    pub max_crawl_delay_ms: u64,

    #[serde(default = "defaults::Crawler::max_politeness_factor")]
    pub max_politeness_factor: f32,

    #[serde(default = "defaults::Crawler::min_politeness_factor")]
    pub min_politeness_factor: f32,

    #[serde(default = "defaults::Crawler::max_url_slowdown_retry")]
    pub max_url_slowdown_retry: u8,

    pub timeout_seconds: u64,
    pub s3: S3Config,
    pub coordinator_host: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", content = "args", rename_all = "snake_case")]
pub enum AcceleratorDevice {
    Cpu,
    Cuda(usize),
    Mps,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AcceleratorDtype {
    Float,
    Bf16,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AliceAcceleratorConfig {
    pub layer_fraction: f64,
    /// percentage of layers on accelerator to quantize
    pub quantize_fraction: f64,
    pub device: AcceleratorDevice,
    pub dtype: AcceleratorDtype,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AliceLocalConfig {
    pub cluster_id: String,
    pub gossip_seed_nodes: Option<Vec<SocketAddr>>,
    pub gossip_addr: SocketAddr,
    pub host: SocketAddr,

    pub alice_path: String,
    pub accelerator: Option<AliceAcceleratorConfig>,
    /// base64 encoded
    pub encryption_key: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WebgraphServerConfig {
    pub host: SocketAddr,
    pub host_graph_path: String,
    pub page_graph_path: String,
    pub inbound_similarity_path: String,
    pub cluster_id: String,
    pub gossip_seed_nodes: Option<Vec<SocketAddr>>,
    pub gossip_addr: SocketAddr,

    #[serde(default = "defaults::WebgraphServer::max_similar_sites")]
    pub max_similar_sites: usize,
}
