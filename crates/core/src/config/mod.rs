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

pub mod defaults;

use super::Result;
use crate::ampc::dht;
use crate::distributed::member::ShardId;
use crate::feed::scheduler::SplitId;

use std::fs::File;
use std::io::{self, BufRead};
use std::net::SocketAddr;

#[derive(Debug, serde::Deserialize, Clone)]
pub struct IndexingLocalConfig {
    pub output_path: String,
    pub limit_warc_files: Option<usize>,
    pub skip_warc_files: Option<usize>,
    pub warc_source: WarcSource,
    pub page_webgraph: Option<IndexingGraphConfig>,
    pub host_centrality_threshold: Option<f64>,
    pub topics_path: Option<String>,
    pub host_centrality_store_path: String,
    pub page_centrality_store_path: Option<String>,
    pub safety_classifier_path: Option<String>,
    pub minimum_clean_words: Option<usize>,

    #[serde(default = "defaults::Indexing::batch_size")]
    pub batch_size: usize,

    #[serde(default = "defaults::Indexing::autocommit_after_num_inserts")]
    pub autocommit_after_num_inserts: usize,

    pub dual_encoder: Option<IndexingDualEncoderConfig>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(tag = "type")]
pub enum IndexingGraphConfig {
    Local { path: String },
    Remote { gossip: GossipConfig },
}

#[derive(Debug, serde::Deserialize, Clone)]
pub struct IndexingDualEncoderConfig {
    pub model_path: String,

    /// Only compute embeddings for pages that has a
    /// centrality rank less than this threshold
    pub page_centrality_rank_threshold: Option<u64>,
}

#[derive(Debug, serde::Deserialize, Clone)]
pub struct WebgraphConstructConfig {
    pub host_graph_base_path: String,
    pub page_graph_base_path: String,
    pub warc_source: WarcSource,
    pub limit_warc_files: Option<usize>,
    pub skip_warc_files: Option<usize>,
    pub batch_size: Option<usize>,
    pub canonical_index_path: Option<String>,
    pub host_centrality_rank_store_path: Option<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Clone)]
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
                warc_paths.clone_from(&config.names);
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

#[derive(Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Clone)]
pub struct LocalConfig {
    pub folder: String,
    pub names: Vec<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Clone)]
pub struct HttpConfig {
    pub base_url: String,
    pub warc_paths_file: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub folder: String,
    pub access_key: String,
    pub secret_key: String,
    pub endpoint: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct CollectorConfig {
    #[serde(default = "defaults::Collector::site_penalty")]
    pub site_penalty: f64,

    #[serde(default = "defaults::Collector::title_penalty")]
    pub title_penalty: f64,

    #[serde(default = "defaults::Collector::url_penalty")]
    pub url_penalty: f64,

    #[serde(default = "defaults::Collector::url_without_tld_penalty")]
    pub url_without_tld_penalty: f64,

    #[serde(default = "defaults::Collector::max_docs_considered")]
    pub max_docs_considered: usize,
}

impl Default for CollectorConfig {
    fn default() -> Self {
        Self {
            site_penalty: defaults::Collector::site_penalty(),
            title_penalty: defaults::Collector::title_penalty(),
            url_penalty: defaults::Collector::url_penalty(),
            url_without_tld_penalty: defaults::Collector::url_without_tld_penalty(),
            max_docs_considered: defaults::Collector::max_docs_considered(),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct ApiThresholds {
    #[serde(default = "defaults::Api::stackoverflow")]
    pub stackoverflow: f64,

    #[serde(default = "defaults::Api::entity_sidebar")]
    pub entity_sidebar: f64,
}

impl Default for ApiThresholds {
    fn default() -> Self {
        Self {
            stackoverflow: defaults::Api::stackoverflow(),
            entity_sidebar: defaults::Api::entity_sidebar(),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct ApiSpellCheck {
    pub path: String,

    #[serde(default)]
    pub correction_config: CorrectionConfig,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct ApiConfig {
    pub queries_csv_path: String,
    pub host: SocketAddr,
    pub prometheus_host: SocketAddr,
    pub crossencoder_model_path: Option<String>,
    pub lambda_model_path: Option<String>,
    pub dual_encoder_model_path: Option<String>,
    pub bangs_path: String,
    pub query_store_db_host: Option<String>,
    pub cluster_id: String,
    pub gossip_seed_nodes: Option<Vec<SocketAddr>>,
    pub gossip_addr: SocketAddr,

    #[serde(default = "defaults::Api::max_similar_hosts")]
    pub max_similar_hosts: usize,

    pub spell_check: Option<ApiSpellCheck>,

    #[serde(default)]
    pub thresholds: ApiThresholds,

    pub widgets: WidgetsConfig,

    #[serde(default)]
    pub collector: CollectorConfig,

    #[serde(default = "defaults::Api::max_concurrent_searches")]
    pub max_concurrent_searches: Option<usize>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct SnippetConfig {
    #[serde(default = "defaults::Snippet::desired_num_chars")]
    pub desired_num_chars: usize,

    #[serde(default = "defaults::Snippet::delta_num_chars")]
    pub delta_num_chars: usize,

    #[serde(default = "defaults::Snippet::min_passage_width")]
    pub min_passage_width: usize,

    pub max_considered_words: Option<usize>,
    pub num_words_for_lang_detection: Option<usize>,

    #[serde(default = "defaults::Snippet::empty_query_snippet_words")]
    pub empty_query_snippet_words: usize,
    #[serde(default = "defaults::Snippet::min_description_words")]
    pub min_description_words: usize,
    #[serde(default = "defaults::Snippet::min_body_length")]
    pub min_body_length: usize,
    #[serde(default = "defaults::Snippet::min_body_length_homepage")]
    pub min_body_length_homepage: usize,
}

impl Default for SnippetConfig {
    fn default() -> Self {
        Self {
            desired_num_chars: defaults::Snippet::desired_num_chars(),
            delta_num_chars: defaults::Snippet::delta_num_chars(),
            min_passage_width: defaults::Snippet::min_passage_width(),
            max_considered_words: None,
            num_words_for_lang_detection: None,
            empty_query_snippet_words: defaults::Snippet::empty_query_snippet_words(),
            min_description_words: defaults::Snippet::min_description_words(),
            min_body_length: defaults::Snippet::min_body_length(),
            min_body_length_homepage: defaults::Snippet::min_body_length_homepage(),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct SearchServerConfig {
    pub cluster_id: String,
    pub gossip_seed_nodes: Option<Vec<SocketAddr>>,
    pub gossip_addr: SocketAddr,
    pub shard: ShardId,
    pub index_path: String,
    pub linear_model_path: Option<String>,
    pub lambda_model_path: Option<String>,
    pub dual_encoder_model_path: Option<String>,
    pub host: SocketAddr,

    #[serde(default)]
    pub collector: CollectorConfig,

    #[serde(default)]
    pub snippet: SnippetConfig,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct EntitySearchServerConfig {
    pub cluster_id: String,
    pub gossip_seed_nodes: Option<Vec<SocketAddr>>,
    pub gossip_addr: SocketAddr,
    pub index_path: String,
    pub host: SocketAddr,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct CrawlCoordinatorConfig {
    pub job_queue: String,
    pub host: SocketAddr,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct UserAgent {
    pub full: String,
    pub token: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct CrawlerConfig {
    pub num_worker_threads: usize,
    pub user_agent: UserAgent,

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

    #[serde(default = "defaults::Crawler::max_url_slowdown_retry")]
    pub max_url_slowdown_retry: u8,

    #[serde(default = "defaults::Crawler::max_redirects")]
    pub max_redirects: usize,

    #[serde(default = "defaults::Crawler::dry_run")]
    pub dry_run: bool,

    pub timeout_seconds: u64,
    pub s3: S3Config,
    pub router_hosts: Vec<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct CrawlRouterConfig {
    pub host: SocketAddr,
    pub coordinator_addrs: Vec<SocketAddr>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
#[serde(tag = "type", content = "args", rename_all = "snake_case")]
pub enum AcceleratorDevice {
    Cpu,
    Cuda(usize),
    Mps,
}

#[derive(
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    PartialEq,
    Eq,
    Hash,
    Clone,
    Copy,
    Debug,
)]
#[serde(rename_all = "snake_case")]
pub enum WebgraphGranularity {
    Host,
    Page,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct WebgraphServerConfig {
    pub host: SocketAddr,
    pub shard: ShardId,
    pub graph_path: String,
    pub granularity: WebgraphGranularity,

    pub cluster_id: String,
    pub gossip_seed_nodes: Option<Vec<SocketAddr>>,
    pub gossip_addr: SocketAddr,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct WidgetsConfig {
    pub thesaurus_paths: Vec<String>,

    #[serde(default = "defaults::Widgets::calculator_fetch_currencies_exchange")]
    pub calculator_fetch_currencies_exchange: bool,
}

impl Default for WidgetsConfig {
    fn default() -> Self {
        Self {
            thesaurus_paths: Vec::new(),
            calculator_fetch_currencies_exchange:
                defaults::Widgets::calculator_fetch_currencies_exchange(),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct CrawlPlannerConfig {
    pub host_harmonic_path: String,
    pub page_harmonic_path: String,
    pub output_path: String,

    pub num_job_queues: usize,

    pub crawl_budget: usize,
    pub wander_fraction: f64,

    pub gossip: GossipConfig,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct LiveIndexConfig {
    pub split_path: String,
    pub downloaded_db_path: String,

    // crawler
    pub user_agent: UserAgent,
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
    #[serde(default = "defaults::Crawler::max_url_slowdown_retry")]
    pub max_url_slowdown_retry: u8,
    #[serde(default = "defaults::Crawler::max_redirects")]
    pub max_redirects: usize,
    pub timeout_seconds: u64,

    // indexer
    pub host_centrality_store_path: String,
    pub page_webgraph: Option<IndexingGraphConfig>,
    pub page_centrality_store_path: Option<String>,
    pub safety_classifier_path: Option<String>,
    pub host_centrality_threshold: Option<f64>,
    pub minimum_clean_words: Option<usize>,

    // search
    pub cluster_id: String,
    pub gossip_seed_nodes: Option<Vec<SocketAddr>>,
    pub gossip_addr: SocketAddr,
    pub split_id: SplitId,
    pub index_path: String,
    pub linear_model_path: Option<String>,
    pub lambda_model_path: Option<String>,
    pub host: SocketAddr,
    #[serde(default)]
    pub collector: CollectorConfig,
    #[serde(default)]
    pub snippet: SnippetConfig,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct LiveIndexSchedulerConfig {
    pub schedule_path: String,
    pub feed_index_path: String,
    pub host_centrality_store_path: String,
    pub host_graph_path: String,
    pub num_splits: u64,
}
#[derive(Debug, serde::Deserialize, Clone)]
pub struct FeedIndexingConfig {
    pub output_path: String,
    pub warc_source: WarcSource,
    pub limit_warc_files: Option<usize>,
    pub skip_warc_files: Option<usize>,
}

#[derive(Debug, serde::Deserialize, Clone)]
pub struct WebSpellConfig {
    pub output_path: String,
    pub warc_source: WarcSource,
    pub languages: Vec<whatlang::Lang>,
    pub limit_warc_files: Option<usize>,
    pub skip_warc_files: Option<usize>,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
pub struct CorrectionConfig {
    /// The probability that a word is misspelled
    #[serde(default = "defaults::Correction::misspelled_prob")]
    pub misspelled_prob: f64,

    /// Lambda in eq. 2 (http://static.googleusercontent.com/media/research.google.com/en/us/pubs/archive/36180.pdf)
    #[serde(default = "defaults::Correction::lm_prob_weight")]
    pub lm_prob_weight: f64,

    /// The threshold that the difference between the log probability of the best
    /// correction and the observed word must be above for the word to be
    /// corrected
    #[serde(default = "defaults::Correction::correction_threshold")]
    pub correction_threshold: f64,
}

impl Default for CorrectionConfig {
    fn default() -> Self {
        Self {
            misspelled_prob: defaults::Correction::misspelled_prob(),
            lm_prob_weight: defaults::Correction::lm_prob_weight(),
            correction_threshold: defaults::Correction::correction_threshold(),
        }
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct GossipConfig {
    pub cluster_id: String,
    pub seed_nodes: Option<Vec<SocketAddr>>,
    pub addr: SocketAddr,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct DhtConfig {
    pub node_id: dht::NodeId,
    pub host: SocketAddr,
    pub shard: ShardId,
    pub seed_node: Option<SocketAddr>,
    pub gossip: GossipConfig,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct HarmonicCoordinatorConfig {
    pub gossip: GossipConfig,
    pub host: SocketAddr,
    pub output_path: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct HarmonicWorkerConfig {
    pub gossip: GossipConfig,
    pub shard: ShardId,
    pub graph_path: String,
    pub host: SocketAddr,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ApproxHarmonicCoordinatorConfig {
    pub gossip: GossipConfig,
    pub host: SocketAddr,
    pub output_path: String,

    #[serde(default = "defaults::ApproxHarmonic::sample_rate")]
    pub sample_rate: f64,

    #[serde(default = "defaults::ApproxHarmonic::max_distance")]
    pub max_distance: u8,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ApproxHarmonicWorkerConfig {
    pub gossip: GossipConfig,
    pub shard: ShardId,
    pub graph_path: String,
    pub host: SocketAddr,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct CanonicalIndexConfig {
    pub output_path: String,
    pub warc_source: WarcSource,
    pub limit_warc_files: Option<usize>,
    pub skip_warc_files: Option<usize>,
}
