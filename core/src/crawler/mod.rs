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

use std::{collections::VecDeque, sync::Arc, time::Duration};

use hashbrown::HashMap;

use url::Url;

use crate::{config::CrawlerConfig, warc, webpage::url_ext::UrlExt};

use self::{warc_writer::WarcWriter, worker::WorkerThread};
pub use worker::JobExecutor;

pub mod coordinator;
mod robots_txt;
pub mod router;
pub use router::Router;
mod file_queue;
pub mod planner;
mod wander_prirotiser;
mod warc_writer;
mod worker;

pub use coordinator::CrawlCoordinator;

pub const MAX_URL_LEN_BYTES: usize = 8192;
pub const MAX_URLS_FOR_DOMAIN_PER_INSERT: usize = 256;
/// Number of new domains that can be discovered for each domain crawled.
pub const MAX_DOMAIN_DISCOVERY_FACTOR: usize = 4;

pub const MAX_OUTGOING_URLS_PER_PAGE: usize = 200;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid content type: {0}")]
    InvalidContentType(String),

    #[error("fetch failed: {0}")]
    FetchFailed(reqwest::StatusCode),

    #[error("content too large")]
    ContentTooLarge,

    #[error("invalid politeness factor")]
    InvalidPolitenessFactor,

    #[error("invalid redirect")]
    InvalidRedirect,
}

type Result<T, E = anyhow::Error> = std::result::Result<T, E>;

#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Serialize,
    rkyv::Deserialize,
    rkyv::Archive,
)]
struct Site(String);

#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Serialize,
    rkyv::Deserialize,
    rkyv::Archive,
)]
#[archive(check_bytes)]
pub struct Domain(String);

impl From<&Url> for Domain {
    fn from(url: &Url) -> Self {
        Self(url.icann_domain().unwrap_or_default().to_string())
    }
}

impl From<Url> for Domain {
    fn from(url: Url) -> Self {
        Self::from(&url)
    }
}

impl From<String> for Domain {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl Domain {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct WeightedUrl {
    pub url: Url,
    pub weight: f64,
}

/// All urls in a job must be from the same domain and only one job per domain.
/// at a time. This ensures that we stay polite when crawling.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct Job {
    pub domain: Domain,
    pub urls: VecDeque<WeightedUrl>,
    pub wandering_urls: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum UrlResponse {
    Success { url: Url },
    Failed { url: Url, status_code: Option<u16> },
    Redirected { url: Url, new_url: Url },
}

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct UrlString(String);

impl From<&Url> for UrlString {
    fn from(url: &Url) -> Self {
        Self(url.as_str().to_string())
    }
}

impl From<Url> for UrlString {
    fn from(url: Url) -> Self {
        Self(url.as_str().to_string())
    }
}

impl TryFrom<&UrlString> for Url {
    type Error = anyhow::Error;
    fn try_from(url: &UrlString) -> Result<Self, Self::Error> {
        Ok(Url::parse(&url.0)?)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UrlToInsert {
    pub url: UrlString,
    pub weight: f64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DiscoveredUrls {
    pub urls: HashMap<Domain, Vec<UrlToInsert>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DomainCrawled {
    pub domain: Domain,
    pub budget_used: f64,
}

pub struct RetrieableUrl {
    weighted_url: WeightedUrl,
    retries: u8,
}

impl RetrieableUrl {
    pub fn url(&self) -> &Url {
        &self.weighted_url.url
    }
}

impl From<WeightedUrl> for RetrieableUrl {
    fn from(weighted_url: WeightedUrl) -> Self {
        Self {
            weighted_url,
            retries: 0,
        }
    }
}

pub struct WorkerJob {
    pub domain: Domain,
    pub urls: VecDeque<RetrieableUrl>,
    pub wandering_urls: u64,
}

impl From<Job> for WorkerJob {
    fn from(value: Job) -> Self {
        Self {
            domain: value.domain,
            urls: value.urls.into_iter().map(RetrieableUrl::from).collect(),
            wandering_urls: value.wandering_urls,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CrawlDatum {
    pub url: Url,
    pub status_code: u16,
    pub payload_type: warc::PayloadType,
    pub body: String,
    pub fetch_time_ms: u64,
}

pub struct Crawler {
    writer: Arc<WarcWriter>,
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl Crawler {
    pub async fn new(config: CrawlerConfig) -> Result<Self> {
        let writer = Arc::new(WarcWriter::new(config.s3.clone()));
        let mut handles = Vec::new();
        let mut router_hosts = Vec::new();

        for host in &config.router_hosts {
            router_hosts.push(host.parse()?);
        }

        for _ in 0..config.num_worker_threads {
            let worker =
                WorkerThread::new(Arc::clone(&writer), config.clone(), router_hosts.clone())?;

            handles.push(tokio::spawn(async move {
                worker.run().await;
            }));
        }

        Ok(Self { writer, handles })
    }

    pub async fn run(self) {
        for handle in self.handles {
            handle.await.ok();
        }

        self.writer.finish().await.unwrap();
    }
}

#[async_trait::async_trait]
pub trait DatumStream: Send + Sync {
    async fn write(&self, crawl_datum: CrawlDatum) -> Result<()>;
    async fn finish(&self) -> Result<()>;
}

pub fn reqwest_client(config: &CrawlerConfig) -> Result<reqwest::Client> {
    let timeout = Duration::from_secs(config.timeout_seconds);

    let mut headers = reqwest::header::HeaderMap::default();
    headers.insert(
        reqwest::header::ACCEPT,
        reqwest::header::HeaderValue::from_static("text/html"),
    );
    headers.insert(
        reqwest::header::ACCEPT_LANGUAGE,
        reqwest::header::HeaderValue::from_static("en-US,en;q=0.9,*;q=0.8"),
    );

    Ok(reqwest::Client::builder()
        .timeout(timeout)
        .connect_timeout(timeout)
        .http2_keep_alive_interval(None)
        .default_headers(headers)
        .redirect(reqwest::redirect::Policy::limited(config.max_redirects))
        .user_agent(&config.user_agent.full)
        .build()?)
}
