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

use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use tokio::sync::Mutex;

use crate::{config::CrawlerConfig, webpage::Url};

use self::{
    warc_writer::{WarcWriter, WarcWriterMessage},
    worker::Worker,
};

pub mod coordinator;
pub mod crawl_db;
mod robots_txt;
mod warc_writer;
mod worker;

pub use coordinator::CrawlCoordinator;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("chrono out-of-range error: {0}")]
    Chrono(#[from] chrono::OutOfRangeError),

    #[error("invalid content type: {0}")]
    InvalidContentType(String),

    #[error("fetch failed: {0}")]
    FetchFailed(reqwest::StatusCode),

    #[error("content too large")]
    ContentTooLarge,

    #[error("invalid politeness factor")]
    InvalidPolitenessFactor,

    #[error("addr parse error: {0}")]
    AddrParse(#[from] std::net::AddrParseError),

    #[error("channel: {0}")]
    SendError(#[from] tokio::sync::mpsc::error::SendError<WarcWriterMessage>),

    #[error("bincode: {0}")]
    Bincode(#[from] bincode::Error),

    #[error("rocksdb: {0}")]
    Rocksdb(#[from] rocksdb::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Site(String);

#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Domain(String);

impl From<&Url> for Domain {
    fn from(url: &Url) -> Self {
        Self(url.domain().to_string())
    }
}

impl From<String> for Domain {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// All urls in a job must be from the same domain and only one job per domain.
/// at a time. This ensures that we stay polite when crawling.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct Job {
    pub domain: Domain,
    pub fetch_sitemap: bool,
    pub urls: VecDeque<Url>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum UrlResponse {
    Success { url: Url },
    Failed { url: Url, status_code: Option<u16> },
    Redirected { url: Url, new_url: Url },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct JobResponse {
    pub domain: Domain,
    pub url_responses: Vec<UrlResponse>,
    pub discovered_urls: Vec<Url>,
}

struct RetrieableUrl {
    url: Url,
    retries: u8,
}

impl From<Url> for RetrieableUrl {
    fn from(url: Url) -> Self {
        Self { url, retries: 0 }
    }
}

struct WorkerJob {
    pub domain: Domain,
    pub fetch_sitemap: bool,
    pub urls: VecDeque<RetrieableUrl>,
}

impl From<Job> for WorkerJob {
    fn from(value: Job) -> Self {
        Self {
            domain: value.domain,
            fetch_sitemap: value.fetch_sitemap,
            urls: value.urls.into_iter().map(RetrieableUrl::from).collect(),
        }
    }
}

pub enum Command {
    Job(Job),
    Shutdown,
}

#[derive(Debug, Clone)]
pub struct CrawlDatum {
    url: Url,
    status_code: u16,
    headers: HashMap<String, String>,
    body: String,
    fetch_time_ms: u64,
}

pub struct Crawler {
    writer: Arc<WarcWriter>,
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl Crawler {
    pub async fn new(config: CrawlerConfig) -> Result<Self> {
        let pending_commands = Arc::new(Mutex::new(VecDeque::new()));
        let writer = Arc::new(WarcWriter::new(config.s3.clone()));
        let timeout = Duration::from_secs(config.timeout_seconds);
        let mut handles = Vec::new();
        let coordinator_host = config.coordinator_host.parse()?;

        for _ in 0..config.num_workers {
            let worker = Worker::new(
                Arc::clone(&pending_commands),
                Arc::clone(&writer),
                config.clone(),
                timeout,
                coordinator_host,
            )?;

            handles.push(tokio::spawn(async move {
                worker.run().await;
            }));
        }

        Ok(Self { writer, handles })
    }

    pub async fn wait(self) {
        for handle in self.handles {
            handle.await.ok();
        }

        self.writer.finish().await.unwrap();
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum Request {
    NewJobs { num_jobs: usize },
    CrawlResult { job_response: JobResponse },
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum Response {
    NewJobs { jobs: Vec<Job> },
    Done,
    Ok,
}
