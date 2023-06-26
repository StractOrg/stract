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

use crate::{webpage::Url, CrawlerConfig};

use self::{
    robots_txt::RobotsTxtManager,
    warc_writer::{WarcWriter, WarcWriterMessage},
    worker::Worker,
};

mod coordinator;
mod crawl_db;
mod robots_txt;
mod warc_writer;
mod worker;

pub use coordinator::CrawlCoordinator;

const DEFAULT_POLITENESS_FACTOR: f32 = 1.0;

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

    #[error("send error")]
    SendError(#[from] tokio::sync::mpsc::error::SendError<WarcWriterMessage>),

    #[error("sqlite error")]
    Sqlite(#[from] rusqlite::Error),

    #[error("addr parse error: {0}")]
    AddrParse(#[from] std::net::AddrParseError),
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Site(String);

#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Domain(String);

/// All urls in a job must be from the same domain and only one job per domain.
/// at a time. This ensures that we stay polite when crawling.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct Job {
    pub domain: Domain,
    pub fetch_sitemap: bool,
    pub urls: VecDeque<Url>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum UrlResponse {
    Success { url: Url },
    Failed { url: Url, status_code: Option<u16> },
    Redirected { url: Url, new_url: Url },
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct JobResponse {
    domain: Domain,
    url_responses: Vec<UrlResponse>,
    discovered_urls: Vec<Url>,
}

struct WorkerJob {
    robotstxt: RobotsTxtManager,
    job: Job,
}

impl From<Job> for WorkerJob {
    fn from(value: Job) -> Self {
        Self {
            robotstxt: RobotsTxtManager::new(),
            job: value,
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
        let writer = Arc::new(WarcWriter::new(config.s3));
        let timeout = Duration::from_secs(config.timeout_seconds);
        let mut handles = Vec::new();
        let coordinator_host = config.coordinator_host.parse()?;

        for _ in 0..config.num_workers {
            let worker = Worker::new(
                Arc::clone(&pending_commands),
                Arc::clone(&writer),
                config.user_agent.clone(),
                config
                    .politeness_factor
                    .unwrap_or(DEFAULT_POLITENESS_FACTOR),
                timeout,
                coordinator_host,
                config.num_workers,
            )?;

            handles.push(tokio::spawn(async move {
                worker.run().await;
            }));
        }

        Ok(Self { writer, handles })
    }

    pub async fn wait(self) {
        for handle in self.handles {
            handle.await.unwrap();
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
