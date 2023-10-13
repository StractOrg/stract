use encoding_rs::{Encoding, UTF_8};
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
use futures::{future::BoxFuture, FutureExt};
use mime::Mime;
use quick_xml::events::Event;
use rand::seq::SliceRandom;
use tokio_stream::StreamExt;

use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::Mutex;
use url::Url;

use crate::{
    config::CrawlerConfig,
    crawler::{JobResponse, Response, MAX_OUTGOING_URLS_PER_PAGE, MAX_URL_LEN_BYTES},
    distributed::{retry_strategy::ExponentialBackoff, sonic},
    entrypoint::crawler::router::{NewJobs, RouterService},
    webpage::Html,
};

use super::{
    robots_txt::RobotsTxtManager, Command, CrawlDatum, Error, Result, Site, UrlResponse,
    WarcWriter, WorkerJob,
};

const MAX_CONTENT_LENGTH: usize = 32 * 1024 * 1024; // 32 MB

pub struct WorkerThread {
    current_job: Option<WorkerJob>,
    pending_commands: Arc<Mutex<VecDeque<Command>>>,
    writer: Arc<WarcWriter>,
    results: Arc<Mutex<Vec<JobResponse>>>,
    client: reqwest::Client,
    config: CrawlerConfig,
    politeness_factor: f32,
    router_hosts: Vec<SocketAddr>,
    num_jobs_per_fetch: usize,
    robotstxt: RobotsTxtManager,
}

impl WorkerThread {
    pub fn new(
        pending_commands: Arc<Mutex<VecDeque<Command>>>,
        writer: Arc<WarcWriter>,
        results: Arc<Mutex<Vec<JobResponse>>>,
        config: CrawlerConfig,
        timeout: Duration,
        router_hosts: Vec<SocketAddr>,
    ) -> Result<Self> {
        if config.politeness_factor < config.min_politeness_factor {
            return Err(Error::InvalidPolitenessFactor.into());
        }

        let mut headers = reqwest::header::HeaderMap::default();
        headers.insert(
            reqwest::header::ACCEPT,
            reqwest::header::HeaderValue::from_static("text/html"),
        );
        headers.insert(
            reqwest::header::ACCEPT_LANGUAGE,
            reqwest::header::HeaderValue::from_static("en-US,en;q=0.9,*;q=0.8"),
        );

        let client = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout)
            .http2_keep_alive_interval(None)
            .default_headers(headers)
            .user_agent(&config.user_agent.full)
            .build()?;

        let robotstxt = RobotsTxtManager::new(
            client.clone(),
            Duration::from_secs(config.robots_txt_cache_sec),
        );

        Ok(Self {
            writer,
            client,
            results,
            current_job: None,
            pending_commands,
            num_jobs_per_fetch: config.num_jobs_per_fetch,
            politeness_factor: config.politeness_factor,
            config,
            router_hosts,
            robotstxt,
        })
    }

    async fn router_conn(&self) -> Result<sonic::service::ResilientConnection<RouterService>> {
        let retry = ExponentialBackoff::from_millis(1_000).with_limit(Duration::from_secs(10));

        let router = *self.router_hosts.choose(&mut rand::thread_rng()).unwrap();

        Ok(sonic::service::ResilientConnection::create_with_timeout(
            router,
            Duration::from_secs(90),
            retry,
        )
        .await?)
    }

    pub async fn run(mut self) {
        loop {
            let mut guard = self.pending_commands.lock().await;
            let command = guard.pop_front();

            if let Some(command) = command {
                drop(guard); // let other workers get jobs

                match command {
                    Command::Job(job) => {
                        self.current_job = Some(job.into());
                        self.process_job().await;
                    }
                    Command::Shutdown => {
                        self.pending_commands
                            .lock()
                            .await
                            .push_back(Command::Shutdown);

                        break;
                    }
                }
            } else {
                let conn = self.router_conn().await.unwrap();
                let results = self.results.lock().await.drain(..).collect::<Vec<_>>();

                let res = conn
                    .send_with_timeout(
                        &NewJobs {
                            responses: results,
                            num_jobs: self.num_jobs_per_fetch,
                        },
                        Duration::from_secs(90),
                    )
                    .await;

                match res {
                    Ok(Response::NewJobs { jobs }) => {
                        if jobs.is_empty() {
                            drop(guard);
                            tokio::time::sleep(Duration::from_secs(30)).await;
                            continue;
                        }

                        guard.extend(jobs.into_iter().map(Command::Job));
                    }
                    Ok(Response::Done) => {
                        guard.push_back(Command::Shutdown);

                        break;
                    }
                    _ => {
                        drop(guard);
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        continue;
                    }
                }
                drop(guard)
            }
        }
    }

    async fn process_job(&mut self) {
        self.politeness_factor = self.config.politeness_factor;

        let mut job = self.current_job.take().unwrap();
        tracing::info!("Processing job: {:?}", job.domain);

        self.robotstxt.clear();

        let mut url_responses: Vec<UrlResponse> = Vec::new();
        let mut discovered_urls: HashSet<Url> = HashSet::new();

        let mut crawled_sitemaps: HashSet<Site> = HashSet::new();

        while let Some(retryable_url) = job.urls.pop_front() {
            if retryable_url.retries > self.config.max_url_slowdown_retry {
                continue;
            }

            if retryable_url.url.host_str().is_none()
                || !matches!(retryable_url.url.scheme(), "http" | "https")
            {
                continue;
            }

            if !self
                .robotstxt
                .is_allowed(&retryable_url.url, &self.config.user_agent.token)
                .await
            {
                continue;
            }

            let site = Site(retryable_url.url.host_str().unwrap_or_default().to_string());
            if job.fetch_sitemap && !crawled_sitemaps.contains(&site) {
                crawled_sitemaps.insert(site.clone());

                if let Some(sitemap) = self.robotstxt.sitemap(&retryable_url.url).await {
                    let sitemap_urls = self.urls_from_sitemap(sitemap, 0, 5).await;
                    discovered_urls.extend(
                        sitemap_urls
                            .into_iter()
                            .filter(|url| url.as_str().len() < MAX_URL_LEN_BYTES),
                    );
                }
            }

            let res = self.process_url(retryable_url.url.clone()).await;

            let mut delay = res.fetch_time.mul_f32(self.politeness_factor);

            if delay < Duration::from_millis(self.config.min_crawl_delay_ms) {
                delay = Duration::from_millis(self.config.min_crawl_delay_ms);
            }

            if delay > Duration::from_millis(self.config.max_crawl_delay_ms) {
                delay = Duration::from_millis(self.config.max_crawl_delay_ms);
            }

            tokio::time::sleep(delay).await;

            if let UrlResponse::Failed {
                url: _,
                status_code,
            } = res.response
            {
                if matches!(status_code, Some(429)) {
                    let mut retryable_url = retryable_url;
                    retryable_url.retries += 1;
                    job.urls.push_back(retryable_url);
                    continue;
                }
            }

            discovered_urls.extend(
                res.new_urls
                    .into_iter()
                    .filter(|url| url.as_str().len() < MAX_URL_LEN_BYTES)
                    .take(MAX_OUTGOING_URLS_PER_PAGE),
            );
            url_responses.push(res.response);
        }

        let job_response = JobResponse {
            domain: job.domain,
            url_responses,
            discovered_urls: discovered_urls
                .into_iter()
                .filter(|url| url.as_str().len() < MAX_URL_LEN_BYTES)
                .collect(),
            weight_budget: job.weight_budget,
        };

        self.results.lock().await.push(job_response);
    }

    async fn process_url(&mut self, url: Url) -> ProcessedUrl {
        let fetch = self.crawl_url(url.clone()).await;

        match fetch {
            Ok(datum) => {
                if matches!(datum.status_code, 200 | 301 | 302) {
                    if datum.status_code == 200 {
                        self.save_datum(datum.clone()).await;

                        match Html::parse(&datum.body, datum.url.as_str()) {
                            Ok(html) => {
                                let new_urls = html
                                    .all_links()
                                    .into_iter()
                                    .map(|link| link.destination)
                                    .filter(|url| {
                                        !url.path().ends_with(".pdf")
                                            && !url.path().ends_with(".jpg")
                                            && !url.path().ends_with(".zip")
                                            && !url.path().ends_with(".png")
                                            && !url.path().ends_with(".css")
                                            && !url.path().ends_with(".js")
                                            && !url.path().ends_with(".json")
                                            && !url.path().ends_with(".jsonp")
                                            && !url.path().ends_with(".woff2")
                                            && !url.path().ends_with(".woff")
                                            && !url.path().ends_with(".ttf")
                                            && !url.path().ends_with(".svg")
                                            && !url.path().ends_with(".gif")
                                            && !url.path().ends_with(".jpeg")
                                            && !url.path().ends_with(".ico")
                                            && !url.path().ends_with(".mp4")
                                            && !url.path().ends_with(".mp3")
                                            && !url.path().ends_with(".avi")
                                            && !url.path().ends_with(".mov")
                                            && !url.path().ends_with(".mpeg")
                                            && !url.path().ends_with(".webm")
                                            && !url.path().ends_with(".wav")
                                            && !url.path().ends_with(".flac")
                                            && !url.path().ends_with(".aac")
                                            && !url.path().ends_with(".ogg")
                                            && !url.path().ends_with(".m4a")
                                            && !url.path().ends_with(".m4v")
                                    })
                                    .collect();

                                let url_res = UrlResponse::Success { url: datum.url };

                                ProcessedUrl {
                                    new_urls,
                                    response: url_res,
                                    fetch_time: Duration::from_millis(datum.fetch_time_ms),
                                }
                            }
                            Err(_) => ProcessedUrl {
                                new_urls: Vec::new(),
                                response: UrlResponse::Failed {
                                    url,
                                    status_code: None,
                                },
                                fetch_time: Duration::from_millis(datum.fetch_time_ms),
                            },
                        }
                    } else {
                        let url_res = UrlResponse::Redirected {
                            url,
                            new_url: datum.url,
                        };

                        ProcessedUrl {
                            new_urls: Vec::new(),
                            response: url_res,
                            fetch_time: Duration::from_millis(datum.fetch_time_ms),
                        }
                    }
                } else {
                    if datum.status_code == 429 {
                        self.politeness_factor *= 2.0;

                        if self.politeness_factor > self.config.max_politeness_factor {
                            self.politeness_factor = self.config.max_politeness_factor;
                        }

                        tracing::warn!(
                            "politeness factor increased to {} for {}",
                            self.politeness_factor,
                            &url
                        );
                    }

                    tracing::debug!("failed to fetch url ({}): {}", &url, datum.status_code);
                    ProcessedUrl {
                        new_urls: Vec::new(),
                        response: UrlResponse::Failed {
                            url,
                            status_code: Some(datum.status_code),
                        },
                        fetch_time: Duration::from_millis(datum.fetch_time_ms),
                    }
                }
            }
            Err(err) => {
                tracing::debug!("failed to fetch url ({}): {}", &url, err);

                ProcessedUrl {
                    new_urls: Vec::new(),
                    response: UrlResponse::Failed {
                        url,
                        status_code: None,
                    },
                    fetch_time: Duration::from_millis(0),
                }
            }
        }
    }

    async fn save_datum(&self, datum: CrawlDatum) {
        if datum.status_code != 200 {
            return;
        }

        self.writer.write(datum).await.ok();
    }

    async fn fetch(&self, url: Url) -> Result<reqwest::Response> {
        let backoff = ExponentialBackoff::from_millis(1000)
            .with_limit(Duration::from_secs(20))
            .take(3);

        let mut res = Err(Error::FetchFailed(reqwest::StatusCode::IM_A_TEAPOT).into());
        for time in backoff {
            if let Ok(cur_res) = self.client.get(url.to_string()).send().await {
                res = Ok(cur_res);
                break;
            } else {
                tokio::time::sleep(time).await;
            }
        }

        res
    }

    async fn crawl_url(&self, url: Url) -> Result<CrawlDatum> {
        let start = Instant::now();
        let res = self.fetch(url.clone()).await?;
        let fetch_time = start.elapsed();

        let headers: HashMap<_, _> = res
            .headers()
            .iter()
            .filter_map(|(k, v)| {
                let k = k.to_string();
                if let Ok(v) = v.to_str() {
                    return Some((k, v.to_string()));
                }

                None
            })
            .collect();

        // check if content length is too large
        if let Some(content_length) = headers.get("content-length") {
            if content_length.parse::<usize>().unwrap_or(0) > MAX_CONTENT_LENGTH {
                return Err(Error::ContentTooLarge.into());
            }
        }

        // check if content type is html
        if let Some(content_type) = headers.get("content-type") {
            if !content_type.contains("text/html") {
                return Err(Error::InvalidContentType(content_type.to_string()).into());
            }
        }

        let status_code = res.status().as_u16();

        if status_code == 301 || status_code == 302 {
            let location = res
                .headers()
                .get("location")
                .ok_or(Error::InvalidRedirect)?;

            let location = location.to_str().map_err(|_| Error::InvalidRedirect)?;

            let url = Url::parse(location)
                .or_else(|_| url.join(location))
                .map_err(|_| Error::InvalidRedirect)?;

            return Ok(CrawlDatum {
                url,
                status_code,
                headers,
                body: String::new(),
                fetch_time_ms: fetch_time.as_millis() as u64,
            });
        }

        let res_url = res.url().clone();

        let content_type = res
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<Mime>().ok());
        let encoding_name = content_type
            .as_ref()
            .and_then(|mime| mime.get_param("charset").map(|charset| charset.as_str()))
            .unwrap_or("utf-8");
        let encoding = Encoding::for_label(encoding_name.as_bytes()).unwrap_or(UTF_8);

        let mut bytes = Vec::new();

        let mut stream = res.bytes_stream();
        while let Some(b) = stream.next().await {
            if b.is_err() {
                return Err(Error::ContentTooLarge.into());
            }

            let b = b.unwrap();

            bytes.extend_from_slice(&b);

            if bytes.len() > MAX_CONTENT_LENGTH {
                return Err(Error::ContentTooLarge.into());
            }
        }

        let (text, _, _) = encoding.decode(&bytes);
        let body = text.to_string();

        Ok(CrawlDatum {
            url: res_url,
            status_code,
            headers,
            body,
            fetch_time_ms: fetch_time.as_millis() as u64,
        })
    }

    fn urls_from_sitemap(
        &self,
        sitemap: Url,
        depth: usize,
        max_depth: usize,
    ) -> BoxFuture<'_, Vec<Url>> {
        async move {
            if depth == max_depth {
                return vec![];
            }

            // fetch url
            let res = self.fetch(sitemap).await;

            if res.is_err() {
                return vec![];
            }
            let res = res.unwrap();

            if res.status() != reqwest::StatusCode::OK {
                return vec![];
            }

            let body = res.text().await;

            if body.is_err() {
                return vec![];
            }

            let body = body.unwrap();

            // parse xml
            let mut reader = quick_xml::Reader::from_str(&body);
            let mut buf = Vec::new();

            let mut urls = vec![];

            let mut in_sitemap = false;
            let mut in_url = false;
            let mut in_loc = false;

            loop {
                match reader.read_event(&mut buf) {
                    Ok(Event::Start(ref e)) => {
                        if e.name() == b"sitemap" {
                            in_sitemap = true;
                        } else if e.name() == b"url" {
                            in_url = true;
                        } else if e.name() == b"loc" {
                            in_loc = true;
                        }
                    }
                    Ok(Event::End(ref e)) => {
                        if e.name() == b"sitemap" {
                            in_sitemap = false;
                        } else if e.name() == b"url" {
                            in_url = false;
                        } else if e.name() == b"loc" {
                            in_loc = false;
                        }
                    }
                    Ok(Event::Text(e)) => {
                        if in_sitemap && in_loc {
                            if let Ok(url) = Url::parse(&e.unescape_and_decode(&reader).unwrap()) {
                                urls.append(
                                    &mut self.urls_from_sitemap(url, depth + 1, max_depth).await,
                                );
                                tokio::time::sleep(Duration::from_millis(
                                    self.config.min_crawl_delay_ms,
                                ))
                                .await;
                            }
                        } else if in_url && in_loc {
                            if let Ok(url) = Url::parse(&e.unescape_and_decode(&reader).unwrap()) {
                                urls.push(url);
                            }
                        }
                    }
                    Ok(Event::Eof) => break,
                    Err(e) => {
                        tracing::debug!("failed to parse sitemap: {}", e);
                        break;
                    }
                    _ => (),
                }

                buf.clear();
            }

            urls
        }
        .boxed()
    }
}

struct ProcessedUrl {
    new_urls: Vec<Url>,
    response: UrlResponse,
    fetch_time: Duration,
}
