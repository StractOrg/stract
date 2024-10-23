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
use anyhow::anyhow;
use hashbrown::HashSet;
use itertools::Itertools;
use rand::seq::SliceRandom;

use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use url::Url;

use crate::{
    config::CrawlerConfig,
    crawler::MAX_URL_LEN_BYTES,
    dated_url::DatedUrl,
    distributed::{retry_strategy::ExponentialBackoff, sonic},
    entrypoint::crawler::router::{NewJob, RouterService},
    sitemap::{parse_sitemap, SitemapEntry},
    warc,
    webpage::{url_ext::UrlExt, Html},
};

use super::{
    encoded_body, robot_client::RobotClient, wander_prirotiser::WanderPrioritiser, CrawlDatum,
    DatumStream, Domain, Error, Result, RetrieableUrl, Site, WarcWriter, WeightedUrl, WorkerJob,
    MAX_CONTENT_LENGTH, MAX_OUTGOING_URLS_PER_PAGE,
};

const IGNORED_EXTENSIONS: [&str; 27] = [
    ".pdf", ".jpg", ".zip", ".png", ".css", ".js", ".json", ".jsonp", ".woff2", ".woff", ".ttf",
    ".svg", ".gif", ".jpeg", ".ico", ".mp4", ".mp3", ".avi", ".mov", ".mpeg", ".webm", ".wav",
    ".flac", ".aac", ".ogg", ".m4a", ".m4v",
];

const INITIAL_WANDER_STEPS: u64 = 4;

enum UrlVisit {
    Skip,
    CanCrawl,
}

struct ProcessedUrl {
    new_urls: Vec<Url>,
}

pub struct WorkerThread {
    writer: Arc<WarcWriter>,
    config: Arc<CrawlerConfig>,
    router_hosts: Vec<SocketAddr>,
    client: RobotClient,
}

impl WorkerThread {
    pub fn new(
        writer: Arc<WarcWriter>,
        client: RobotClient,
        config: CrawlerConfig,
        router_hosts: Vec<SocketAddr>,
    ) -> Result<Self> {
        Ok(Self {
            writer,
            client,
            config: Arc::new(config),
            router_hosts,
        })
    }

    async fn router_conn(&self) -> Result<sonic::service::Connection<RouterService>> {
        let retry = ExponentialBackoff::from_millis(1_000).with_limit(Duration::from_secs(10));

        let router = *self.router_hosts.choose(&mut rand::thread_rng()).unwrap();

        sonic::service::Connection::create_with_timeout_retry(
            router,
            Duration::from_secs(90),
            retry,
        )
        .await
        .map_err(|e| Error::from(anyhow!(e)))
    }

    pub async fn run(self) {
        loop {
            let mut conn = self.router_conn().await.unwrap();
            let res = conn
                .send_with_timeout(NewJob {}, Duration::from_secs(90))
                .await;

            match res {
                Ok(Some(job)) => {
                    let executor = JobExecutor::new(
                        job.into(),
                        self.config.clone(),
                        self.writer.clone(),
                        self.client.clone(),
                    );
                    executor.run().await;
                }
                Ok(None) => {
                    return;
                }
                _ => {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    continue;
                }
            }
        }
    }
}

pub struct JobExecutor<S: DatumStream> {
    writer: Arc<S>,
    client: RobotClient,
    has_gotten_429_response: bool,
    politeness_factor: u32,
    crawled_urls: HashSet<Url>,
    crawled_sitemaps: HashSet<Site>,
    sitemap_urls: HashSet<Url>,
    min_crawl_delay: Duration,
    max_crawl_delay: Duration,
    max_url_slowdown_retry: u8,
    min_politeness_factor: u32,
    max_politeness_factor: u32,
    wander_prioritiser: WanderPrioritiser,
    wandered_urls: u64,
    job: WorkerJob,
}

impl<S: DatumStream> JobExecutor<S> {
    pub fn new(
        job: WorkerJob,
        config: Arc<CrawlerConfig>,
        writer: Arc<S>,
        client: RobotClient,
    ) -> Self {
        Self {
            writer,
            politeness_factor: config.start_politeness_factor,
            min_politeness_factor: config.min_politeness_factor,
            client,
            crawled_urls: HashSet::new(),
            crawled_sitemaps: HashSet::new(),
            wandered_urls: 0,
            sitemap_urls: HashSet::new(),
            min_crawl_delay: Duration::from_millis(config.min_crawl_delay_ms),
            max_crawl_delay: Duration::from_millis(config.max_crawl_delay_ms),
            max_url_slowdown_retry: config.max_url_slowdown_retry,
            max_politeness_factor: config.max_politeness_factor,
            wander_prioritiser: WanderPrioritiser::new(),
            has_gotten_429_response: false,
            job,
        }
    }

    pub async fn run(mut self) {
        tracing::info!("Processing job: {:?}", self.job.domain);
        for site in self
            .job
            .urls
            .iter()
            .filter_map(|url| url.url().normalized_host())
        {
            if let Ok(url) = Url::parse(format!("https://{}", site).as_str()) {
                self.wander_prioritiser.inc(url, 1.0);
            }
        }

        let mut wander_steps = 0;
        while self.wandered_urls < self.job.wandering_urls
            && self.wander_prioritiser.known_urls() > 0
            && wander_steps < INITIAL_WANDER_STEPS
        {
            self.wander().await;
            wander_steps += 1;
        }

        self.scheduled_urls().await;

        if self.wandered_urls < self.job.wandering_urls {
            self.crawl_sitemaps().await;
        }

        while self.wandered_urls < self.job.wandering_urls
            && self.wander_prioritiser.known_urls() > 0
        {
            self.wander().await;
        }
    }

    async fn scheduled_urls(&mut self) {
        let urls = self.job.urls.drain(..).collect();
        self.process_urls(urls).await;
    }

    async fn wander(&mut self) {
        let mut urls: Vec<(Url, f64)> = self
            .wander_prioritiser
            .top_and_clear(self.job.wandering_urls.saturating_sub(self.wandered_urls) as usize)
            .into_iter()
            .chain(self.sitemap_urls.drain().map(|url| (url.clone(), 0.0)))
            .map(|(mut url, score)| {
                url.normalize_in_place();
                (url, score)
            })
            .filter(|(url, _)| !self.crawled_urls.contains(url))
            .filter(|(url, _)| self.job.domain == Domain::from(url))
            .filter(|(_, score)| score.is_finite())
            .unique_by(|(url, _)| url.clone())
            .collect();

        urls.sort_by(|(_, a), (_, b)| b.total_cmp(a));

        let urls: VecDeque<_> = urls
            .into_iter()
            .map(|(url, _)| url)
            .map(|mut url| {
                url.normalize_in_place();
                url
            })
            .filter(|url| !self.crawled_urls.contains(url))
            .take(self.job.wandering_urls.saturating_sub(self.wandered_urls) as usize)
            .map(|url| WeightedUrl { url, weight: 0.0 })
            .map(RetrieableUrl::from)
            .collect();

        self.wandered_urls += urls.len() as u64;

        self.process_urls(urls).await;
    }

    async fn verify_url(&mut self, retryable_url: &RetrieableUrl) -> UrlVisit {
        if Domain::from(retryable_url.url()) != self.job.domain {
            return UrlVisit::Skip;
        }

        if self.crawled_urls.contains(retryable_url.url()) {
            return UrlVisit::Skip;
        }

        if retryable_url.retries > self.max_url_slowdown_retry {
            return UrlVisit::Skip;
        }

        if retryable_url.url().host_str().is_none()
            || !matches!(retryable_url.url().scheme(), "http" | "https")
        {
            return UrlVisit::Skip;
        }

        if !self
            .client
            .robots_txt_manager()
            .is_allowed(retryable_url.url())
            .await
        {
            return UrlVisit::Skip;
        }

        if let Some(port) = retryable_url.url().port() {
            if port != 80 && port != 443 {
                return UrlVisit::Skip;
            }
        }

        UrlVisit::CanCrawl
    }

    async fn crawl_sitemaps(&mut self) {
        let urls: Vec<_> = self.job.urls.iter().map(|url| url.url()).cloned().collect();

        for url in urls {
            let site = Site(url.host_str().unwrap_or_default().to_string());
            if !self.crawled_sitemaps.contains(&site) {
                self.crawled_sitemaps.insert(site.clone());

                let sitemaps = self.client.robots_txt_manager().sitemaps(&url).await;

                for sitemap in sitemaps {
                    let res = self.urls_from_sitemap(sitemap, 5).await;
                    self.sitemap_urls
                        .extend(res.into_iter().map(|dated_url| dated_url.url));
                }
            }
        }
    }

    pub async fn process_urls(&mut self, mut urls: VecDeque<RetrieableUrl>) {
        tracing::debug!("processing {} urls", urls.len());
        while let Some(mut retryable_url) = urls.pop_front() {
            retryable_url.weighted_url.url.normalize_in_place();

            if let UrlVisit::Skip = self.verify_url(&retryable_url).await {
                tracing::debug!("skipping url: {}", retryable_url.url());
                continue;
            }

            if let Some(delay) = self
                .client
                .robots_txt_manager()
                .crawl_delay(retryable_url.url())
                .await
            {
                if delay > self.min_crawl_delay {
                    self.min_crawl_delay = delay.min(self.max_crawl_delay);
                }
            }

            let res = self.process_url(retryable_url.url().clone()).await;

            match res {
                Ok(res) => {
                    if !self.has_gotten_429_response {
                        self.decrease_politeness();
                    }

                    let weight = retryable_url.weighted_url.weight;

                    for new_url in res.new_urls {
                        if new_url.host_str().is_none() {
                            continue;
                        }

                        if new_url.root_domain() != retryable_url.url().root_domain() {
                            continue;
                        }

                        self.wander_prioritiser.inc(new_url, weight);
                    }
                }
                Err(Error::FetchFailed {
                    status_code,
                    headers,
                }) if status_code == 429 => {
                    self.has_gotten_429_response = true;

                    if headers.contains_key("retry-after") {
                        let retry_after = headers["retry-after"]
                            .to_str()
                            .ok()
                            .and_then(|s| s.parse::<u64>().ok())
                            .map(Duration::from_secs);

                        if let Some(retry_after) = retry_after {
                            if retry_after > self.max_crawl_delay {
                                return; // don't crawl anymore from this site
                            }

                            tokio::time::sleep(retry_after).await;
                        }
                    }

                    self.increase_politeness();
                    let mut retryable_url = retryable_url;
                    retryable_url.retries += 1;
                    urls.push_back(retryable_url);
                }
                Err(Error::FetchFailed {
                    status_code,
                    headers,
                }) if status_code == 301 || status_code == 302 => {
                    if let Some(new_url) = headers
                        .get("location")
                        .and_then(|location| std::str::from_utf8(location.as_bytes()).ok())
                        .and_then(|location| {
                            Url::parse_with_base_url(&retryable_url.weighted_url.url, location).ok()
                        })
                    {
                        let mut retryable_url = retryable_url;
                        retryable_url.weighted_url.url = new_url;
                        retryable_url.retries += 1;
                        urls.push_back(retryable_url);
                    }
                }
                Err(_) => {}
            }
        }
    }

    fn increase_politeness(&mut self) {
        self.politeness_factor += 1;

        if self.politeness_factor > self.max_politeness_factor {
            self.politeness_factor = self.max_politeness_factor;
        }

        tracing::warn!(
            "politeness factor increased to {} for {}",
            self.politeness_factor,
            self.job.domain.as_str()
        );
    }

    fn decrease_politeness(&mut self) {
        if self.min_politeness_factor >= self.politeness_factor {
            self.politeness_factor = self.min_politeness_factor;
            return;
        }

        self.politeness_factor = self.politeness_factor.saturating_sub(1);
    }

    fn new_urls(&self, html: &Html) -> Vec<Url> {
        html.anchor_links()
            .into_iter()
            .map(|link| link.destination)
            .map(|mut url| {
                url.normalize_in_place();
                url
            })
            .filter(|url| url.as_str().len() <= MAX_URL_LEN_BYTES)
            .filter(|url| {
                IGNORED_EXTENSIONS
                    .iter()
                    .all(|ext| !url.as_str().ends_with(ext))
            })
            .filter(|url| !self.crawled_urls.contains(url))
            .unique()
            .collect()
    }

    async fn process_url(&mut self, url: Url) -> Result<ProcessedUrl> {
        let datum = self.polite_crawl_url(url.clone()).await?;
        self.save_datum(datum.clone()).await;

        match Html::parse(&datum.body, datum.url.as_str()) {
            Ok(html) => {
                let root_domain = datum.url.root_domain();
                let new_urls = self
                    .new_urls(&html)
                    .into_iter()
                    .filter(|new_url| new_url.root_domain() == root_domain)
                    .take(MAX_OUTGOING_URLS_PER_PAGE)
                    .collect();
                Ok(ProcessedUrl { new_urls })
            }
            Err(_) => Err(Error::InvalidHtml),
        }
    }

    async fn save_datum(&self, datum: CrawlDatum) {
        self.writer.write(datum).await.ok();
    }

    async fn fetch(&self, url: Url) -> Result<reqwest::Response> {
        self.client
            .get(url)
            .await?
            .send()
            .await
            .map_err(|e| Error::from(anyhow!(e)))
    }

    async fn fetch_with_https_priority(&self, url: Url) -> Result<reqwest::Response> {
        if url.scheme() == "http" {
            let mut https = url.clone();
            https
                .set_scheme("https")
                .map_err(|_| anyhow!("set scheme on url failed"))?;

            match self.fetch(https).await {
                Ok(res) => Ok(res),
                Err(_) => {
                    tokio::time::sleep(self.delay_duration()).await;
                    self.fetch(url.clone()).await
                }
            }
        } else {
            self.fetch(url.clone()).await
        }
    }

    fn delay_duration(&self) -> Duration {
        let mut delay = self.min_crawl_delay;
        delay = delay.mul_f64(2.0_f64.powi(self.politeness_factor as i32));

        if delay > self.max_crawl_delay {
            delay = self.max_crawl_delay;
        }

        delay
    }

    async fn politeness_delay(&self, fetch_time: Duration) {
        let mut delay = fetch_time;

        if delay < self.min_crawl_delay {
            delay = self.min_crawl_delay;
        }

        delay = delay.mul_f64(2.0_f64.powi(self.politeness_factor as i32));

        if delay > self.max_crawl_delay {
            delay = self.max_crawl_delay;
        }

        tokio::time::sleep(delay).await;
    }

    fn check_headers(&self, res: &reqwest::Response) -> Result<warc::PayloadType> {
        // check if content length is too large
        if let Some(content_length) = res
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok())
        {
            if content_length > MAX_CONTENT_LENGTH {
                return Err(Error::ContentTooLarge);
            }
        }

        // check if content type is html
        match res
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|val| val.to_str().ok())
        {
            Some(ct) if ct.contains("text/html") => Ok(warc::PayloadType::Html),
            Some(ct) if ct.contains("application/rss") => Ok(warc::PayloadType::Rss),
            Some(ct) if ct.contains("application/atom") => Ok(warc::PayloadType::Atom),
            ct => Err(Error::InvalidContentType(format!("{ct:?}"))),
        }
    }

    fn redirect_datum(
        &self,
        res: &reqwest::Response,
        url: &Url,
        payload_type: warc::PayloadType,
    ) -> Result<Option<CrawlDatum>> {
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

            Ok(Some(CrawlDatum {
                url,
                payload_type,
                body: String::new(),
                fetch_time_ms: 0,
            }))
        } else {
            Ok(None)
        }
    }

    async fn unpolite_crawl_url(&mut self, url: Url) -> Result<CrawlDatum> {
        tracing::debug!("crawling url: {}", url);

        let res = self.fetch_with_https_priority(url.clone()).await?;

        let payload_type = self.check_headers(&res);
        let status_code = res.status();
        let mut res_url = res.url().clone();

        if let Ok(payload_type) = payload_type {
            if let Ok(Some(datum)) = self.redirect_datum(&res, &url, payload_type) {
                return Ok(datum);
            }
        }
        if status_code != reqwest::StatusCode::OK {
            return Err(Error::FetchFailed {
                status_code,
                headers: res.headers().clone(),
            });
        }

        let body = encoded_body(res).await;

        self.crawled_urls.insert(url.clone());

        res_url.normalize_in_place();

        self.crawled_urls.insert(res_url.clone());

        Ok(CrawlDatum {
            url: res_url,
            body: body?,
            payload_type: payload_type?,
            fetch_time_ms: 0,
        })
    }

    async fn polite_crawl_url(&mut self, url: Url) -> Result<CrawlDatum> {
        let mut url = url;
        url.normalize_in_place();

        if self.crawled_urls.contains(&url) {
            return Err(Error::from(anyhow!("url already crawled: {}", url)));
        }

        let start = Instant::now();
        let res = self.unpolite_crawl_url(url).await;
        let fetch_time = start.elapsed();
        self.politeness_delay(fetch_time).await;
        let mut datum = res?;
        datum.fetch_time_ms = fetch_time.as_millis() as u64;
        Ok(datum)
    }

    async fn urls_from_sitemap(&mut self, sitemap: Url, max_depth: usize) -> Vec<DatedUrl> {
        let mut stack = vec![(sitemap, 0)];
        let mut urls = vec![];

        while let Some((url, depth)) = stack.pop() {
            if depth >= max_depth {
                continue;
            }

            let res = self.fetch_with_https_priority(url).await;
            tokio::time::sleep(self.delay_duration()).await;

            if res.is_err() {
                continue;
            }

            let res = res.unwrap();

            if res.status() != reqwest::StatusCode::OK {
                continue;
            }

            let body = res.text().await;

            if body.is_err() {
                continue;
            }

            let body = body.unwrap();

            let entries = parse_sitemap(&body);

            for entry in entries {
                match entry {
                    SitemapEntry::Url(url) => {
                        urls.push(url);
                    }
                    SitemapEntry::Sitemap(url) => {
                        stack.push((url, depth + 1));
                    }
                }
            }
        }

        urls
    }
}
