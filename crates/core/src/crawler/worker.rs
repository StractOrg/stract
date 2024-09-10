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
use quick_xml::events::Event;
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
    distributed::{retry_strategy::ExponentialBackoff, sonic},
    entrypoint::crawler::router::{NewJob, RouterService},
    warc,
    webpage::{url_ext::UrlExt, Html},
};

use super::{
    encoded_body, reqwest_client, robots_txt::RobotsTxtManager,
    wander_prirotiser::WanderPrioritiser, CrawlDatum, DatumStream, Domain, Error, Result,
    RetrieableUrl, Site, WarcWriter, WeightedUrl, WorkerJob, MAX_CONTENT_LENGTH,
    MAX_OUTGOING_URLS_PER_PAGE,
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
    client: reqwest::Client,
    config: Arc<CrawlerConfig>,
    router_hosts: Vec<SocketAddr>,
}

impl WorkerThread {
    pub fn new(
        writer: Arc<WarcWriter>,
        config: CrawlerConfig,
        router_hosts: Vec<SocketAddr>,
    ) -> Result<Self> {
        let client = reqwest_client(&config)?;

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
                        self.client.clone(),
                        self.config.clone(),
                        self.writer.clone(),
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
    client: reqwest::Client,
    has_gotten_429_response: bool,
    politeness_factor: u32,
    robotstxt: RobotsTxtManager,
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
        client: reqwest::Client,
        config: Arc<CrawlerConfig>,
        writer: Arc<S>,
    ) -> Self {
        Self {
            writer,
            politeness_factor: config.start_politeness_factor,
            min_politeness_factor: config.min_politeness_factor,
            robotstxt: RobotsTxtManager::new(&config),
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
        self.crawl_sitemaps().await;

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
            .collect();

        urls.sort_by(|(a, _), (b, _)| a.cmp(b));
        urls.dedup_by(|(a, _), (b, _)| a == b);

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

        if !self.robotstxt.is_allowed(retryable_url.url()).await {
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
        for url in self.job.urls.iter().map(|url| url.url()) {
            let site = Site(url.host_str().unwrap_or_default().to_string());
            if !self.crawled_sitemaps.contains(&site) {
                self.crawled_sitemaps.insert(site.clone());

                let sitemaps = self.robotstxt.sitemaps(url).await;

                for sitemap in sitemaps {
                    self.sitemap_urls
                        .extend(self.urls_from_sitemap(sitemap, 5).await);
                }
            }
        }
    }

    async fn process_urls(&mut self, mut urls: VecDeque<RetrieableUrl>) {
        while let Some(retryable_url) = urls.pop_front() {
            if let UrlVisit::Skip = self.verify_url(&retryable_url).await {
                continue;
            }

            if let Some(delay) = self.robotstxt.crawl_delay(retryable_url.url()).await {
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
            .collect()
    }

    async fn process_url(&mut self, url: Url) -> Result<ProcessedUrl> {
        let datum = self.crawl_url(url.clone()).await?;
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
            .get(url.to_string())
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
        fetch_time: Duration,
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
                fetch_time_ms: fetch_time.as_millis() as u64,
            }))
        } else {
            Ok(None)
        }
    }

    async fn crawl_url(&mut self, url: Url) -> Result<CrawlDatum> {
        let mut url = url;
        url.normalize_in_place();

        if self.crawled_urls.contains(&url) {
            return Err(Error::from(anyhow!("url already crawled: {}", url)));
        }

        let start = Instant::now();
        let res = self.fetch_with_https_priority(url.clone()).await;
        let fetch_time = start.elapsed();
        self.politeness_delay(fetch_time).await;

        // we want to delay before returning the error
        let res = res?;

        self.crawled_urls.insert(url.clone());
        let payload_type = self.check_headers(&res)?;

        if let Some(datum) = self.redirect_datum(&res, &url, payload_type, fetch_time)? {
            return Ok(datum);
        }

        let status_code = res.status();

        if status_code != reqwest::StatusCode::OK {
            return Err(Error::FetchFailed {
                status_code,
                headers: res.headers().clone(),
            });
        }

        let mut res_url = res.url().clone();
        res_url.normalize_in_place();

        self.crawled_urls.insert(res_url.clone());

        let body = encoded_body(res).await?;

        Ok(CrawlDatum {
            url: res_url,
            body,
            payload_type,
            fetch_time_ms: fetch_time.as_millis() as u64,
        })
    }

    async fn urls_from_sitemap(&self, sitemap: Url, max_depth: usize) -> Vec<Url> {
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum SitemapEntry {
    Url(Url),
    Sitemap(Url),
}

fn parse_sitemap(s: &str) -> Vec<SitemapEntry> {
    let mut reader = quick_xml::Reader::from_str(s);

    let mut res = vec![];

    let mut in_sitemap = false;
    let mut in_url = false;
    let mut in_loc = false;

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                if e.name().as_ref() == b"sitemap" {
                    in_sitemap = true;
                } else if e.name().as_ref() == b"url" {
                    in_url = true;
                } else if e.name().as_ref() == b"loc" {
                    in_loc = true;
                }
            }
            Ok(Event::End(ref e)) => {
                if e.name().as_ref() == b"sitemap" {
                    in_sitemap = false;
                } else if e.name().as_ref() == b"url" {
                    in_url = false;
                } else if e.name().as_ref() == b"loc" {
                    in_loc = false;
                }
            }
            Ok(Event::Text(e)) => {
                if in_sitemap && in_loc {
                    if let Ok(url) = Url::parse(&e.unescape().unwrap()) {
                        res.push(SitemapEntry::Sitemap(url));
                    }
                } else if in_url && in_loc {
                    if let Ok(url) = Url::parse(&e.unescape().unwrap()) {
                        res.push(SitemapEntry::Url(url));
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
    }

    res
}

#[cfg(test)]
mod tests {
    #[test]
    fn parse_sitemap() {
        let dr = r#"<sitemapindex>
        <sitemap>
        <loc>https://www.dr.dk/drtv/sitemap.xml</loc>
        </sitemap>
        <sitemap>
        <loc>https://www.dr.dk/sitemap.tvguide.xml</loc>
        </sitemap>
        <sitemap>
        <loc>
        https://www.dr.dk/sitemap.kommunalvalg.resultater.xml
        </loc>
        </sitemap>
        <sitemap>
        <loc>https://www.dr.dk/sitemap.folketingsvalg2022.xml</loc>
        </sitemap>
        </sitemapindex>"#;

        let entries = super::parse_sitemap(dr);
        assert_eq!(
            entries,
            vec![
                super::SitemapEntry::Sitemap("https://www.dr.dk/drtv/sitemap.xml".parse().unwrap()),
                super::SitemapEntry::Sitemap(
                    "https://www.dr.dk/sitemap.tvguide.xml".parse().unwrap()
                ),
                super::SitemapEntry::Sitemap(
                    "https://www.dr.dk/sitemap.kommunalvalg.resultater.xml"
                        .parse()
                        .unwrap()
                ),
                super::SitemapEntry::Sitemap(
                    "https://www.dr.dk/sitemap.folketingsvalg2022.xml"
                        .parse()
                        .unwrap()
                ),
            ]
        );

        let dr = r#"<urlset>
        <url>
        <lastmod>2023-10-18T05:40:04.7435930+00:00</lastmod>
        <loc>https://www.dr.dk/drtv/serie/sleepover_6382</loc>
        </url>
        <url>
        <lastmod>2023-10-18T05:40:04.7435930+00:00</lastmod>
        <loc>https://www.dr.dk/drtv/saeson/sleepover_9673</loc>
        </url>
        <url>
        <lastmod>2023-10-18T05:40:04.7435930+00:00</lastmod>
        <loc>
        https://www.dr.dk/drtv/episode/sleepover_-zoologisk-museum_52239
        </loc>
        </url>
        <url>
        <lastmod>2023-10-18T05:40:04.7435930+00:00</lastmod>
        <loc>
        https://www.dr.dk/drtv/episode/sleepover_-koebenhavns-raadhus_52252
        </loc>
        </url>
        </urlset>"#;

        let entries = super::parse_sitemap(dr);
        assert_eq!(
            entries,
            vec![
                super::SitemapEntry::Url(
                    "https://www.dr.dk/drtv/serie/sleepover_6382"
                        .parse()
                        .unwrap()
                ),
                super::SitemapEntry::Url(
                    "https://www.dr.dk/drtv/saeson/sleepover_9673"
                        .parse()
                        .unwrap()
                ),
                super::SitemapEntry::Url(
                    "https://www.dr.dk/drtv/episode/sleepover_-zoologisk-museum_52239"
                        .parse()
                        .unwrap()
                ),
                super::SitemapEntry::Url(
                    "https://www.dr.dk/drtv/episode/sleepover_-koebenhavns-raadhus_52252"
                        .parse()
                        .unwrap()
                ),
            ]
        );
    }
}
