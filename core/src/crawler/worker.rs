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
use encoding_rs::{Encoding, UTF_8};
use futures::{future::BoxFuture, FutureExt};
use hashbrown::{HashMap, HashSet};
use mime::Mime;
use quick_xml::events::Event;
use rand::seq::SliceRandom;
use tokio_stream::StreamExt;

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
    webpage::Html,
};

use super::{
    reqwest_client, robots_txt::RobotsTxtManager, site_graph::SiteGraph, CrawlDatum, DatumStream,
    Domain, Error, Result, RetrieableUrl, Site, UrlResponse, WarcWriter, WorkerJob,
};

const MAX_CONTENT_LENGTH: usize = 32 * 1024 * 1024; // 32 MB

struct ProcessedUrl {
    new_urls: Vec<Url>,
    response: UrlResponse,
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

    pub async fn run(self) {
        loop {
            let conn = self.router_conn().await.unwrap();
            let res = conn
                .send_with_timeout(&NewJob {}, Duration::from_secs(90))
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
    politeness_factor: f32,
    robotstxt: RobotsTxtManager,
    crawled_urls: HashSet<Url>,
    crawled_sitemaps: HashSet<Site>,
    sitemap_urls: HashSet<Url>,
    config: Arc<CrawlerConfig>,
    site_graph: SiteGraph,
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
            politeness_factor: config.politeness_factor,
            robotstxt: RobotsTxtManager::new(
                client.clone(),
                Duration::from_secs(config.robots_txt_cache_sec),
            ),
            client,
            crawled_urls: HashSet::new(),
            crawled_sitemaps: HashSet::new(),
            sitemap_urls: HashSet::new(),
            config,
            site_graph: SiteGraph::new(),
            job,
        }
    }

    pub async fn run(mut self) {
        tracing::info!("Processing job: {:?}", self.job.domain);

        self.scheduled_urls().await;

        if self.job.wandering_urls > 0 {
            self.wander().await;
        }
    }

    async fn scheduled_urls(&mut self) {
        let urls = self.job.urls.drain(..).collect();
        self.process_urls(urls, true).await;
    }

    async fn wander(&mut self) {
        let mut urls: Vec<(Url, f64)> = self
            .site_graph
            .compute_centralities()
            .into_iter()
            .map(|(node_ref, score)| {
                (
                    Url::from(self.site_graph.get_node(node_ref).unwrap().clone()),
                    score,
                )
            })
            .chain(self.sitemap_urls.drain().map(|url| (url.clone(), 0.0)))
            .filter(|(url, _)| !self.crawled_urls.contains(url))
            .filter(|(url, _)| self.job.domain == Domain::from(url))
            .filter(|(_, score)| score.is_finite())
            .collect();

        urls.sort_by(|(a, _), (b, _)| a.cmp(b));
        urls.dedup();

        urls.sort_by(|(_, a), (_, b)| b.total_cmp(a));

        let urls = urls
            .into_iter()
            .take(self.job.wandering_urls as usize)
            .map(|(url, _)| url)
            .map(RetrieableUrl::from)
            .collect();

        self.process_urls(urls, false).await;
    }

    async fn process_urls(&mut self, mut urls: VecDeque<RetrieableUrl>, fetch_sitemap: bool) {
        while let Some(retryable_url) = urls.pop_front() {
            if self.crawled_urls.contains(&retryable_url.url) {
                continue;
            }

            if retryable_url.retries > self.config.max_url_slowdown_retry {
                continue;
            }

            if retryable_url.url.host_str().is_none()
                || !matches!(retryable_url.url.scheme(), "http" | "https")
            {
                continue;
            }

            if !self.config.dry_run
                && !self
                    .robotstxt
                    .is_allowed(&retryable_url.url, &self.config.user_agent.token)
                    .await
            {
                continue;
            }

            let site = Site(retryable_url.url.host_str().unwrap_or_default().to_string());
            if !self.config.dry_run && fetch_sitemap && !self.crawled_sitemaps.contains(&site) {
                self.crawled_sitemaps.insert(site.clone());

                if let Some(sitemap) = self.robotstxt.sitemap(&retryable_url.url).await {
                    self.sitemap_urls
                        .extend(self.urls_from_sitemap(sitemap, 0, 5).await);
                }
            }

            let res = self.process_url(retryable_url.url.clone()).await;

            match res.response {
                UrlResponse::Success { url } => {
                    self.crawled_urls.insert(url.clone());

                    let from_node = self.site_graph.add_node(url.clone().into());

                    for new_url in res.new_urls {
                        if new_url.host_str().is_none() {
                            continue;
                        }

                        if new_url.host_str() != retryable_url.url.host_str() {
                            continue;
                        }

                        let to_node = self.site_graph.add_node(new_url.clone().into());
                        self.site_graph.add_edge(from_node, to_node);
                    }
                }
                UrlResponse::Failed {
                    url: _,
                    status_code,
                } => {
                    if matches!(status_code, Some(429)) {
                        let mut retryable_url = retryable_url;
                        retryable_url.retries += 1;
                        urls.push_back(retryable_url);
                        continue;
                    }
                }
                UrlResponse::Redirected { url: _, new_url: _ } => {}
            }
        }
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
                                    .filter(|url| url.as_str().len() <= MAX_URL_LEN_BYTES)
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
                                }
                            }
                            Err(_) => ProcessedUrl {
                                new_urls: Vec::new(),
                                response: UrlResponse::Failed {
                                    url,
                                    status_code: None,
                                },
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
        if self.config.dry_run {
            tracing::debug!("dry run: {}", url);
            return Err(Error::FetchFailed(reqwest::StatusCode::IM_A_TEAPOT).into());
        }

        let backoff = ExponentialBackoff::from_millis(self.config.min_crawl_delay_ms)
            .with_limit(Duration::from_millis(self.config.max_crawl_delay_ms))
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

        let mut delay = fetch_time.mul_f32(self.politeness_factor);

        if delay < Duration::from_millis(self.config.min_crawl_delay_ms) {
            delay = Duration::from_millis(self.config.min_crawl_delay_ms);
        }

        if delay > Duration::from_millis(self.config.max_crawl_delay_ms) {
            delay = Duration::from_millis(self.config.max_crawl_delay_ms);
        }

        tokio::time::sleep(delay).await;

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
        let payload_type = match headers.get("content-type") {
            Some(ct) if ct.contains("text/html") => warc::PayloadType::Html,
            Some(ct) if ct.contains("application/rss") => warc::PayloadType::Rss,
            Some(ct) if ct.contains("application/atom") => warc::PayloadType::Atom,
            ct => return Err(Error::InvalidContentType(format!("{ct:?}")).into()),
        };

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
                payload_type,
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
            body,
            payload_type,
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
            let entries = parse_sitemap(&body);

            let mut urls = vec![];

            for entry in entries {
                match entry {
                    SitemapEntry::Url(url) => {
                        urls.push(url);
                    }
                    SitemapEntry::Sitemap(url) => {
                        tokio::time::sleep(Duration::from_millis(self.config.min_crawl_delay_ms))
                            .await;
                        urls.append(&mut self.urls_from_sitemap(url, depth + 1, max_depth).await);
                    }
                }
            }

            urls
        }
        .boxed()
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
