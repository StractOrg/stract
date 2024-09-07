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

use std::{
    collections::{HashMap, HashSet},
    ops::AddAssign,
    sync::{Arc, Mutex},
};

use bloom::BytesBloomFilter;

use crate::{
    config::{self, SiteStatsConfig},
    entrypoint::download_all_warc_files,
    webgraph::Node,
    webpage::{url_ext::UrlExt, Html},
    Result,
};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct SiteStats {
    pages: u64,
    blogposts: u64,
    news_articles: u64,
}

impl AddAssign<SiteStats> for SiteStats {
    fn add_assign(&mut self, rhs: SiteStats) {
        self.pages += rhs.pages;
        self.blogposts += rhs.blogposts;
        self.news_articles += rhs.news_articles
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Hash, PartialEq, Eq)]
#[serde(transparent)]
struct Site(String);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct SiteId([u8; 8]);

impl SiteId {
    fn from_url(url: String) -> Self {
        let node_id = Node::from(url).into_host().id();
        Self(node_id.as_u64().to_be_bytes())
    }
}

impl AsRef<[u8]> for SiteId {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct FinalSiteStats {
    site: Site,
    #[serde(flatten)]
    stats: SiteStats,
}

struct SiteFilter {
    bloom: BytesBloomFilter<SiteId>,
    full: HashSet<SiteId>,
}

impl SiteFilter {
    fn new(sites: Vec<SiteId>) -> Self {
        let mut bloom = BytesBloomFilter::new(sites.len() as u64, 0.01);
        let mut full = HashSet::new();

        for site in sites {
            bloom.insert(&site);
            full.insert(site);
        }

        Self { bloom, full }
    }
}

impl SiteFilter {
    fn should_process(&self, site: &SiteId) -> bool {
        self.bloom.contains(site) && self.full.contains(site)
    }
}

pub struct StatsWorker {
    stats: Mutex<HashMap<Site, SiteStats>>,
    site_filter: SiteFilter,
}

impl StatsWorker {
    fn new(site_ids: Vec<SiteId>) -> Self {
        Self {
            stats: Mutex::new(HashMap::new()),
            site_filter: SiteFilter::new(site_ids),
        }
    }

    fn process(&self, job: Job) -> Result<()> {
        let name = job.warc_path.split('/').last().unwrap();

        tracing::info!("processing {}", name);

        let source = job.source_config.clone();

        let paths = vec![job.warc_path.clone()];
        let warc_files = download_all_warc_files(&paths, &source);
        tokio::pin!(warc_files);

        for file in warc_files.by_ref() {
            for record in file.records().flatten() {
                if !self
                    .site_filter
                    .should_process(&SiteId::from_url(record.request.url.clone()))
                {
                    continue;
                }

                let webpage = match Html::parse(&record.response.body, &record.request.url) {
                    Ok(webpage) => webpage,
                    Err(err) => {
                        tracing::error!("error parsing webpage: {}", err);
                        continue;
                    }
                };

                let mut stats = SiteStats {
                    pages: 1,
                    blogposts: 0,
                    news_articles: 0,
                };

                for schema in webpage.schema_org() {
                    if schema.types_contains("NewsArticle") {
                        stats.news_articles = 1;
                    }

                    if schema.types_contains("BlogPosting") {
                        stats.blogposts = 1;
                    }
                }
                if let Some(site) = webpage
                    .url()
                    .root_domain()
                    .map(|site| Site(site.to_string()))
                {
                    *self.stats.lock().unwrap().entry(site).or_default() += stats;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Job {
    pub source_config: config::WarcSource,
    pub warc_path: String,
}

pub fn run(config: SiteStatsConfig) -> Result<()> {
    let host_centrality = speedy_kv::Db::open_or_create(&config.host_centrality_path)?;

    let site_ids: Vec<_> = crate::webgraph::centrality::top_nodes(
        &host_centrality,
        crate::webgraph::centrality::TopNodes::Top(config.top_sites),
    )
    .into_iter()
    .map(|(node, _)| SiteId(node.as_u64().to_be_bytes()))
    .collect();

    let jobs: Vec<_> = config
        .warc_source
        .paths()?
        .into_iter()
        .skip(config.skip_warc_files.unwrap_or(0))
        .take(config.limit_warc_files.unwrap_or(usize::MAX))
        .map(|warc_path| Job {
            source_config: config.warc_source.clone(),
            warc_path,
        })
        .collect();

    let num_workers = usize::from(std::thread::available_parallelism()?).min(jobs.len());
    let mut handlers = Vec::new();
    let worker = Arc::new(StatsWorker::new(site_ids.clone()));

    for i in 0..num_workers {
        let jobs = jobs.clone();
        let worker = Arc::clone(&worker);

        handlers.push(std::thread::spawn(move || {
            for job in jobs.into_iter().skip(i).step_by(num_workers) {
                worker.process(job).unwrap();
            }
        }));
    }

    for handler in handlers {
        handler.join().unwrap();
    }

    let mut final_stats: Vec<_> = worker
        .stats
        .lock()
        .unwrap()
        .clone()
        .into_iter()
        .map(|(site, stats)| FinalSiteStats { site, stats })
        .collect();

    final_stats.sort_by(|a, b| b.stats.pages.cmp(&a.stats.pages));

    let writer = std::fs::File::create(&config.output_path)?;
    let writer = std::io::BufWriter::new(writer);

    serde_json::to_writer_pretty(writer, &final_stats)?;

    Ok(())
}
