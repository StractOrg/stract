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
use anyhow::{anyhow, Result};
use indicatif::ParallelProgressIterator;
use itertools::Itertools;
use rayon::{prelude::*, ThreadPoolBuilder};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::{
    path::Path,
    sync::{atomic::AtomicUsize, Mutex},
};
use url::Url;

use crate::config::WebgraphGranularity;
use crate::crawler::WeightedUrl;
use crate::distributed::cluster::Cluster;
use crate::webgraph::centrality::{top_nodes, TopNodes};
use crate::webgraph::remote::RemoteWebgraph;
use crate::webgraph::Node;
use crate::webpage::url_ext::UrlExt;
use crate::{
    config::CrawlPlannerConfig,
    crawler::{file_queue::FileQueueWriter, Job},
    webgraph::NodeID,
};

use super::Domain;

pub struct CrawlPlanner {
    host_centrality: speedy_kv::Db<NodeID, f64>,
    page_centrality: speedy_kv::Db<NodeID, f64>,
    page_graph: RemoteWebgraph,
    config: CrawlPlannerConfig,
}

impl CrawlPlanner {
    pub async fn new(
        host_centrality: speedy_kv::Db<NodeID, f64>,
        page_centrality: speedy_kv::Db<NodeID, f64>,
        cluster: Arc<Cluster>,
        config: CrawlPlannerConfig,
    ) -> Result<Self> {
        Self::check_config(&config)?;

        let page_graph = RemoteWebgraph::new(cluster, WebgraphGranularity::Page).await;

        Ok(Self {
            host_centrality,
            page_centrality,
            page_graph,
            config,
        })
    }

    fn check_config(config: &CrawlPlannerConfig) -> Result<()> {
        if !(0.0..=1.0).contains(&config.wander_fraction) {
            return Err(anyhow::anyhow!(
                "top_host_fraction must be in range [0.0, 1.0]"
            ));
        }

        Ok(())
    }

    fn prepare_job(
        &self,
        domain: Domain,
        pages: Vec<Url>,
        wander_budget: f64,
        total_host_centralities: f64,
    ) -> (Job, DomainStats) {
        let hosts = pages
            .iter()
            .map(|url| Node::from(url).into_host())
            .unique()
            .collect::<Vec<_>>();

        let host_centrality: f64 = hosts
            .iter()
            .map(|host| host.id())
            .map(|id| self.host_centrality.get(&id).unwrap().unwrap_or_default())
            .sum();

        let wander_budget = (wander_budget * (host_centrality / total_host_centralities))
            .max(1.0)
            .round() as u64;

        let mut urls: Vec<_> = pages
            .into_iter()
            .chain(
                hosts
                    .into_iter()
                    .map(|host| Url::parse(&format!("https://{}", host.as_str())).unwrap()),
            )
            .unique()
            .map(|url| WeightedUrl {
                url: url.clone(),
                weight: self
                    .page_centrality
                    .get(&Node::from(url).id())
                    .unwrap()
                    .unwrap_or_default(),
            })
            .collect();

        urls.sort_by(|a, b| b.weight.total_cmp(&a.weight));
        urls.reverse();

        let job = Job {
            domain: domain.clone(),
            urls: urls.into_iter().collect(),
            wandering_urls: wander_budget,
        };

        let domain_stats = DomainStats {
            domain,
            known_urls: job.urls.len(),
            num_hosts: job
                .urls
                .iter()
                .map(|url| url.url.host_str())
                .unique()
                .count(),
            scheduled_urls: job.urls.len() as u64,
            wander_budget,
        };

        (job, domain_stats)
    }

    async fn pages_to_crawl(&self) -> Vec<Url> {
        let page_ids = top_nodes(
            &self.page_centrality,
            TopNodes::Fraction(
                self.config.crawl_budget as f64 * (1.0 - self.config.wander_fraction),
            ),
        )
        .into_iter()
        .map(|(page, _)| page)
        .collect::<Vec<_>>();

        let mut pages = Vec::with_capacity(page_ids.len());

        for chunk in page_ids.into_iter().chunks(1024).into_iter() {
            let nodes = chunk.collect::<Vec<_>>();
            let nodes = self.page_graph.batch_get_node(&nodes).await.unwrap();
            pages.extend(
                nodes
                    .into_iter()
                    .flatten()
                    .map(|n| Url::parse(n.as_str()).unwrap()),
            );
        }

        pages
    }

    pub async fn build<P: AsRef<Path>>(&self, output: P) -> Result<()> {
        if output.as_ref().exists() {
            return Err(anyhow!("output path already exists"));
        }

        let queue_path = output.as_ref().join("job_queue");
        std::fs::create_dir_all(&queue_path)?;

        tracing::info!("getting pages to crawl");

        let pages = self.pages_to_crawl().await;

        let mut grouped_pages = BTreeMap::new();

        for page in pages {
            if let Some(domain) = page.icann_domain() {
                grouped_pages
                    .entry(Domain::from(domain.to_string()))
                    .or_insert_with(Vec::new)
                    .push(page);
            }
        }

        let job_queues: Vec<Mutex<FileQueueWriter<Job>>> = (0..self.config.num_job_queues)
            .map(|i| {
                let path = queue_path.join(format!("{}.queue", i));
                FileQueueWriter::new(path)
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(Mutex::new)
            .collect();

        let stats = Mutex::new(Vec::new());

        let pool = ThreadPoolBuilder::new()
            .num_threads(usize::from(std::thread::available_parallelism().unwrap()))
            .stack_size(80_000_000)
            .thread_name(move |num| format!("crawl-planner-{num}"))
            .build()?;

        pool.install(|| {
            let total_host_centralities: f64 = grouped_pages
                .par_iter()
                .map(|(_, pages)| {
                    pages
                        .iter()
                        .map(|url| Node::from(url).into_host().id())
                        .unique()
                        .map(|id| self.host_centrality.get(&id).unwrap().unwrap_or_default())
                        .sum::<f64>()
                })
                .sum();

            let wander_budget = self.config.crawl_budget as f64 * self.config.wander_fraction;

            let next_queue = AtomicUsize::new(0);

            let num_groups = grouped_pages.len();

            grouped_pages
                .into_par_iter()
                .progress_count(num_groups as u64)
                .for_each(|(domain, pages)| {
                    if domain.as_str().is_empty() {
                        return;
                    }

                    let (job, domain_stats) =
                        self.prepare_job(domain, pages, wander_budget, total_host_centralities);

                    stats
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(domain_stats);

                    // `fetch_add` wraps around on overflow
                    let queue_index = next_queue.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    job_queues[queue_index % job_queues.len()]
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(job)
                        .unwrap();
                })
        });

        for queue in job_queues {
            queue
                .into_inner()
                .unwrap_or_else(|e| e.into_inner())
                .finalize()?;
        }

        let mut stats = stats.into_inner().unwrap_or_else(|e| e.into_inner());
        stats.sort_by_key(|b| std::cmp::Reverse(b.schedule_budget()));

        let total_scheduled_urls: u64 = stats.iter().map(|d| d.scheduled_urls).sum();
        let total_wander_budget: u64 = stats.iter().map(|d| d.wander_budget).sum();

        let metadata = Metadata {
            stats,
            total_scheduled_urls,
            total_wander_budget,
        };

        tracing::info!("total scheduled urls: {}", metadata.total_scheduled_urls);

        tracing::info!("total wander budget: {}", metadata.total_wander_budget);

        let metadata_path = output.as_ref().join("metadata.json");

        let metadata_file = std::fs::File::create(metadata_path)?;
        serde_json::to_writer_pretty(metadata_file, &metadata)?;

        Ok(())
    }
}

#[derive(serde::Serialize)]
struct DomainStats {
    domain: Domain,
    known_urls: usize,
    num_hosts: usize,
    scheduled_urls: u64,
    wander_budget: u64,
}

impl DomainStats {
    pub fn schedule_budget(&self) -> u64 {
        self.scheduled_urls + self.wander_budget
    }
}

#[derive(serde::Serialize)]
struct Metadata {
    total_scheduled_urls: u64,
    total_wander_budget: u64,
    stats: Vec<DomainStats>,
}
