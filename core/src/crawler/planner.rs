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
use indicatif::ProgressIterator;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::{
    collections::VecDeque,
    path::Path,
    sync::{atomic::AtomicUsize, Mutex},
};
use url::Url;

use crate::webgraph::centrality::{top_hosts, TopHosts};
use crate::{
    config::CrawlPlannerConfig,
    crawler::{file_queue::FileQueueWriter, Job},
    kv::{rocksdb_store::RocksDbStore, Kv},
    webgraph::{NodeID, Webgraph},
};

use super::Domain;

const MAX_SURPLUS_BUDGET_ITERATIONS: usize = 100;

fn all_pages(
    page_centrality: &RocksDbStore<NodeID, f64>,
    page_graph: &Webgraph,
    host: NodeID,
) -> Vec<(NodeID, f64)> {
    page_graph
        .pages_by_host(&host)
        .into_iter()
        .map(|id| (id, page_centrality.get(&id).unwrap_or_default()))
        .collect::<Vec<_>>()
}

fn group_domain(hosts: &[NodeID], host_graph: &Webgraph) -> HashMap<Domain, Vec<NodeID>> {
    let mut domains: HashMap<Domain, Vec<NodeID>> = HashMap::new();

    for host in hosts {
        let node = host_graph.id2node(host).unwrap();
        if let Ok(url) = Url::parse(&format!("http://{}", node.name)) {
            let domain = Domain::from(url);
            domains.entry(domain).or_default().push(*host);
        }
    }

    domains
}

fn check_config(config: &CrawlPlannerConfig) -> Result<()> {
    if !(0.0..=1.0).contains(&config.wander_fraction) {
        return Err(anyhow::anyhow!(
            "top_host_fraction must be in range [0.0, 1.0]"
        ));
    }

    if !(0.0..=1.0).contains(&config.top_host_fraction) {
        return Err(anyhow::anyhow!(
            "top_host_fraction must be in range [0.0, 1.0]"
        ));
    }

    Ok(())
}

pub fn make_crawl_plan<P: AsRef<Path>>(
    host_centrality: RocksDbStore<NodeID, f64>,
    page_centrality: RocksDbStore<NodeID, f64>,
    host_graph: Webgraph,
    page_graph: Webgraph,
    config: CrawlPlannerConfig,
    output: P,
) -> Result<()> {
    check_config(&config)?;

    if output.as_ref().exists() {
        return Err(anyhow!("output path already exists"));
    }

    let queue_path = output.as_ref().join("job_queue");
    std::fs::create_dir_all(&queue_path)?;

    let hosts = top_hosts(
        &host_centrality,
        TopHosts::Fraction(config.top_host_fraction),
    );
    let num_hosts = hosts.len();
    tracing::info!("found {} hosts", num_hosts);

    let mut total_host_centrality = hosts
        .iter()
        .filter_map(|v| host_centrality.get(v))
        .sum::<f64>();
    let grouped = group_domain(&hosts, &host_graph);
    let num_groups = grouped.len();

    let job_queues: Vec<Mutex<FileQueueWriter<Job>>> = (0..config.num_job_queues)
        .map(|i| {
            let path = queue_path.join(format!("{}.queue", i));
            FileQueueWriter::new(path)
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(Mutex::new)
        .collect();

    let stats = Mutex::new(Vec::new());

    let next_queue = AtomicUsize::new(0);

    let pool = ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .stack_size(80_000_000)
        .thread_name(move |num| format!("crawl-planner-{num}"))
        .build()?;

    pool.install(|| {
        let host_pages: BTreeMap<_, _> = hosts
            .par_iter()
            .progress_count(num_hosts as u64)
            .map(|host| {
                let num_pages = page_graph.pages_by_host(host).len() as u64;
                (*host, num_pages)
            })
            .collect();

        let mut host_budgets: BTreeMap<_, _> = hosts
            .par_iter()
            .progress_count(num_hosts as u64)
            .map(|host| {
                let host_centrality = host_centrality.get(host).unwrap_or_default();

                let host_budget = ((config.crawl_budget as f64 * host_centrality)
                    / total_host_centrality)
                    .round()
                    .max(0.0) as u64;

                let num_pages = host_pages.get(host).copied().unwrap_or_default();

                (*host, host_budget.min(num_pages as u64))
            })
            .collect();

        let mut surplus_budget = config.crawl_budget as u64
            - host_budgets
                .values()
                .sum::<u64>()
                .min(config.crawl_budget as u64);

        let mut has_updated = true;
        let mut i = 0;
        while surplus_budget > 0 && has_updated && i < MAX_SURPLUS_BUDGET_ITERATIONS {
            tracing::info!("trying to schedule surplus budget: {}", surplus_budget);
            i += 1;

            has_updated = false;
            let mut new_surplus_budget = surplus_budget;
            let mut new_total_host_centrality = 0.0;

            for host in hosts.iter().take(config.top_n_hosts_surplus).progress() {
                if new_surplus_budget == 0 {
                    break;
                }

                let num_pages = host_pages.get(host).copied().unwrap_or_default();
                let host_budget = host_budgets.get_mut(host).unwrap();

                if num_pages > *host_budget {
                    let centrality = host_centrality.get(host).unwrap_or_default();
                    new_total_host_centrality += centrality;

                    let part = (centrality * surplus_budget as f64 / total_host_centrality)
                        .round()
                        .max(0.0) as u64;

                    let diff = num_pages - *host_budget;
                    let extra = part.min(diff);

                    if extra > 0 {
                        has_updated = true;
                    }

                    let extra = ((extra as f64 / (1.0 - config.wander_fraction)) as u64)
                        .min(new_surplus_budget);

                    *host_budget += extra;
                    new_surplus_budget -= extra;
                }
            }

            total_host_centrality = new_total_host_centrality;
            surplus_budget = new_surplus_budget;
        }

        tracing::info!("surplus done (remaining={})", surplus_budget);

        grouped
            .into_par_iter()
            .progress_count(num_groups as u64)
            .for_each(|(domain, hosts)| {
                if domain.as_str().is_empty() {
                    return;
                }

                let mut total_wander_budget = 0;
                let mut total_schedule_budget = 0;
                let mut total_scheduled_urls = 0;
                let mut total_known_urls = 0;
                let mut urls = VecDeque::new();

                for host in &hosts {
                    let mut pages = all_pages(&page_centrality, &page_graph, *host);
                    total_known_urls += pages.len();

                    if pages.is_empty() {
                        continue;
                    }

                    pages.sort_by(|(_, a), (_, b)| b.total_cmp(a));
                    tracing::debug!("num pages: {}", pages.len());
                    let host_budget = host_budgets.get(host).copied().unwrap_or_default();

                    tracing::debug!("host_budget: {host_budget}");
                    let schedule_budget = (host_budget as f64 * (1.0 - config.wander_fraction))
                        .round()
                        .max(0.0) as u64;

                    tracing::debug!("schedule_budget: {schedule_budget}");
                    let wander_budget = (host_budget as f64 * config.wander_fraction)
                        .max(0.0)
                        .round() as u64;
                    tracing::debug!("wander_budget: {wander_budget}");

                    total_wander_budget += wander_budget;
                    total_schedule_budget += schedule_budget;

                    let before = urls.len();
                    urls.extend(
                        pages
                            .into_iter()
                            .map(|(id, _)| id)
                            .filter_map(|id| page_graph.id2node(&id))
                            .map(|n| n.name)
                            .filter_map(|n| Url::parse(&format!("http://{n}")).ok())
                            .take(schedule_budget as usize),
                    );

                    total_scheduled_urls += urls.len() as u64 - before as u64;
                }

                tracing::trace!(
                    "domain: {:#?} hosts: {:#?} urls: {:#?}",
                    domain,
                    hosts,
                    urls
                );

                let job = Job {
                    domain: domain.clone(),
                    urls,
                    wandering_urls: total_wander_budget,
                };

                let domain_stats = DomainStats {
                    domain,
                    num_hosts: hosts.len(),
                    schedule_budget: total_schedule_budget,
                    wander_budget: total_wander_budget,
                    scheduled_urls: total_scheduled_urls,
                    known_urls: total_known_urls,
                };
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

    let mut metadata = Metadata {
        stats: stats.into_inner().unwrap_or_else(|e| e.into_inner()),
    };

    tracing::info!(
        "total scheduled urls: {}",
        metadata.stats.iter().map(|d| d.scheduled_urls).sum::<u64>()
    );

    tracing::info!(
        "total wander budget: {}",
        metadata.stats.iter().map(|d| d.wander_budget).sum::<u64>()
    );

    metadata
        .stats
        .sort_by(|a, b| b.schedule_budget.cmp(&a.schedule_budget));

    let metadata_path = output.as_ref().join("metadata.json");

    let metadata_file = std::fs::File::create(metadata_path)?;
    serde_json::to_writer_pretty(metadata_file, &metadata)?;

    Ok(())
}

#[derive(serde::Serialize)]
struct DomainStats {
    domain: Domain,
    known_urls: usize,
    num_hosts: usize,
    schedule_budget: u64,
    scheduled_urls: u64,
    wander_budget: u64,
}

#[derive(serde::Serialize)]
struct Metadata {
    stats: Vec<DomainStats>,
}
