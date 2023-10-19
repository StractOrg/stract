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
use kv::{rocksdb_store::RocksDbStore, Kv};
use rayon::prelude::*;
use std::collections::HashMap;
use std::{
    collections::VecDeque,
    path::Path,
    sync::{atomic::AtomicUsize, Mutex},
};
use url::Url;
use webgraph::{
    centrality::{top_hosts, TopHosts},
    NodeID, Webgraph,
};

use crate::{
    config::CrawlPlannerConfig,
    crawler::{file_queue::FileQueueWriter, Job},
};

use super::Domain;

fn all_pages(
    page_centrality: &RocksDbStore<NodeID, f64>,
    page_graph: &Webgraph,
    host: NodeID,
) -> Vec<(NodeID, f64)> {
    page_graph
        .raw_ingoing_edges_by_host(&host)
        .into_iter()
        .map(|e| e.to)
        .map(|id| (id, page_centrality.get(&id).unwrap_or_default()))
        .collect::<Vec<_>>()
}

fn group_domain(hosts: Vec<NodeID>, host_graph: &Webgraph) -> HashMap<Domain, Vec<NodeID>> {
    let mut domains: HashMap<Domain, Vec<NodeID>> = HashMap::new();

    for host in hosts {
        let node = host_graph.id2node(&host).unwrap();
        if let Ok(url) = Url::parse(&format!("http://{}", node.name)) {
            let domain = Domain::from(url);
            domains.entry(domain).or_default().push(host);
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
    tracing::debug!("generating for {} hosts", hosts.len());
    let grouped = group_domain(hosts, &host_graph);

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

    grouped.into_par_iter().for_each(|(domain, hosts)| {
        let mut total_wander_budget = 0;
        let mut total_schedule_budget = 0;
        let mut total_scheduled_urls = 0;
        let mut urls = VecDeque::new();

        for host in hosts {
            let mut pages = all_pages(&page_centrality, &page_graph, host);
            pages.sort_by(|(_, a), (_, b)| b.total_cmp(a));
            tracing::debug!("num pages: {}", pages.len());
            let host_centrality = host_centrality.get(&host).unwrap_or_default();

            let host_budget = (config.crawl_budget as f64 * host_centrality).max(1.0) as u64;
            tracing::debug!("host_budget: {host_budget}");
            let schedule_budget =
                (host_budget as f64 * (1.0 - config.wander_fraction)).max(1.0) as u64;
            tracing::debug!("schedule_budget: {schedule_budget}");
            let wander_budget = (host_budget as f64 * config.wander_fraction).max(0.0) as u64;
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

        let job = Job {
            domain: domain.clone(),
            urls,
            wandering_urls: total_wander_budget,
        };

        let domain_stats = DomainStats {
            domain,
            schedule_budget: total_schedule_budget,
            wander_budget: total_wander_budget,
            scheduled_urls: total_scheduled_urls,
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
    });

    for queue in job_queues {
        queue
            .into_inner()
            .unwrap_or_else(|e| e.into_inner())
            .finalize()?;
    }

    let metadata = Metadata {
        stats: stats.into_inner().unwrap_or_else(|e| e.into_inner()),
    };

    let metadata_path = output.as_ref().join("metadata.json");

    let metadata_file = std::fs::File::create(metadata_path)?;
    serde_json::to_writer_pretty(metadata_file, &metadata)?;

    Ok(())
}

#[derive(serde::Serialize)]
struct DomainStats {
    domain: Domain,
    schedule_budget: u64,
    scheduled_urls: u64,
    wander_budget: u64,
}

#[derive(serde::Serialize)]
struct Metadata {
    stats: Vec<DomainStats>,
}
