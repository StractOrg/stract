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
use hashbrown::HashSet;
use indicatif::ParallelProgressIterator;
use indicatif::ProgressIterator;
use itertools::Itertools;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::iter;
use std::{
    path::Path,
    sync::{atomic::AtomicUsize, Mutex},
};
use url::Url;

use crate::crawler::WeightedUrl;
use crate::webgraph::centrality::{top_nodes, TopNodes};
use crate::SortableFloat;
use crate::{
    config::CrawlPlannerConfig,
    crawler::{file_queue::FileQueueWriter, Job},
    kv::{rocksdb_store::RocksDbStore, Kv},
    webgraph::{NodeID, Webgraph},
};

use super::Domain;

const MAX_SURPLUS_BUDGET_ITERATIONS: usize = 100;

pub struct CrawlPlanner {
    host_centrality: RocksDbStore<NodeID, f64>,
    page_centrality: RocksDbStore<NodeID, f64>,
    host_graph: Webgraph,
    page_graph: Webgraph,
    config: CrawlPlannerConfig,
}

impl CrawlPlanner {
    pub fn new(
        host_centrality: RocksDbStore<NodeID, f64>,
        page_centrality: RocksDbStore<NodeID, f64>,
        host_graph: Webgraph,
        page_graph: Webgraph,
        config: CrawlPlannerConfig,
    ) -> Result<Self> {
        Self::check_config(&config)?;

        Ok(Self {
            host_centrality,
            page_centrality,
            host_graph,
            page_graph,
            config,
        })
    }

    fn all_pages(&self, host: NodeID) -> Vec<(NodeID, f64)> {
        self.page_graph
            .pages_by_host(&host)
            .into_iter()
            .map(|id| (id, self.page_centrality.get(&id).unwrap_or_default()))
            .collect::<Vec<_>>()
    }

    fn group_domain(&self, hosts: &[NodeID]) -> Vec<(Domain, Vec<NodeID>)> {
        let mut domains: HashMap<Domain, Vec<NodeID>> = HashMap::new();

        for host in hosts {
            let node = self.host_graph.id2node(host).unwrap();
            if let Ok(url) = Url::parse(&format!("http://{}", node.as_str())) {
                let domain = Domain::from(url);
                domains.entry(domain).or_default().push(*host);
            }
        }

        domains
            .into_iter()
            .sorted_by_cached_key(|(_, hosts)| {
                let s = hosts
                    .iter()
                    .map(|host| self.host_centrality.get(host).unwrap_or_default())
                    .sum::<f64>();

                SortableFloat::from(s)
            })
            .rev()
            .collect()
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

    fn prepare_job(
        &self,
        domain: Domain,
        hosts: &[NodeID],
        host_budgets: &BTreeMap<NodeID, u64>,
    ) -> (Job, DomainStats) {
        let mut total_wander_budget = 0;
        let mut total_schedule_budget = 0;
        let mut total_scheduled_urls = 0;
        let mut total_known_urls = 0;
        let mut urls = HashSet::new();

        for host in hosts {
            let mut pages = self.all_pages(*host);
            total_known_urls += pages.len();

            if pages.is_empty() {
                continue;
            }

            pages.sort_by(|(_, a), (_, b)| b.total_cmp(a));
            tracing::debug!("num pages: {}", pages.len());
            let host_budget = host_budgets.get(host).copied().unwrap_or_default();

            tracing::debug!("host_budget: {host_budget}");
            let schedule_budget = (host_budget as f64 * (1.0 - self.config.wander_fraction))
                .round()
                .max(0.0) as u64;

            tracing::debug!("schedule_budget: {schedule_budget}");
            let wander_budget = (host_budget as f64 * self.config.wander_fraction)
                .max(0.0)
                .round() as u64;
            tracing::debug!("wander_budget: {wander_budget}");

            total_wander_budget += wander_budget;
            total_schedule_budget += schedule_budget;
            let host_name = self.host_graph.id2node(host).unwrap().as_str().to_string();

            let before = urls.len();
            urls.extend(
                pages
                    .into_iter()
                    .filter_map(|(id, score)| self.page_graph.id2node(&id).map(|n| (n, score)))
                    .map(|(n, score)| (n.as_str().to_string(), score))
                    .filter_map(|(n, score)| {
                        Url::parse(&format!("http://{n}")).ok().map(|u| (u, score))
                    })
                    .map(|(url, score)| WeightedUrl { url, weight: score })
                    .take(schedule_budget as usize)
                    .chain(
                        iter::once_with(|| {
                            Some(WeightedUrl {
                                url: format!("http://{host_name}").parse().ok()?,
                                weight: 1.0,
                            })
                        })
                        .flatten(),
                    ),
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
            urls: urls
                .into_iter()
                .sorted_by(|a, b| b.weight.total_cmp(&a.weight))
                .collect(),
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

        (job, domain_stats)
    }

    fn assign_host_budgets(&self, hosts: &[NodeID]) -> BTreeMap<NodeID, u64> {
        let mut total_host_centrality = hosts
            .iter()
            .filter_map(|v| self.host_centrality.get(v))
            .sum::<f64>();

        let host_pages: BTreeMap<_, _> = hosts
            .par_iter()
            .progress_count(hosts.len() as u64)
            .map(|host| {
                let num_pages = self.page_graph.pages_by_host(host).len() as u64;
                (*host, num_pages)
            })
            .collect();

        let mut host_budgets: BTreeMap<_, _> = hosts
            .par_iter()
            .progress_count(hosts.len() as u64)
            .map(|host| {
                let host_centrality = self.host_centrality.get(host).unwrap_or_default();

                let host_budget = ((self.config.crawl_budget as f64 * host_centrality)
                    / total_host_centrality)
                    .round()
                    .max(0.0) as u64;

                let num_pages = host_pages.get(host).copied().unwrap_or_default();

                (*host, host_budget.min(num_pages))
            })
            .collect();

        let mut surplus_budget = self.config.crawl_budget as u64
            - host_budgets
                .values()
                .sum::<u64>()
                .min(self.config.crawl_budget as u64);

        // assign surplus budget
        let mut has_updated = true;
        let mut i = 0;
        while surplus_budget > 0 && has_updated && i < MAX_SURPLUS_BUDGET_ITERATIONS {
            tracing::info!("trying to schedule surplus budget: {}", surplus_budget);
            i += 1;

            has_updated = false;
            let mut new_surplus_budget = surplus_budget;
            let mut new_total_host_centrality = 0.0;

            for host in hosts
                .iter()
                .take(self.config.top_n_hosts_surplus)
                .progress()
            {
                if new_surplus_budget == 0 {
                    break;
                }

                let num_pages = host_pages.get(host).copied().unwrap_or_default();
                let host_budget = host_budgets.get_mut(host).unwrap();

                if num_pages > *host_budget {
                    let centrality = self.host_centrality.get(host).unwrap_or_default();
                    new_total_host_centrality += centrality;

                    let part = (centrality * surplus_budget as f64 / total_host_centrality)
                        .round()
                        .max(0.0) as u64;

                    let diff = num_pages - *host_budget;
                    let extra = part.min(diff);

                    if extra > 0 {
                        has_updated = true;
                    }

                    let extra = ((extra as f64 / (1.0 - self.config.wander_fraction)) as u64)
                        .min(new_surplus_budget);

                    *host_budget += extra;
                    new_surplus_budget -= extra;
                }
            }

            total_host_centrality = new_total_host_centrality;
            surplus_budget = new_surplus_budget;
        }

        tracing::info!("surplus done (remaining={})", surplus_budget);
        host_budgets
    }

    pub fn build<P: AsRef<Path>>(&self, output: P) -> Result<()> {
        if output.as_ref().exists() {
            return Err(anyhow!("output path already exists"));
        }

        let queue_path = output.as_ref().join("job_queue");
        std::fs::create_dir_all(&queue_path)?;

        let hosts = top_nodes(
            &self.host_centrality,
            TopNodes::Fraction(self.config.top_host_fraction),
        )
        .into_iter()
        .map(|(host, _)| host)
        .collect::<Vec<_>>();
        tracing::info!("found {} hosts", hosts.len());

        let grouped_hosts = self.group_domain(&hosts);
        let num_groups = grouped_hosts.len();

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

        let next_queue = AtomicUsize::new(0);

        let num_threads = self.config.num_threads.unwrap_or_else(num_cpus::get);

        let pool = ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .stack_size(80_000_000)
            .thread_name(move |num| format!("crawl-planner-{num}"))
            .build()?;

        pool.install(|| {
            let host_budgets = self.assign_host_budgets(&hosts);

            grouped_hosts
                .into_par_iter()
                .progress_count(num_groups as u64)
                .for_each(|(domain, hosts)| {
                    if domain.as_str().is_empty() {
                        return;
                    }

                    let (job, domain_stats) = self.prepare_job(domain, &hosts, &host_budgets);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        crawler::file_queue::FileQueue,
        executor::Executor,
        gen_temp_path,
        webgraph::{centrality::harmonic::HarmonicCentrality, Compression, Node, WebgraphWriter},
    };

    fn host_test_edges() -> Vec<(Node, Node, String)> {
        //     ┌────┐
        //     │    │
        // ┌───A◄─┐ │
        // │      │ │
        // ▼      │ │
        // B─────►C◄┘
        //        ▲
        //        │
        //        │
        //        D

        vec![
            (
                Node::from("http://a.com"),
                Node::from("http://b.com"),
                String::new(),
            ),
            (
                Node::from("http://b.com"),
                Node::from("http://c.com"),
                String::new(),
            ),
            (
                Node::from("http://a.com"),
                Node::from("http://c.com"),
                String::new(),
            ),
            (
                Node::from("http://c.com"),
                Node::from("http://a.com"),
                String::new(),
            ),
            (
                Node::from("http://d.com"),
                Node::from("http://c.com"),
                String::new(),
            ),
        ]
    }

    fn page_test_edges() -> Vec<(Node, Node, String)> {
        vec![
            (
                Node::from("http://a.com/"),
                Node::from("http://b.com/123"),
                String::new(),
            ),
            (
                Node::from("http://b.com/123"),
                Node::from("http://c.com/page"),
                String::new(),
            ),
            (
                Node::from("http://a.com/whut"),
                Node::from("http://c.com/page"),
                String::new(),
            ),
            (
                Node::from("http://c.com/"),
                Node::from("http://a.com/"),
                String::new(),
            ),
            (
                Node::from("http://d.com/321"),
                Node::from("http://c.com/page"),
                String::new(),
            ),
        ]
    }

    fn test_graph(edges: Vec<(Node, Node, String)>) -> Webgraph {
        let mut graph = WebgraphWriter::new(
            crate::gen_temp_path(),
            Executor::single_thread(),
            Compression::default(),
        );

        for (from, to, label) in edges {
            graph.insert(from, to, label);
        }

        graph.commit();

        graph.finalize()
    }

    fn test_plan() -> Vec<Job> {
        let host_graph = test_graph(host_test_edges());
        let page_graph = test_graph(page_test_edges());

        let centrality = HarmonicCentrality::calculate(&host_graph);
        let host_centrality = RocksDbStore::open(crate::gen_temp_path());

        for (node, score) in centrality.iter() {
            host_centrality.insert(*node, score);
        }
        host_centrality.flush();

        let centrality = HarmonicCentrality::calculate(&page_graph);
        let page_centrality = RocksDbStore::open(crate::gen_temp_path());

        for (node, score) in centrality.iter() {
            page_centrality.insert(*node, score);
        }
        page_centrality.flush();

        let planner = CrawlPlanner::new(
            host_centrality,
            page_centrality,
            host_graph,
            page_graph,
            CrawlPlannerConfig {
                crawl_budget: 100,
                top_host_fraction: 1.0,
                wander_fraction: 0.1,
                top_n_hosts_surplus: 2,
                num_job_queues: 1,
                page_harmonic_path: String::new(),
                host_harmonic_path: String::new(),
                page_graph_path: String::new(),
                host_graph_path: String::new(),
                output_path: String::new(),
                num_threads: Some(1),
            },
        )
        .unwrap();

        let planner_path = gen_temp_path();
        planner.build(planner_path.clone()).unwrap();

        let mut queue: FileQueue<Job> =
            FileQueue::open(planner_path.join("job_queue").join("0.queue")).unwrap();

        let mut jobs = Vec::new();

        while let Some(job) = queue.pop().unwrap() {
            jobs.push(job);
        }

        jobs
    }

    #[test]
    fn test_ordered_by_centralities() {
        let jobs = test_plan();

        assert_eq!(jobs.len(), 3);

        assert_eq!(jobs[0].domain.as_str(), "c.com");
        assert_eq!(jobs[1].domain.as_str(), "a.com");
        assert_eq!(jobs[2].domain.as_str(), "b.com");

        for job in jobs {
            let urls: Vec<_> = job.urls.iter().collect();

            assert!(urls.windows(2).all(|w| w[0].weight >= w[1].weight));
        }
    }

    #[test]
    fn test_contains_frontpage() {
        let jobs = test_plan();

        for job in jobs {
            let domain = job.domain.as_str();
            assert!(job.urls.iter().any(|url| {
                url.url.path() == "/"
                    && url.url.host_str() == Some(domain)
                    && url.url.query().is_none()
            }))
        }
    }
}
