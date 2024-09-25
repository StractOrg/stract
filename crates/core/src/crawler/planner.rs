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
use futures::stream::FuturesOrdered;
use futures::StreamExt;
use indicatif::ProgressIterator;
use itertools::Itertools;
use rayon::{prelude::*, ThreadPoolBuilder};
use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::{
    path::Path,
    sync::{atomic::AtomicUsize, Mutex},
};
use url::Url;

use crate::crawler::WeightedUrl;
use crate::distributed::cluster::Cluster;
use crate::external_sort::ExternalSorter;
use crate::webgraph::remote::{Host, Page, RemoteWebgraph};
use crate::webgraph::Node;
use crate::webpage::url_ext::UrlExt;
use crate::SortableFloat;
use crate::{
    config::CrawlPlannerConfig,
    crawler::{file_queue::FileQueueWriter, Job},
    webgraph::NodeID,
};

use super::Domain;

const MAX_UNCOMMITTED_INSERTS_PER_GROUP: usize = 50_000;
const NUM_GROUPS: usize = 1024;
const CONCURRENCY_LIMIT: usize = 32;

#[derive(bincode::Encode, bincode::Decode, Clone, Debug, PartialEq, Eq, Hash)]
struct StoredUrl(#[bincode(with_serde)] Url);

impl StoredUrl {
    pub fn icann_domain(&self) -> Option<&str> {
        self.0.icann_domain()
    }
}

impl From<Url> for StoredUrl {
    fn from(url: Url) -> Self {
        Self(url)
    }
}

impl From<StoredUrl> for Url {
    fn from(url: StoredUrl) -> Self {
        url.0
    }
}

struct UrlGrouper {
    groups: Vec<speedy_kv::Db<StoredUrl, ()>>,
    folder: std::path::PathBuf,
}

impl UrlGrouper {
    pub fn new<P>(output_path: P, num_groups: usize) -> Self
    where
        P: AsRef<std::path::Path>,
    {
        let folder = output_path.as_ref().join("groups").to_path_buf();

        Self {
            groups: (0..num_groups)
                .map(|g| {
                    speedy_kv::Db::open_or_create(folder.as_path().join(format!("{g}"))).unwrap()
                })
                .collect(),
            folder,
        }
    }

    fn group(&mut self, url: &StoredUrl) -> usize {
        let domain = url.icann_domain().unwrap_or_default();

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        domain.hash(&mut hasher);

        hasher.finish() as usize % self.groups.len()
    }

    pub fn insert(&mut self, url: Url) {
        let url = StoredUrl::from(url);
        let group = self.group(&url);
        let group = &mut self.groups[group];
        group.insert(url, ()).unwrap();

        if group.uncommitted_inserts() > MAX_UNCOMMITTED_INSERTS_PER_GROUP {
            group.commit().unwrap();
        }
    }

    pub fn into_groups(mut self) -> Vec<speedy_kv::Db<StoredUrl, ()>> {
        for group in &mut self.groups {
            group.commit().unwrap();
        }

        self.groups
    }
}

impl std::fmt::Debug for UrlGrouper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UrlGrouper")
            .field("num_groups", &self.groups.len())
            .field("folder", &self.folder)
            .finish()
    }
}

fn sorted_centralities(
    it: impl Iterator<Item = (NodeID, f64)>,
) -> impl Iterator<Item = (NodeID, f64)> {
    ExternalSorter::new()
        .with_chunk_size(100_000_000)
        .sort(it.map(|(node_id, centrality)| (Reverse(SortableFloat(centrality)), node_id)))
        .unwrap()
        .map(|(Reverse(SortableFloat(centrality)), node_id)| (node_id, centrality))
}

#[derive(Debug, Clone)]
struct Budget {
    remaining_schedulable: u64,
}

pub struct CrawlPlanner {
    host_centrality: speedy_kv::Db<NodeID, f64>,
    page_centrality: speedy_kv::Db<NodeID, f64>,
    page_graph: RemoteWebgraph<Page>,
    host_graph: RemoteWebgraph<Host>,
    config: CrawlPlannerConfig,

    excluded_domains: HashSet<Domain>,

    domain_boosts: HashMap<Domain, f64>,
}

impl CrawlPlanner {
    pub async fn new(
        host_centrality: speedy_kv::Db<NodeID, f64>,
        page_centrality: speedy_kv::Db<NodeID, f64>,
        cluster: Arc<Cluster>,
        config: CrawlPlannerConfig,
    ) -> Result<Self> {
        Self::check_config(&config)?;

        let page_graph = RemoteWebgraph::new(cluster.clone()).await;
        page_graph.await_ready().await;
        let host_graph = RemoteWebgraph::new(cluster.clone()).await;
        host_graph.await_ready().await;

        let domain_boosts = config
            .domain_boosts
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|domain_boost| (Domain(domain_boost.domain), domain_boost.boost))
            .collect();

        Ok(Self {
            host_centrality,
            page_centrality,
            page_graph,
            host_graph,
            domain_boosts,
            excluded_domains: config
                .excluded_domains
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(Domain)
                .collect(),
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

        let wander_budget = ((wander_budget * host_centrality) / total_host_centralities).max(1.0);

        let boost = *self.domain_boosts.get(&domain).unwrap_or(&1.0);
        let wander_budget = boost * wander_budget;
        let wander_budget = wander_budget.round() as u64;

        let mut urls: Vec<_> = pages
            .into_iter()
            .chain(
                hosts
                    .into_iter()
                    .filter_map(|host| Url::parse(&format!("https://{}", host.as_str())).ok()),
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

        urls.sort_by(|a, b| a.weight.total_cmp(&b.weight));
        urls.reverse();

        let job = Job {
            domain: domain.clone(),
            urls: urls.into_iter().collect(),
            wandering_urls: wander_budget,
        };

        let domain_stats = DomainStats {
            domain,
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

    fn assign_budgets(&self) -> BTreeMap<NodeID, Budget> {
        let total_crawl_budget = self.config.crawl_budget as f64;
        let num_hosts = self.host_centrality.len();
        let num_hosts = ((num_hosts as f64) * self.config.top_host_fraction).ceil() as usize;

        let crawl_budget_top_hosts_boost = total_crawl_budget
            * self
                .config
                .top_hosts_budget_boost
                .as_ref()
                .map(|top_hosts_budget_boost| top_hosts_budget_boost.reserved_budget_fraction)
                .unwrap_or_default();
        let total_crawl_budget = total_crawl_budget - crawl_budget_top_hosts_boost;

        let mut prev_sum = 0.0;
        let total_host_centrality = sorted_centralities(self.host_centrality.iter())
            .map(|(_, centrality)| centrality)
            .take_while(|centrality| {
                let sum = prev_sum + centrality;
                prev_sum = sum;
                // take as long as budget * c_i / S(c_i) > 0.5
                // as this will include exactly the hosts where
                // we crawl at least one page.
                // S(c_i) denotes the sum of all centralities
                // up to and including c_i
                2.0 * total_crawl_budget * centrality > sum
            })
            .take(num_hosts)
            .sum::<f64>();

        let top_hosts_total_centrality = self
            .config
            .top_hosts_budget_boost
            .as_ref()
            .map(|top_hosts_budget_boost| {
                sorted_centralities(self.host_centrality.iter())
                    .take(top_hosts_budget_boost.top_hosts)
                    .map(|(_, centrality)| centrality)
                    .sum::<f64>()
            })
            .unwrap_or_default();

        sorted_centralities(self.host_centrality.iter())
            .take(num_hosts)
            .enumerate()
            .filter_map(|(rank, (id, centrality))| {
                let mut total =
                    ((total_crawl_budget * centrality) / total_host_centrality).ceil() as u64;

                if let Some(top_hosts_budget_boost) = &self.config.top_hosts_budget_boost {
                    if rank < top_hosts_budget_boost.top_hosts {
                        total += ((crawl_budget_top_hosts_boost * centrality)
                            / top_hosts_total_centrality)
                            .round() as u64;
                    }
                }

                let budget = Budget {
                    remaining_schedulable: (total as f64 * (1.0 - self.config.wander_fraction))
                        .ceil() as u64,
                };

                if budget.remaining_schedulable > 0 {
                    Some((id, budget))
                } else {
                    None
                }
            })
            .collect()
    }

    async fn pages_to_crawl(&self) -> (Vec<speedy_kv::Db<StoredUrl, ()>>, u64) {
        let mut budgets = self.assign_budgets();

        let mut grouper = UrlGrouper::new(&self.config.output_path, NUM_GROUPS);
        let mut futures = FuturesOrdered::new();

        let num_pages = self.page_centrality.len();
        for chunk in sorted_centralities(self.page_centrality.iter())
            .map(|(id, _)| id)
            .progress_count(num_pages as u64)
            .chunks(128)
            .into_iter()
        {
            while futures.len() >= CONCURRENCY_LIMIT {
                let nodes: Vec<Option<Node>> = futures.next().await.unwrap();
                add_nodes_to_grouper(nodes.into_iter().flatten(), &mut grouper, &mut budgets);
            }

            if budgets.is_empty() {
                break;
            }

            let nodes = chunk.collect::<Vec<_>>();
            let page_graph = self.page_graph.clone();

            futures.push_back(async move { page_graph.batch_get_node(&nodes).await.unwrap() });
        }

        while let Some(nodes) = futures.next().await {
            add_nodes_to_grouper(nodes.into_iter().flatten(), &mut grouper, &mut budgets);
        }

        // make sure frontpage is added for all hosts with a budget.
        // this is necessary to ensure that we crawl at least one page
        // for each host.
        let mut futures = FuturesOrdered::new();
        let num_missing_budgets = budgets.len();
        let missing_budgets = budgets.clone();
        for chunk in missing_budgets
            .keys()
            .copied()
            .progress_count(num_missing_budgets as u64)
            .chunks(128)
            .into_iter()
        {
            while futures.len() >= CONCURRENCY_LIMIT {
                let nodes: Vec<Option<Node>> = futures.next().await.unwrap();

                let nodes = nodes.into_iter().flatten().filter_map(|node| {
                    if let Ok(url) = Url::parse(&format!("https://{}", node.as_str())) {
                        Some(Node::from(url))
                    } else {
                        None
                    }
                });

                add_nodes_to_grouper(nodes, &mut grouper, &mut budgets);
            }

            let nodes = chunk.collect::<Vec<_>>();
            let host_graph = self.host_graph.clone();

            futures.push_back(async move { host_graph.batch_get_node(&nodes).await.unwrap() });
        }

        while let Some(nodes) = futures.next().await {
            add_nodes_to_grouper(nodes.into_iter().flatten(), &mut grouper, &mut budgets);
        }

        let missing_budgets = budgets
            .values()
            .map(|budget| budget.remaining_schedulable)
            .sum::<u64>();

        (grouper.into_groups(), missing_budgets)
    }

    pub async fn build(self) -> Result<()> {
        let output = Path::new(&self.config.output_path);

        if output.exists() {
            return Err(anyhow!("output path already exists"));
        }

        let queue_path = output.join("job_queue");
        std::fs::create_dir_all(&queue_path)?;

        tracing::info!("getting pages to crawl");

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

        let (groups, missing_budgets) = self.pages_to_crawl().await;

        tracing::info!("grouping pages by domain");

        for group in groups.into_iter().progress() {
            let mut grouped_pages = BTreeMap::new();

            for page in group.iter().map(|(url, _)| url).map(Url::from) {
                if let Some(domain) = page.icann_domain() {
                    grouped_pages
                        .entry(Domain::from(domain.to_string()))
                        .or_insert_with(Vec::new)
                        .push(page);
                }
            }

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

                let total_host_centralities = total_host_centralities * NUM_GROUPS as f64;

                let wander_budget = self.config.crawl_budget as f64 * self.config.wander_fraction
                    + missing_budgets as f64;

                let next_queue = AtomicUsize::new(0);

                grouped_pages.into_par_iter().for_each(|(domain, pages)| {
                    if domain.as_str().is_empty() || self.excluded_domains.contains(&domain) {
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
        }

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
        let total_num_hosts: u64 = stats.iter().map(|d| d.num_hosts as u64).sum();
        let total_num_domains: u64 = stats.len() as u64;

        let metadata = Metadata {
            stats,
            total_scheduled_urls,
            total_wander_budget,
            total_num_hosts,
            total_num_domains,
        };

        tracing::info!("total scheduled urls: {}", metadata.total_scheduled_urls);

        tracing::info!("total wander budget: {}", metadata.total_wander_budget);

        let metadata_path = output.join("metadata.json");

        let metadata_file = std::fs::File::create(metadata_path)?;
        serde_json::to_writer_pretty(metadata_file, &metadata)?;

        std::fs::remove_dir_all(output.join("groups"))?;

        Ok(())
    }
}

fn add_nodes_to_grouper(
    nodes: impl Iterator<Item = Node>,
    grouper: &mut UrlGrouper,
    budgets: &mut BTreeMap<NodeID, Budget>,
) {
    for node in nodes {
        let url = Url::parse(&format!("https://{}", node.as_str()));
        let host = node.into_host();

        if let Some(budget) = budgets.get_mut(&host.id()) {
            if budget.remaining_schedulable == 0 {
                continue;
            }

            if let Ok(url) = url {
                budget.remaining_schedulable -= 1;
                grouper.insert(url);

                if budget.remaining_schedulable == 0 {
                    budgets.remove(&host.id());
                }
            }
        }
    }
}

#[derive(serde::Serialize)]
struct DomainStats {
    domain: Domain,
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
    total_num_hosts: u64,
    total_num_domains: u64,
    stats: Vec<DomainStats>,
}
