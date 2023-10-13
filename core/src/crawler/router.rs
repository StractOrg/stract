use anyhow::Result;
use hashbrown::HashMap;
use rand::seq::SliceRandom;
use std::{net::SocketAddr, time::Duration};
use url::Url;

use crate::{
    crawler::{DomainCrawled, UrlToInsert},
    distributed::{retry_strategy::ExponentialBackoff, sonic},
    entrypoint::crawler::coordinator::{CoordinatorService, GetJobs, InsertUrls, MarkJobsComplete},
};

use super::{Domain, Job, JobResponse, MAX_URLS_FOR_DOMAIN_PER_INSERT, MAX_URL_LEN_BYTES};

struct RemoteCoordinator {
    addr: SocketAddr,
}

impl RemoteCoordinator {
    async fn conn(&self) -> Result<sonic::service::ResilientConnection<CoordinatorService>> {
        let retry = ExponentialBackoff::from_millis(1_000).with_limit(Duration::from_secs(10));

        Ok(sonic::service::ResilientConnection::create_with_timeout(
            self.addr,
            Duration::from_secs(60),
            retry,
        )
        .await?)
    }

    async fn sample_jobs(&self, num_jobs: usize) -> Result<Vec<Job>> {
        let conn = self.conn().await?;

        let response = conn
            .send_with_timeout(&GetJobs { num_jobs }, Duration::from_secs(90))
            .await?;

        Ok(response)
    }

    async fn insert_urls(&self, urls: HashMap<Domain, Vec<UrlToInsert>>) -> Result<()> {
        let conn = self.conn().await?;

        conn.send_with_timeout(&InsertUrls { urls }, Duration::from_secs(90))
            .await?;

        Ok(())
    }

    async fn mark_jobs_complete(&self, domains: Vec<DomainCrawled>) -> Result<()> {
        let conn = self.conn().await?;

        conn.send_with_timeout(&MarkJobsComplete { domains }, Duration::from_secs(90))
            .await?;

        Ok(())
    }
}

pub struct Router {
    coordinators: Vec<RemoteCoordinator>,
}

impl Router {
    pub async fn new(coordinator_addrs: Vec<SocketAddr>, seed_urls: Vec<String>) -> Result<Self> {
        let s = Self {
            coordinators: coordinator_addrs
                .into_iter()
                .map(|addr| RemoteCoordinator { addr })
                .collect(),
        };

        let mut coordinator_urls: HashMap<usize, HashMap<Domain, Vec<UrlToInsert>>> =
            HashMap::new();

        for url in seed_urls {
            let url = Url::parse(&url)?;
            let domain = Domain::from(&url);

            let coordinator_index = s.coordinator_index(&domain);

            coordinator_urls
                .entry(coordinator_index)
                .or_default()
                .entry(domain)
                .or_default()
                .push(UrlToInsert { url, weight: 0.0 });
        }

        let mut futures = Vec::new();
        for (coordinator_index, urls) in coordinator_urls {
            let coordinator = &s.coordinators[coordinator_index];

            futures.push(coordinator.insert_urls(urls));
        }

        futures::future::join_all(futures).await;

        Ok(s)
    }

    fn coordinator_index(&self, domain: &Domain) -> usize {
        let hash = md5::compute(domain.0.as_bytes());
        let hash = u128::from_le_bytes(hash.0) as usize;
        hash % self.coordinators.len()
    }

    pub async fn sample_jobs(&self, num_jobs: usize) -> Result<Vec<Job>> {
        let random_coordinator = self.coordinators.choose(&mut rand::thread_rng()).unwrap();

        random_coordinator.sample_jobs(num_jobs).await
    }

    pub async fn add_responses(&self, responses: &[JobResponse]) -> Result<()> {
        let mut domain_urls: HashMap<Domain, Vec<UrlToInsert>> = HashMap::new();
        let mut domain_budgets: Vec<DomainCrawled> = Vec::new();

        for res in responses {
            let mut urls: Vec<(Domain, Url)> = res
                .discovered_urls
                .iter()
                .map(|url| {
                    let domain = Domain::from(url);
                    (domain, url.clone())
                })
                .collect();

            let diff_domains = urls
                .iter()
                .filter(|(domain, _)| res.domain != *domain)
                .count() as f64;

            urls.sort_unstable_by(|(_, a), (_, b)| a.as_str().cmp(b.as_str()));
            urls.dedup_by(|(_, a), (_, b)| a.as_str() == b.as_str());

            let mut used_budget = 0.0;

            for (domain, url) in urls {
                if url.as_str().len() > MAX_URL_LEN_BYTES {
                    continue;
                }

                let different_domain = res.domain != domain;

                let weight = if different_domain {
                    (res.weight_budget / diff_domains).min(1.0)
                } else {
                    0.0
                };

                used_budget += weight;

                let urls = domain_urls.entry(domain).or_default();

                if urls.len() >= MAX_URLS_FOR_DOMAIN_PER_INSERT {
                    continue;
                }

                urls.push(UrlToInsert { url, weight });
            }

            domain_budgets.push(DomainCrawled {
                domain: res.domain.clone(),
                budget_used: used_budget,
            });
        }

        let mut coordinator_urls: HashMap<usize, HashMap<Domain, Vec<UrlToInsert>>> =
            HashMap::new();

        for (domain, urls) in domain_urls {
            let coordinator_index = self.coordinator_index(&domain);

            coordinator_urls
                .entry(coordinator_index)
                .or_default()
                .insert(domain, urls);
        }

        let mut futures = Vec::new();

        for (coordinator_index, urls) in coordinator_urls {
            let coordinator = &self.coordinators[coordinator_index];

            futures.push(coordinator.insert_urls(urls));
        }

        futures::future::join_all(futures).await;

        let mut futures = Vec::new();
        let mut coordinator_budgets: HashMap<usize, Vec<DomainCrawled>> = HashMap::new();

        for domain in domain_budgets {
            let coordinator_index = self.coordinator_index(&domain.domain);

            coordinator_budgets
                .entry(coordinator_index)
                .or_default()
                .push(domain);
        }

        for (coordinator_index, budgets) in coordinator_budgets {
            let coordinator = &self.coordinators[coordinator_index];

            futures.push(coordinator.mark_jobs_complete(budgets));
        }

        futures::future::join_all(futures).await;

        Ok(())
    }
}
