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
// along with this program.  If not, see <https://www.gnu.org/licenses/

use url::Url;

use crate::config::DailyLiveIndexCrawlerBudget;
use crate::entrypoint::site_stats::{FinalSiteStats, Site};
use crate::webgraph;
use crate::webpage::url_ext::UrlExt;
use crate::Result;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use super::SiteStats;

const MILLIS_PER_DAY: u64 = 24 * 60 * 60 * 1000;

fn assign_budgets(
    sites: &mut HashMap<Site, f64>,
    daily_budget: u64,
    total_centrality: f64,
    host_centrality: &speedy_kv::Db<webgraph::NodeID, f64>,
) {
    for (site, budget) in sites.iter_mut() {
        if let Some(centrality) = Url::robust_parse(site.as_str())
            .ok()
            .map(webgraph::Node::from)
            .map(|node| node.id())
            .and_then(|id| host_centrality.get(&id).ok().flatten())
        {
            *budget = (centrality * daily_budget as f64) / total_centrality;
        }
    }
}

fn calculate_total<'a>(
    sites: impl Iterator<Item = &'a FinalSiteStats>,
    host_centrality: &speedy_kv::Db<webgraph::NodeID, f64>,
) -> f64 {
    sites
        .filter_map(|site| Url::robust_parse(site.site().as_str()).ok())
        .map(webgraph::Node::from)
        .filter_map(|node| host_centrality.get(&node.id()).ok().flatten())
        .sum()
}

pub struct SiteBudgets {
    blogs: HashMap<Site, f64>,
    news: HashMap<Site, f64>,
    remaining: HashMap<Site, f64>,
}

impl SiteBudgets {
    pub fn new<P: AsRef<Path>>(
        host_centrality: P,
        stats: &SiteStats,
        daily_budget: DailyLiveIndexCrawlerBudget,
    ) -> Result<Self> {
        tracing::debug!("Assigning budgets");
        let host_centrality: speedy_kv::Db<webgraph::NodeID, f64> =
            speedy_kv::Db::open_or_create(host_centrality)?;

        let mut blogs = HashMap::new();
        let mut news = HashMap::new();
        let mut remaining = HashMap::new();

        for site in stats.blogs() {
            blogs.insert(site.site().clone(), 0.0);
        }

        for site in stats.news() {
            news.insert(site.site().clone(), 0.0);
        }

        for site in stats.all() {
            if blogs.contains_key(site.site()) || news.contains_key(site.site()) {
                continue;
            }

            remaining.insert(site.site().clone(), 0.0);
        }

        let blogs_total = calculate_total(stats.blogs(), &host_centrality);
        let news_total = calculate_total(stats.news(), &host_centrality);
        let remaining_total = calculate_total(
            stats
                .all()
                .filter(|site| !blogs.contains_key(site.site()) && !news.contains_key(site.site())),
            &host_centrality,
        );

        assign_budgets(
            &mut blogs,
            daily_budget.blogs,
            blogs_total,
            &host_centrality,
        );
        assign_budgets(&mut news, daily_budget.news, news_total, &host_centrality);
        assign_budgets(
            &mut remaining,
            daily_budget.remaining,
            remaining_total,
            &host_centrality,
        );

        tracing::debug!("Budgets assigned");
        tracing::debug!(
            "Number of sites with budgets: blogs: {}, news: {}, remaining: {}",
            blogs.len(),
            news.len(),
            remaining.len(),
        );
        Ok(Self {
            blogs,
            news,
            remaining,
        })
    }

    pub fn drip_rate(&self, site: &Site) -> Option<Duration> {
        let budget = *self
            .blogs
            .get(site)
            .or_else(|| self.news.get(site))
            .or_else(|| self.remaining.get(site))?;

        if budget == 0.0 {
            Some(Duration::from_millis(MILLIS_PER_DAY)) // once per day
        } else {
            Some(Duration::from_millis(MILLIS_PER_DAY / budget as u64))
        }
    }
}
