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

use std::time::{Duration, Instant};

use crate::{
    entrypoint::site_stats::{self, FinalSiteStats},
    live_index::crawler::checker::{Feeds, Frontpage, Sitemap},
    Result,
};

use super::{
    checker::{CheckIntervals, Checker},
    Client,
};

pub const MAX_PENDING_BUDGET: u64 = 128;

pub struct CrawlableSite {
    site: site_stats::Site,
    feeds: Feeds,
    sitemap: Sitemap,
    frontpage: Frontpage,
    last_drip: Instant,
    drip_rate: Duration,
    budget: u64,
}

impl CrawlableSite {
    pub fn new(site: FinalSiteStats, drip_rate: Duration) -> Result<Self> {
        Ok(Self {
            site: site.site().clone(),
            feeds: Feeds::new(
                site.stats()
                    .feeds
                    .clone()
                    .into_iter()
                    .map(|feed| feed.into())
                    .collect(),
            ),
            sitemap: Sitemap::new(site.site())?,
            frontpage: Frontpage::new(site.site())?,
            last_drip: Instant::now(),
            drip_rate,
            budget: 0,
        })
    }
    pub fn drip(&mut self) {
        let pages_to_drip =
            (self.last_drip.elapsed().as_secs_f32() / self.drip_rate.as_secs_f32()) as u64;

        if pages_to_drip > 0 {
            self.last_drip = Instant::now();
            self.budget += pages_to_drip;

            if self.budget > MAX_PENDING_BUDGET {
                self.budget = MAX_PENDING_BUDGET;
            }
        }
    }

    pub fn should_check(&self, interval: &CheckIntervals) -> bool {
        (self.frontpage.should_check(interval)
            || self.sitemap.should_check(interval)
            || self.feeds.should_check(interval))
            && self.budget > 0
    }

    pub async fn crawl(&mut self, client: &Client) {
        todo!()
    }
}
