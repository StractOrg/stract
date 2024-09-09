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
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::{
    config::{CrawlerConfig, LiveIndexConfig},
    feed::scheduler::{DomainFeeds, Split},
};

pub use self::index::LiveIndex;
pub use self::index_manager::IndexManager;

mod crawler;
pub mod index;
mod index_manager;

const TTL: Duration = Duration::from_secs(60 * 60 * 24 * 60); // 60 days
const PRUNE_INTERVAL: Duration = Duration::from_secs(60 * 60); // 1 hour
const COMPACT_INTERVAL: Duration = Duration::from_secs(60 * 60); // 1 hour
const FEED_CHECK_INTERVAL: Duration = Duration::from_secs(60 * 10); // 10 minutes
const AUTO_COMMIT_INTERVAL: Duration = Duration::from_secs(60 * 5); // 5 minutes
const EVENT_LOOP_INTERVAL: Duration = Duration::from_secs(5);
const BATCH_SIZE: usize = 512;

#[derive(Debug, Clone)]
struct Feeds {
    last_checked: DateTime<Utc>,
    feed: DomainFeeds,
}

impl From<Split> for Vec<Feeds> {
    fn from(split: Split) -> Self {
        split
            .feeds
            .into_iter()
            .map(|feed| Feeds {
                last_checked: Utc::now(),
                feed,
            })
            .collect()
    }
}

impl From<&LiveIndexConfig> for CrawlerConfig {
    fn from(live: &LiveIndexConfig) -> Self {
        Self {
            num_worker_threads: 1, // no impact
            user_agent: live.user_agent.clone(),
            robots_txt_cache_sec: live.robots_txt_cache_sec,
            start_politeness_factor: live.start_politeness_factor,
            min_politeness_factor: live.min_politeness_factor,
            max_politeness_factor: live.max_politeness_factor,
            min_crawl_delay_ms: live.min_crawl_delay_ms,
            max_crawl_delay_ms: live.max_crawl_delay_ms,
            max_url_slowdown_retry: live.max_url_slowdown_retry,
            max_redirects: live.max_redirects,
            timeout_seconds: live.timeout_seconds,
            // no impact
            s3: crate::config::S3Config {
                bucket: String::new(),
                folder: String::new(),
                access_key: String::new(),
                secret_key: String::new(),
                endpoint: String::new(),
            },
            router_hosts: Vec::new(),
        }
    }
}
