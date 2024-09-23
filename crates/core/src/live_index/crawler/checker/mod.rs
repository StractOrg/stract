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

use std::time::Duration;

use chrono::{DateTime, Utc};
use url::Url;

mod feeds;
mod frontpage;
mod sitemap;

pub use feeds::Feeds;
pub use frontpage::Frontpage;
pub use sitemap::Sitemap;

#[derive(Debug, Clone)]
pub struct CheckIntervals {
    pub rss: Duration,
    pub sitemap: Duration,
    pub frontpage: Duration,
}

pub struct CrawlableUrl {
    pub url: Url,
    pub last_modified: Option<DateTime<Utc>>,
}

pub trait Checker {
    async fn check(&mut self) -> Vec<CrawlableUrl>;
    fn should_check(&self, interval: &CheckIntervals) -> bool;
}
