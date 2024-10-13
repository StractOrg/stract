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

use chrono::{DateTime, Utc};
use url::Url;

mod feeds;
mod frontpage;
mod sitemap;

pub use self::feeds::Feeds;
pub use frontpage::Frontpage;
pub use sitemap::Sitemap;

use crate::config::CheckIntervals;
use crate::dated_url::DatedUrl;

use crate::Result;

#[derive(Debug)]
pub struct CrawlableUrl {
    pub url: Url,
    pub last_modified: Option<DateTime<Utc>>,
}

impl From<DatedUrl> for CrawlableUrl {
    fn from(url: DatedUrl) -> Self {
        Self {
            url: url.url,
            last_modified: url.last_modified,
        }
    }
}

impl From<Url> for CrawlableUrl {
    fn from(url: Url) -> Self {
        Self {
            url,
            last_modified: None,
        }
    }
}

pub trait Checker {
    async fn get_urls(&self) -> Result<Vec<CrawlableUrl>>;
    fn should_check(&self, interval: &CheckIntervals) -> bool;
    fn update_last_check(&mut self);

    async fn get_urls_and_update_last_check(&mut self) -> Result<Vec<CrawlableUrl>> {
        self.update_last_check();
        self.get_urls().await
    }
}
