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

use url::Url;

use crate::config::CheckIntervals;
use crate::Result;
use crate::{entrypoint::site_stats, webpage::url_ext::UrlExt};

use super::{Checker, CrawlableUrl};

pub struct Frontpage {
    url: Url,
    last_check: std::time::Instant,
    client: reqwest::Client,
}

impl Frontpage {
    pub fn new(site: &site_stats::Site, client: reqwest::Client) -> Result<Self> {
        let url = Url::robust_parse(&format!("https://{}/", site.as_str()))?;

        Ok(Self {
            url,
            last_check: std::time::Instant::now(),
            client,
        })
    }
}

impl Checker for Frontpage {
    async fn get_urls(&mut self) -> Result<Vec<CrawlableUrl>> {
        todo!()
    }

    fn should_check(&self, interval: &CheckIntervals) -> bool {
        self.last_check.elapsed() > interval.frontpage
    }
}
