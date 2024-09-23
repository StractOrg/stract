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

use crate::Result;
use crate::{entrypoint::site_stats, webpage::url_ext::UrlExt};

use super::{CheckIntervals, Checker, CrawlableUrl};

pub struct Sitemap {
    robots_txt: Url,
    last_check: std::time::Instant,
}

impl Sitemap {
    pub fn new(site: &site_stats::Site) -> Result<Self> {
        let robots_txt = Url::robust_parse(&format!("{}/robots.txt", site.as_str()))?;

        Ok(Self {
            robots_txt,
            last_check: std::time::Instant::now(),
        })
    }
}

impl Checker for Sitemap {
    async fn check(&mut self) -> Vec<CrawlableUrl> {
        todo!()
    }

    fn should_check(&self, interval: &CheckIntervals) -> bool {
        todo!()
    }
}
