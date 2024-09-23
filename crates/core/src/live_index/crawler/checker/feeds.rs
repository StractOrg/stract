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

use crate::feed::Feed;
use chrono::DateTime;
use url::Url;

use super::{CheckIntervals, Checker, CrawlableUrl};

pub struct Feeds {
    feeds: Vec<Feed>,
    last_check: std::time::Instant,
}

impl Feeds {
    pub fn new(feeds: Vec<Feed>) -> Self {
        Self {
            feeds,
            last_check: std::time::Instant::now(),
        }
    }
}

impl Checker for Feeds {
    async fn check(&mut self) -> Vec<CrawlableUrl> {
        todo!()
    }

    fn should_check(&self, interval: &CheckIntervals) -> bool {
        todo!()
    }
}
