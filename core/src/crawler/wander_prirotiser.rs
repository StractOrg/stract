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

//! In-memory graph that the worker constructs for the site during crawl.

use std::collections::BTreeMap;

use url::Url;

#[derive(Default)]
pub struct WanderPrioritiser {
    url_weights: BTreeMap<Url, f64>,
}

impl WanderPrioritiser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc(&mut self, url: Url, weight: f64) {
        self.url_weights
            .entry(url)
            .and_modify(|w| *w += weight)
            .or_insert(weight);
    }

    pub fn top_and_clear(&mut self, top_n: usize) -> Vec<(Url, f64)> {
        let mut urls: Vec<_> = self
            .url_weights
            .iter()
            .map(|(url, weight)| (weight, url))
            .collect();

        urls.sort_by(|(w1, _), (w2, _)| w2.total_cmp(w1));

        let res = urls
            .into_iter()
            .take(top_n)
            .map(|(w, url)| (url.clone(), *w))
            .collect();

        self.url_weights = BTreeMap::new();

        res
    }
}
