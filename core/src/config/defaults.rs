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

pub struct Collector;

impl Collector {
    pub fn site_penalty() -> f64 {
        0.3
    }

    pub fn title_penalty() -> f64 {
        2.0
    }

    pub fn url_penalty() -> f64 {
        20.0
    }
}

pub struct Frontend;

impl Frontend {
    pub fn stackoverflow() -> f64 {
        0.5
    }

    pub fn discussions_widget() -> f64 {
        0.1
    }
}

pub struct Snippet;

impl Snippet {
    pub fn desired_num_chars() -> usize {
        275
    }

    pub fn delta_num_chars() -> usize {
        50
    }

    pub fn min_passage_width() -> usize {
        20
    }
}

pub struct Crawler;

impl Crawler {
    pub fn politeness_factor() -> f32 {
        1.0
    }

    pub fn min_crawl_delay_ms() -> u64 {
        200
    }

    pub fn max_crawl_delay_ms() -> u64 {
        30_000
    }

    pub fn max_politeness_factor() -> f32 {
        1024.0
    }

    pub fn min_politeness_factor() -> f32 {
        1.0
    }

    pub fn max_url_slowdown_retry() -> u8 {
        3
    }
}

pub struct WebgraphServer;

impl WebgraphServer {
    pub fn max_similar_sites() -> usize {
        1_000
    }
}