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

use std::time::Duration;

pub struct Collector;

impl Collector {
    pub fn site_penalty() -> f64 {
        0.1
    }

    pub fn title_penalty() -> f64 {
        1.0
    }

    pub fn url_penalty() -> f64 {
        20.0
    }

    pub fn url_without_tld_penalty() -> f64 {
        1.0
    }

    pub fn max_docs_considered() -> usize {
        250_000
    }
}

pub struct Api;

impl Api {
    pub fn stackoverflow() -> f64 {
        0.5
    }

    pub fn entity_sidebar() -> f64 {
        10.0
    }

    pub fn max_concurrent_searches() -> Option<usize> {
        None
    }

    pub fn max_similar_hosts() -> usize {
        1_000
    }

    pub fn top_phrases_for_autosuggest() -> usize {
        1_000_000
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

    pub fn empty_query_snippet_words() -> usize {
        50
    }

    pub fn min_description_words() -> usize {
        10
    }

    pub fn min_body_length() -> usize {
        256
    }

    pub fn min_body_length_homepage() -> usize {
        1024
    }
}

pub struct Crawler;

impl Crawler {
    pub fn robots_txt_cache_sec() -> u64 {
        60 * 60
    }

    pub fn min_politeness_factor() -> u32 {
        0
    }

    pub fn start_politeness_factor() -> u32 {
        2
    }

    pub fn max_politeness_factor() -> u32 {
        11
    }

    pub fn min_crawl_delay_ms() -> u64 {
        10_000
    }

    pub fn max_crawl_delay_ms() -> u64 {
        180_000
    }

    pub fn max_url_slowdown_retry() -> u8 {
        3
    }

    pub fn timeout_seconds() -> u64 {
        60
    }
}

pub struct SearchQuery;

impl SearchQuery {
    pub fn flatten_response() -> bool {
        true
    }

    pub fn return_ranking_signals() -> bool {
        false
    }

    pub fn safe_search() -> bool {
        false
    }

    pub fn count_results_exact() -> bool {
        false
    }

    pub fn return_structured_data() -> bool {
        false
    }
}

pub struct Correction;

impl Correction {
    pub fn misspelled_prob() -> f64 {
        0.1
    }

    pub fn correction_threshold() -> f64 {
        50.0 // logprob difference
    }

    pub fn lm_prob_weight() -> f64 {
        5.77
    }
}

pub struct Widgets;

impl Widgets {
    pub fn calculator_fetch_currencies_exchange() -> bool {
        true
    }
}

pub struct Indexing;

impl Indexing {
    pub fn batch_size() -> usize {
        512
    }

    pub fn autocommit_after_num_inserts() -> usize {
        25_000
    }
}

pub struct ApproxHarmonic;
impl ApproxHarmonic {
    pub fn sample_rate() -> f64 {
        0.3
    }

    pub fn max_distance() -> u8 {
        7
    }

    pub fn save_centralities_with_zero() -> bool {
        false
    }
}

pub struct Webgraph;
impl Webgraph {
    pub fn merge_all_segments() -> bool {
        true
    }
}

pub struct LiveIndex;

impl LiveIndex {
    pub fn feeds_crawl_interval() -> Duration {
        Duration::from_secs(30 * 60) // 30 minutes
    }

    pub fn sitemap_crawl_interval() -> Duration {
        Duration::from_secs(24 * 60 * 60) // daily
    }

    pub fn frontpage_crawl_interval() -> Duration {
        Duration::from_secs(12 * 60 * 60) // 12 hours
    }

    pub fn blogs_budget() -> u64 {
        300_000
    }

    pub fn news_budget() -> u64 {
        500_000
    }

    pub fn remaining_budget() -> u64 {
        200_000
    }
}
