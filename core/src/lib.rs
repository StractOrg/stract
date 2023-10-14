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

//! Main library for Stract.

#![doc(html_logo_url = "https://trystract.com/images/biglogo.svg")]
// #![warn(clippy::pedantic)]
// #![warn(missing_docs)]
// #![warn(clippy::missing_docs_in_private_items)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::missing_errors_doc)]

use std::path::PathBuf;
use thiserror::Error;

pub mod entrypoint;
mod inverted_index;

pub mod mapreduce;

#[cfg(feature = "with_alice")]
pub mod alice;
mod api;
mod autosuggest;
mod bangs;
mod collector;
pub mod config;
pub mod crawler;
mod directory;
mod distributed;
mod entity_index;
mod enum_map;
mod executor;
mod fastfield_reader;
mod human_website_annotations;
pub mod hyperloglog;
mod image_downloader;
mod image_store;
mod improvement;
pub mod index;
mod intmap;
mod kahan_sum;
mod kv;
mod leaky_queue;
#[cfg(feature = "libtorch")]
mod llm_utils;
mod metrics;
pub mod naive_bayes;
pub mod prehashed;
#[cfg(feature = "libtorch")]
mod qa_model;
mod query;
pub mod ranking;
mod schema;
mod search_ctx;
mod search_prettifier;
pub mod searcher;
mod simhash;
pub mod similar_sites;
mod snippet;
pub mod spell;
mod subdomain_count;
#[cfg(feature = "libtorch")]
pub mod summarizer;
mod tokenizer;
#[allow(unused)]
mod ttl_cache;
pub mod warc;
pub mod webgraph;
pub mod webpage;
mod widgets;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Failed to parse WARC file")]
    WarcParse(&'static str),

    #[error("Encountered an empty required field ({0}) when converting to tantivy")]
    EmptyField(&'static str),

    #[error("Parsing error")]
    ParsingError(String),

    #[error("Failed to download warc files after all retries")]
    DownloadFailed,

    #[error("Query cannot be completely empty")]
    EmptyQuery,

    #[error("Unknown region")]
    UnknownRegion,

    #[error("Unknown CLI option")]
    UnknownCLIOption,

    #[error("The stackoverflow schema was not structured as expected")]
    InvalidStackoverflowSchema,

    #[error("Internal error")]
    InternalError(String),

    #[error("Unknown webpage robots meta tag")]
    UnknownRobotsMetaTag,
}

pub(crate) type Result<T, E = anyhow::Error> = std::result::Result<T, E>;

// taken from https://docs.rs/sled/0.34.7/src/sled/config.rs.html#445
pub fn gen_temp_path() -> PathBuf {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::SystemTime;

    static SALT_COUNTER: AtomicUsize = AtomicUsize::new(0);

    let seed = SALT_COUNTER.fetch_add(1, Ordering::SeqCst) as u128;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        << 48;

    let pid = u128::from(std::process::id());

    let salt = (pid << 16) + now + seed;

    if cfg!(target_os = "linux") {
        // use shared memory for temporary linux files
        format!("/dev/shm/pagecache.tmp.{salt}").into()
    } else {
        std::env::temp_dir().join(format!("pagecache.tmp.{salt}"))
    }
}

#[cfg(test)]
fn rand_words(num_words: usize) -> String {
    use rand::{distributions::Alphanumeric, Rng};
    let mut res = String::new();

    for _ in 0..num_words {
        res.push_str(
            rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(30)
                .map(char::from)
                .collect::<String>()
                .as_str(),
        );
        res.push(' ');
    }

    res.trim().to_string()
}

fn ceil_char_boundary(str: &str, index: usize) -> usize {
    let mut res = index;

    while !str.is_char_boundary(res) && res < str.len() {
        res += 1;
    }

    res
}

fn floor_char_boundary(str: &str, index: usize) -> usize {
    let mut res = index;

    while !str.is_char_boundary(res) && res > 0 {
        res -= 1;
    }

    res
}

pub fn split_u128(num: u128) -> [u64; 2] {
    [(num >> 64) as u64, num as u64]
}

pub fn combine_u64s(nums: [u64; 2]) -> u128 {
    ((nums[0] as u128) << 64) | (nums[1] as u128)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_combine_u128() {
        for num in 0..10000_u128 {
            assert_eq!(combine_u64s(split_u128(num)), num);
        }
    }
}
