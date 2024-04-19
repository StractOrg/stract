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

//! Main library for Stract.

#![doc(html_logo_url = "https://stract.com/images/biglogo.svg")]
#![warn(clippy::too_many_lines)]
// pedantic stuff
// #![warn(clippy::pedantic)]
// #![allow(clippy::unreadable_literal)]
// #![allow(clippy::missing_fields_in_debug)]
// #![allow(clippy::cast_possible_truncation)]
// #![allow(clippy::cast_precision_loss)]
// #![allow(clippy::cast_sign_loss)]
// #![allow(clippy::module_name_repetitions)] // maybe we should remove this later
// #![allow(clippy::missing_errors_doc)]

use std::path::PathBuf;
use thiserror::Error;

pub mod entrypoint;
pub mod inverted_index;

pub mod ampc;

mod api;
pub mod autosuggest;
pub mod bangs;
mod bincode_utils;
pub mod bloom;
mod collector;
pub mod config;
pub mod crawler;
pub mod distributed;
pub mod entity_index;
mod enum_map;
mod executor;
mod external_sort;
mod fastfield_reader;
pub mod feed;
mod file_store;
mod highlighted;
mod human_website_annotations;
pub mod hyperloglog;
pub mod image_store;
mod improvement;
pub mod index;
mod intmap;
mod kahan_sum;
mod leaky_queue;
mod live_index;
mod llm_utils;
mod metrics;
mod models;
pub mod naive_bayes;
pub mod prehashed;
mod query;
mod rake;
pub mod ranking;
mod schema;
mod search_ctx;
mod search_prettifier;
pub mod searcher;
mod simhash;
pub mod similar_hosts;
mod snippet;
pub mod speedy_kv;
mod stopwords;
pub mod summarizer;
mod tokenizer;
#[allow(unused)]
mod ttl_cache;
pub mod warc;
pub mod web_spell;
pub mod webgraph;
pub mod webpage;
mod widgets;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Failed to parse WARC file: {0}")]
    WarcParse(String),

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

pub type Result<T, E = anyhow::Error> = std::result::Result<T, E>;

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

#[cfg(test)]
fn free_socket_addr() -> std::net::SocketAddr {
    use std::net::{Ipv4Addr, TcpListener};

    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let addr = listener.local_addr().unwrap();

    addr
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

#[derive(
    Debug, Clone, Copy, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
)]
pub struct SortableFloat(f64);

impl From<f64> for SortableFloat {
    fn from(f: f64) -> Self {
        SortableFloat(f)
    }
}

impl From<SortableFloat> for f64 {
    fn from(f: SortableFloat) -> Self {
        f.0
    }
}

impl PartialEq for SortableFloat {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for SortableFloat {}

impl PartialOrd for SortableFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortableFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
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

macro_rules! enum_dispatch_from_discriminant {
    ($discenum:ident => $enum:ident, [$($disc:ident),*$(,)?]) => {
        impl From<$discenum> for $enum {
            fn from(value: $discenum) -> Self {
                match value {
                    $(
                    $discenum::$disc => $disc.into(),
                    )*
                }
            }
        }
    };
}

pub(crate) use enum_dispatch_from_discriminant;

const XXH3_SECRET: &[u8] = &xxhash_rust::const_xxh3::const_custom_default_secret(42);
pub fn fast_stable_hash_64(t: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64_with_secret(t, XXH3_SECRET)
}

pub fn fast_stable_hash_128(t: &[u8]) -> u128 {
    xxhash_rust::xxh3::xxh3_128_with_secret(t, XXH3_SECRET)
}
