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

use config::GossipConfig;
use distributed::{
    cluster::Cluster,
    member::{Member, Service},
};
pub use file_store::{gen_temp_dir, gen_temp_path};
use std::{cmp::Reverse, sync::Arc};
use thiserror::Error;

pub mod entrypoint;
pub mod inverted_index;

pub mod ampc;

pub mod api;
pub mod autosuggest;
mod backlink_grouper;
pub mod bangs;
mod bincode_utils;
mod block_on;
pub mod canon_index;
mod collector;
pub mod config;
pub mod crawler;
mod dated_url;
pub mod distributed;
pub mod entity_index;
mod enum_map;
pub mod executor;
mod external_sort;
pub mod feed;
mod highlighted;
pub mod hyperloglog;
pub mod image_store;
mod improvement;
pub mod index;
mod intmap;
pub mod iter_ext;
mod kahan_sum;
mod leaky_queue;
mod live_index;
pub mod log_group;
mod metrics;
mod models;
pub mod naive_bayes;
mod numericalfield_reader;
pub mod prehashed;
pub mod query;
mod rake;
pub mod ranking;
mod schema;
mod search_ctx;
mod search_prettifier;
pub mod searcher;
mod simhash;
pub mod similar_hosts;
mod sitemap;
mod snippet;
mod stopwords;
pub mod summarizer;
pub mod tokenizer;
#[allow(unused)]
mod ttl_cache;
pub mod warc;
pub mod web_spell;
pub mod webgraph;
pub mod webpage;
mod widgets;

pub mod generic_query;

pub use block_on::block_on;

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

/// Starts a gossip cluster in the background and returns a handle to it.
/// This is useful for blocking contexts where there is no runtime to spawn the cluster on.
pub fn start_gossip_cluster_thread(config: GossipConfig, service: Option<Service>) -> Arc<Cluster> {
    let (tx, rx) = std::sync::mpsc::channel();

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let cluster = match service {
                Some(service) => Cluster::join(
                    Member::new(service),
                    config.addr,
                    config.seed_nodes.unwrap_or_default(),
                )
                .await
                .unwrap(),
                None => {
                    Cluster::join_as_spectator(config.addr, config.seed_nodes.unwrap_or_default())
                        .await
                        .unwrap()
                }
            };

            let cluster = Arc::new(cluster);
            tx.send(cluster.clone()).unwrap();

            // need to keep tokio runtime alive
            // otherwise the spawned task in Cluster::join will be dropped
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
        });
    });

    rx.recv().unwrap()
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

    listener.local_addr().unwrap()
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

#[derive(
    serde::Serialize,
    serde::Deserialize,
    bincode::Encode,
    bincode::Decode,
    PartialEq,
    Eq,
    Debug,
    Clone,
    Hash,
)]
pub enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

impl<T> OneOrMany<T> {
    pub fn one(self) -> Option<T> {
        match self {
            OneOrMany::One(one) => Some(one),
            OneOrMany::Many(many) => many.into_iter().next(),
        }
    }

    pub fn many(self) -> Vec<T> {
        match self {
            OneOrMany::One(one) => vec![one],
            OneOrMany::Many(many) => many,
        }
    }
}

pub trait TopKOrderable {
    type SortKey: Ord + Copy;

    fn sort_key(&self) -> Self::SortKey;
}

impl<K, T> TopKOrderable for (K, T)
where
    K: Ord + Copy,
{
    type SortKey = K;

    fn sort_key(&self) -> Self::SortKey {
        self.0
    }
}

impl<T> TopKOrderable for Reverse<T>
where
    T: TopKOrderable,
{
    type SortKey = Reverse<T::SortKey>;

    fn sort_key(&self) -> Self::SortKey {
        Reverse(self.0.sort_key())
    }
}

/// Source (and explanation): [https://quickwit.io/blog/top-k-complexity]
pub fn sorted_k<T>(mut hits: impl Iterator<Item = T>, k: usize) -> Vec<T>
where
    T: TopKOrderable,
{
    if k == 0 {
        return Vec::new();
    }

    let mut top_k = Vec::with_capacity(2 * k);
    top_k.extend((&mut hits).take(k));

    let mut threshold = None;
    for hit in hits {
        if let Some(threshold) = threshold {
            if hit.sort_key() > threshold {
                continue;
            }
        }
        top_k.push(hit);
        if top_k.len() >= 2 * k {
            // The standard library does all of the heavy lifting here.
            let (_, median_el, _) = top_k.select_nth_unstable_by_key(k - 1, |el| el.sort_key());
            threshold = Some(median_el.sort_key());
            top_k.truncate(k);
        }
    }
    top_k.sort_unstable_by_key(|el| el.sort_key());
    top_k.truncate(k);
    top_k
}

/// Recursively move a file or directory to a new location.
/// Intended to be similar to the `mv` command in Unix.
pub fn mv<P1: AsRef<std::path::Path>, P2: AsRef<std::path::Path>>(
    from: P1,
    to: P2,
) -> std::io::Result<()> {
    let from = from.as_ref();
    let to = to.as_ref();

    if from.is_dir() {
        std::fs::create_dir_all(to)?;
        for entry in std::fs::read_dir(from)? {
            let entry = entry?;
            let new_from = entry.path();
            let new_to = to.join(entry.file_name());
            mv(new_from, new_to)?;
        }
        std::fs::remove_dir(from)?;
    } else {
        std::fs::rename(from, to)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_ceil_char_boundary(s: String, index: usize) {
            let index = if s.is_empty() {
                0
            } else {
                index % s.len()
            };

            let ceil = ceil_char_boundary(&s, index);
            prop_assert!(s.is_char_boundary(ceil));
        }

        #[test]
        fn prop_floor_char_boundary(s: String, index: usize) {
            let index = if s.is_empty() {
                0
            } else {
                index % s.len()
            };

            let floor = floor_char_boundary(&s, index);
            prop_assert!(s.is_char_boundary(floor));
        }
    }
}
